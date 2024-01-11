from datetime import datetime
from functools import reduce
from typing import List, Tuple
from uuid import uuid4

from delta import DeltaTable
from pyspark.sql.functions import (
    col,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    DateType,
    DecimalType,
)

from main.build_spark import build_spark
import pathlib


def merge_by_tenants() -> None:
    #
    # CONFIG
    #
    root_folder = pathlib.Path(__file__).parent.parent.parent.resolve()
    print(root_folder)

    fake_target_path = f"{root_folder}/data/test_fake_target"
    fake_source_path = f"{root_folder}/data/test_fake_source"

    run_id = uuid4()
    run_delta_api_target_path = f"{root_folder}/dist/test_{run_id}_delta"
    run_spark_api_target_path = f"{root_folder}/dist/test_{run_id}_spark"

    session = build_spark()

    schema = StructType(
        [
            StructField("localDate", DateType(), False),
            StructField("brandId", IntegerType(), True),
            StructField("mailingId", LongType(), True),
            StructField("mailingVariantId", LongType(), True),
            StructField("channelType", StringType(), True),
            StructField("revenue", DecimalType(19, 5), True),
            StructField("conversionRevenue", DecimalType(19, 5), True),
            StructField("count", LongType(), True),
            StructField("conversionCount", LongType(), True),
            StructField("_tenant", StringType(), True),
        ]
    )
    partitioning_columns = ["_tenant"]
    merge_columns = ["_tenant", "localDate", "brandId", "mailingId", "mailingVariantId", "channelType"]

    #
    # WRITE DF TO S3 THEN READ
    #
    source = session.read.format("delta").load(fake_source_path)

    #
    # CREATE TALBE USING DELTA API & FILL IT WITH DATA
    #
    (
        DeltaTable.createIfNotExists(session)
        .addColumns(schema)
        .location(run_delta_api_target_path)
        .partitionedBy(partitioning_columns)
        .execute()
    )

    (
        session
        #
        .read.format("delta")
        .load(fake_target_path)
        .write.format("delta")
        .mode("append")
        .save(run_delta_api_target_path)
    )

    #
    # CREATE TABLE USING SPARK API & FILL IT WITH DATA
    #
    (
        session.read.format("delta")
        .load(fake_target_path)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .partitionBy("_tenant")
        .save(run_spark_api_target_path)
    )

    targets: List[Tuple[str, DeltaTable]] = [
        ("delta api", DeltaTable.forPath(session, run_delta_api_target_path).alias("table")),
        ("spark api", DeltaTable.forPath(session, run_spark_api_target_path).alias("table")),
    ]

    DeltaTable.forPath(session, run_delta_api_target_path).history().orderBy(col("version")).show(truncate=False)
    DeltaTable.forPath(session, run_spark_api_target_path).history().orderBy(col("version")).show(truncate=False)

    #
    # MERGE
    #
    for i in range(3):
        for api, target in targets:
            start_time = datetime.utcnow()

            print(f"source schema: {source.schema.json()}")
            print(f"target schema: {target.toDF().schema.json()}")

            merge_conditions_exprs = [
                (col(f"target.{c}") == col(f"source.{c}")) | (col(f"target.{c}").isNull() & col(f"source.{c}").isNull())
                for c in merge_columns
            ]
            merge_conditions_exprs.append(col("target._tenant") == col("source._tenant"))

            merge_condition = reduce(lambda a, b: a & b, merge_conditions_exprs)
            update_condition = col("calculatedMechanicId").isNotNull()
            delete_condition = col("calculatedMechanicId").isNull()

            (
                target.alias("target")
                .merge(source.alias("source"), merge_condition)
                .whenMatchedUpdateAll(update_condition)
                .whenMatchedDelete(delete_condition)
                .whenNotMatchedInsertAll()
                .execute()
            )

            time = (datetime.utcnow() - start_time).total_seconds()
            print(f"Run: {i}, api: {api}, time: {time}")
