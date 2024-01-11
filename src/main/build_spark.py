from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _configure_common(builder: SparkSession.Builder) -> SparkSession.Builder:
    return (
        builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.ui.explainMode", "extended")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.task.maxFailures", "12")
        .config("spark.stage.maxConsecutiveAttempts", "8")
    )


def _configure_for_tests(builder: SparkSession.Builder) -> SparkSession.Builder:
    return (
        builder.master("local[1]")
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.rdd.compress", False)
        .config("spark.shuffle.compress", False)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.executor.memory", "4G")
        .config("spark.driver.memory", "8G")
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        )
    )


def build_spark() -> SparkSession:
    builder = SparkSession.Builder()

    _configure_common(builder)
    _configure_for_tests(builder)

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2",
        "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.2",
        "io.delta:delta-core_2.12:2.0.0",
        "org.apache.hadoop:hadoop-aws:3.3.6",
    ]

    spark = configure_spark_with_delta_pip(builder, packages).getOrCreate()
    spark.catalog.clearCache()

    return builder.getOrCreate()
