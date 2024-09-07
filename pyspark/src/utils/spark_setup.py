"""Start the Spark session."""

from pyspark.sql import SparkSession


def start_spark(
    bucket: str,
) -> SparkSession:
    """
    Start the spark session.

    :param app_name: Name of the Spark App
    :return: SparkSession
    """
    builder = SparkSession.builder

    spark = builder.getOrCreate()
    spark.conf.set("temporaryGcsBucket", bucket)
    return spark
