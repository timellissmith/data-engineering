"""Read the CSV file and process it."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


def read_file(
    spark: SparkSession,
    files: str,
    schema: str,
    delimiter: str = ",",
    column_name_of_corrupt_record: str = "broken",
    header: bool = True,
) -> DataFrame:
    """Read a CSV file and return the dataframe.

    Args:
        spark (SparkSession): The Spark Session.
        file_uri (str): The url of the file to be processed
        delimiter (str): The delimiter of the CSV file
        schema (str): The schema in string format ("name type")
        column_name_of_corrupt_record (str, optional): The column name to store corrupt records in. Defaults to "bad_records".
        header (str): Whether the

    Returns:
        DataFrame: The dataframe for loading into BigQuery.
    """
    return (
        spark.read.option("delimiter", delimiter)
        .option("header", header)
        .option("columnNameOfCorruptRecord", column_name_of_corrupt_record)
        .schema(schema)
        .csv(files)
        .withColumn("file_name", F.input_file_name())
        .withColumn("load_timestamp", F.current_timestamp())
    )
