"""Save the file to bigquery."""

from pyspark.sql.dataframe import DataFrame


def save_file_to_bq(df: DataFrame, table: str) -> None:
    """
    Save the file to BigQuery.

    :param stocks_df: Dataframe
    :param table: table_name
    :return: Status
    """
    (df.write.format("bigquery").mode("overwrite").option("table", table).save())
