"""Run the pyspark application."""

import fire
from src.csv_loader.read_file import read_file
from src.csv_loader.save_to_bq import save_file_to_bq
# Import local modules
from src.utils.spark_setup import start_spark


def run(
    app_name: str,
    temp_bq_bucket: str,
    schema: str,
    dataset_name: str,
    data_bucket: str,
    table_name: str,
    files: str,
    delimiter: str = ",",
) -> None:
    """
    Run the spark app.

    :param app_name: Spark App Name,
    :param bucket: Cloud Storage bucket for temporary BigQuery export
    :param file_uri: Location of File to be Read
    :return: None
    """
    # ensure that the files have the gs://bucket prefix if not then populate it
    # files = [f"gs://{data_bucket}/{file}" for file in files if f"gs://{data_bucket}" not in file]
    print(f"{schema=}")
    files = f"gs://{data_bucket}/{files}/*"
    spark = start_spark(bucket=temp_bq_bucket)

    df = read_file(files=files, spark=spark, schema=schema, delimiter=delimiter)

    save_file_to_bq(df=df, table=f"{dataset_name}.{table_name}")


if __name__ == "__main__":
    fire.Fire(run)
