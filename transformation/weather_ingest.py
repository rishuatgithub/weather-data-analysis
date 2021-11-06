from pyspark.sql import SparkSession


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Reads the Input file and creates the parquet version of the data.
    :param spark:
    :param input_path:
    :param output_path:
    :return:
    """
    input_df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    # input_df.printSchema()
    # input_df.show(5)

    input_df.coalesce(1).write.mode("overwrite").parquet(output_path)
