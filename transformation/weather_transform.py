from pyspark.sql import SparkSession, DataFrame


def calculate_max_temperature(spark, weather_df) -> DataFrame:
    """
    Uses spark sql to query the weather dataframe to calculate hottest_temp and return transformed dataframe
    :param spark:
    :param weather_df:
    :return:
    """

    weather_df.createOrReplaceTempView("weather")

    weather_hottest_df = spark.sql("""
                    SELECT distinct
                        base.ObservationDate, 
                        base.Region,
                        base.MaxTemperature,
                        rank() over(ORDER BY base.MaxTemperature DESC) as IsHottestRank
                    FROM(
                        SELECT  
                            ObservationDate, 
                            Region,
                            MAX(ScreenTemperature) OVER(PARTITION BY ObservationDate, Region) as MaxTemperature
                        FROM weather
                    ) as base
    """)

    weather_hottest_df.show(5, truncate=False)
    # weather_hottest_df.printSchema()

    return weather_hottest_df


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Read parquet weather data and save the transformed hottest day data in a parquet file
    :param spark:
    :param input_path:
    :param output_path:
    :return:
    """

    input_df = spark.read.parquet(input_path)

    weather_hottest_df = calculate_max_temperature(spark, input_df)

    weather_hottest_df.coalesce(1).write.mode("overwrite").parquet(output_path)
