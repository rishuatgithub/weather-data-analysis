from conf import spark, logging
from transformation import weather_ingest, weather_transform

if __name__ == '__main__':

    WEATHER_INPUT_PATH = '../data/input'
    WEATHER_INGEST_OUTPUT_PATH = '../data/output/weather_parquet'
    WEATHER_TRANSFORM_OUTPUT_PATH = '../data/output/weather_calculate_temperature'

    logging.info("Starting the Weather Data Analysis Spark Application")

    logging.info("Ingest .csv files to .parquet file with schema enabled")
    weather_ingest.run(spark, WEATHER_INPUT_PATH, WEATHER_INGEST_OUTPUT_PATH)

    logging.info("Analyzing the weather data and saving it as an .csv file")
    weather_transform.run(spark, WEATHER_INGEST_OUTPUT_PATH, WEATHER_TRANSFORM_OUTPUT_PATH)

    logging.info("Stopping the execution")
    spark.stop()
