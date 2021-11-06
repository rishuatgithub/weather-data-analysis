from pyspark.sql import SparkSession
import logging

spark = SparkSession.builder.appName("Weather-Data-Analysis").getOrCreate()

FORMAT = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
logging.basicConfig(filename="../logs/weather-data-analysis-log.log", level=logging.INFO, format=FORMAT)
