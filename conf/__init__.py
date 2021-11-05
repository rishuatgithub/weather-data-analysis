from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Weather-Data-Analysis-App").getOrCreate()