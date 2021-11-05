from conf import spark
from transformation import parse_input_data

if __name__ == '__main__':

    INPUT_PATH = '../data/input'
    OUTPUT_PATH = '../data/output/weather_parquet'

    parse_input_data.run(spark, INPUT_PATH, OUTPUT_PATH)

