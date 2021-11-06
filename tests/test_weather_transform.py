import os
import tempfile
from typing import Tuple

from pyspark.sql.types import StructField, IntegerType, StringType, LongType

from conf import spark
from transformation import weather_transform

TEST_INPUT_COLUMNS = ['ObservationDate', 'ScreenTemperature', 'SiteName', 'Region']
TEST_INPUT_DATA = [
    ['2016-02-01T00:00:00', 10, 'S1', 'REGION-A'],
    ['2016-02-01T00:00:00', 9, 'S2', 'REGION-A'],
    ['2016-02-01T00:00:00', 7, 'S1', 'REGION-B'],
    ['2016-02-01T00:00:00', 8, 'S2', 'REGION-B'],
    ['2016-02-01T00:00:00', 2, 'S1', 'REGION-C'],
    ['2016-02-01T00:00:00', 1, 'S2', 'REGION-C'],
    ['2016-02-02T00:00:00', 7, 'S1', 'REGION-A'],
    ['2016-02-02T00:00:00', 8, 'S2', 'REGION-A'],
    ['2016-02-02T00:00:00', 12, 'S1', 'REGION-B'],
    ['2016-02-02T00:00:00', 2, 'S2', 'REGION-C'],
]


def test_should_return_expected_columns_and_schema() -> None:
    ingest_test_folder, transform_test_folder = __create_ingest_and_transform_folder()
    weather_transform.run(spark, ingest_test_folder, transform_test_folder)

    actual_dataframe = spark.read.parquet(transform_test_folder)
    actual_columns = list(actual_dataframe.columns)
    actual_schema = list(actual_dataframe.schema)

    expected_columns = ['ObservationDate', 'Region', 'MaxTemperature', 'IsHottestRank']
    expected_schema = [StructField('ObservationDate', StringType(), nullable=True),
                       StructField('Region', StringType(), nullable=True),
                       StructField('MaxTemperature', LongType(), nullable=True),
                       StructField('IsHottestRank', IntegerType(), nullable=True)]

    assert expected_columns == actual_columns
    assert expected_schema == actual_schema


def test_should_calculate_maxtemperature_and_rank() -> None:
    ingest_test_folder, transform_test_folder = __create_ingest_and_transform_folder()
    weather_transform.run(spark, ingest_test_folder, transform_test_folder)

    expected_columns = ['ObservationDate', 'Region', 'MaxTemperature', 'IsHottestRank']
    actual_dataframe = spark.read.parquet(transform_test_folder)
    expected_dataframe = spark.createDataFrame(
        [
            ['2016-02-02T00:00:00', 'REGION-B', 12, 1],
            ['2016-02-01T00:00:00', 'REGION-A', 10, 2],
            ['2016-02-01T00:00:00', 'REGION-B', 8, 4],
            ['2016-02-02T00:00:00', 'REGION-A', 8, 4],
            ['2016-02-01T00:00:00', 'REGION-C', 2, 8],
            ['2016-02-02T00:00:00', 'REGION-C', 2, 8]
        ],
        expected_columns
    )

    assert expected_dataframe.collect() == actual_dataframe.collect()


def __create_ingest_and_transform_folder() -> Tuple[str, str]:
    base_path = tempfile.mkdtemp()
    ingest_folder = "{}{}ingest".format(base_path, os.path.sep)
    transform_folder = "{}{}transform".format(base_path, os.path.sep)
    ingest_test_dataframe = spark.createDataFrame(TEST_INPUT_DATA, TEST_INPUT_COLUMNS)
    ingest_test_dataframe.write.mode("overwrite").parquet(ingest_folder)
    return ingest_folder, transform_folder
