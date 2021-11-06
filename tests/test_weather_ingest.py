import csv
import os.path
import tempfile
from typing import List, Tuple

from conf import spark
from transformation import weather_ingest


def test_should_maintain_all_data_it_reads() -> None:
    test_ingest_folder, test_transform_folder = __create_ingest_and_transform_folder()
    test_input_file = "{}/test_weather_input.csv".format(test_ingest_folder)
    csv_content = [
        ['Date', 'Region', 'Temperature', 'WindSpeed'],
        ['2021-01-01', 'Region 1', 10, 10.1],
        ['2021-02-02', 'Region 2', 1, 9],
        ['2021-03-04', 'Region 1', 13, 10.0],
        ['2021-04-10', 'Region 3', 2, 12.5],
        ['2021-02-10', 'Region 2', 3, 13.1]
    ]

    __create_csv_file(test_input_file, csv_content)

    weather_ingest.run(spark, test_input_file, test_transform_folder)

    actual = spark.read.parquet(test_transform_folder)
    expected = spark.createDataFrame(
        [
            ['2021-01-01', 'Region 1', 10, 10.1],
            ['2021-02-02', 'Region 2', 1, 9.0],
            ['2021-03-04', 'Region 1', 13, 10.0],
            ['2021-04-10', 'Region 3', 2, 12.5],
            ['2021-02-10', 'Region 2', 3, 13.1]
        ],
        ['Date', 'Region', 'Temperature', 'WindSpeed']
    )

    assert actual.collect() == expected.collect()
    assert actual.columns == expected.columns


def __create_ingest_and_transform_folder() -> Tuple[str, str]:
    base_path = tempfile.mkdtemp()
    ingest_folder = "{}{}".format(base_path, os.path.sep)
    transform_folder = "{}{}transform".format(base_path, os.path.sep)

    return ingest_folder, transform_folder


def __create_csv_file(file_path: str, content: List[List[str]]) -> None:
    with open(file_path, 'w') as f:
        import_csv_writer = csv.writer(f)
        import_csv_writer.writerows(content)
        f.close()
