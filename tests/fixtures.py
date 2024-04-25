"""
fixtures.py
-----------
This module contains the fixtures and other utilities for the testing suite
"""
import shutil
import pathlib
import tempfile

from pyspark.sql import SparkSession
from pyspark import __version__ as spark_version

import pytest

if float(spark_version.rsplit('.', 1)[0]) > 3.5:
    from pyspark.testing import (
        assertDataFrameEqual,
        assertSchemaEqual
    )


    def standard_dataframe_comparison(actual, expected):  # pragma: no cover
        assertDataFrameEqual(actual, expected)
        assertSchemaEqual(actual.schema, expected.schema)
else:
    def standard_dataframe_comparison(df1, df2):  # pragma: no cover
        """Pytest fixture for comparing two dataframes"""
        # assertions and expectations
        actual, expected = df1, df2
        try:  # pragma: no cover
            assert expected.count() == actual.count()
            assert sorted(expected.columns) == sorted(actual.columns), (
                f"actual has `{', '.join(sorted(actual.columns))}` and "
                f"expected has  `{', '.join(sorted(expected.columns))}`")
            assert actual.exceptAll(expected.select(*actual.columns)).rdd.isEmpty()
        except Exception as e:  # pragma: no cover
            print(e)
            print('result_df')
            print(actual.toPandas().to_string())
            print('expected_df')
            print(expected.select(*actual.columns).toPandas().to_string())
            raise e

data_dir = pathlib.Path(__file__).parent.absolute().joinpath('data')


@pytest.fixture(scope='session', autouse=True)
def filesystem():
    """Pytest fixture for setting up and tearing down the filesystem"""
    test_dir = tempfile.TemporaryDirectory().name
    pathlib.Path(test_dir).mkdir(parents=True, exist_ok=True)
    yield test_dir
    shutil.rmtree(test_dir)


@pytest.fixture(scope='session', autouse=True)
def spark():
    """Pytest fixture for setting up spark."""
    spark = SparkSession.builder.master("local[1]") \
        .appName('dbutils-testing-suite') \
        .getOrCreate()
    yield spark
