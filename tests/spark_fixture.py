"""spark_fixture.

Create re-usable Spark modules to help the tests create and test Spark 
dataframes.
"""

# test imports
import pytest

# standard imports
import json
import os
import logging
from pathlib import Path
from typing import Generator

# pyspark imports
os.environ["SPARK_VERSION"] = "3.3"
from pyspark.sql import SparkSession, DataFrame # noqa: E402
from pyspark.sql.types import StructType # noqa: E402

# deequ imports
import pydeequ # noqa: E402
from pydeequ.checks import Check, CheckLevel # noqa: E402
from pydeequ.verification import VerificationSuite, VerificationResult # noqa: E402

@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """ fixture for creating a spark session"""

    session = SparkSession \
        .builder \
        .master("local[2]") \
        .appName('test_session') \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .getOrCreate()

    quiet_py4j()

    return session


def quiet_py4j() -> None:
    """ turn down spark logging for the test context """

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.ERROR)