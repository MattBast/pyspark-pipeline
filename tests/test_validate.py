"""spark_fixture.

Create re-usable Spark modules to help the tests create and test Spark 
dataframes.
"""

# test imports
import pytest
from spark_fixture import spark_session
from chispa.dataframe_comparer import assert_df_equality

# source code imports
from src.main import extract

# standard imports
from pathlib import Path

@pytest.mark.usefixtures("spark_session")
def test_extract(spark_session):
    
    test_df = extract(
        session=spark_session, 
        schema_path=Path("./tests/schemas/schema_1.json"), 
        data_path=Path("./tests/data/test_1.csv")
    )

    expected_df = spark_session.createDataFrame(
        [(1, "foo"),(2, "bar")], # data
        "id int, label string"  # schema
    )

    assert test_df.is_ok(), "Error was returned when extracting a dataframe"
    assert_df_equality(test_df.unwrap(), expected_df)


# @pytest.mark.usefixtures("spark_session")
# def test_validate(spark_session):


# @pytest.mark.usefixtures("spark_session")
# def test_load(spark_session):