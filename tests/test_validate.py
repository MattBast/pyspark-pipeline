"""spark_fixture.

Create re-usable Spark modules to help the tests create and test Spark 
dataframes.
"""

# test imports
import pytest
from spark_fixture import spark_session
from schema_fixture import schema_path, not_obj_json_path
from data_fixture import csv_path
from chispa.dataframe_comparer import assert_df_equality

# source code imports (to be tested)
from src.main import extract, load_schema, read_csv

# standard imports
from pathlib import Path
from json import JSONDecodeError
from result import Err
from pyspark.sql.types import StructType, IntegerType, StringType

# @pytest.mark.usefixtures("spark_session")
# def test_extract(spark_session):
    
#     test_df = extract(
#         session=spark_session, 
#         schema_path=Path("./tests/schemas/schema_1.json"), 
#         data_path=Path("./tests/data/test_1.csv")
#     )

#     expected_df = spark_session.createDataFrame(
#         [(1, "foo"),(2, "bar")], # data
#         "id int, label string"  # schema
#     )

#     assert test_df.is_ok(), "Error was returned when extracting a dataframe"
#     assert_df_equality(test_df.unwrap(), expected_df)


# ************************************************************************************************
# load schema tests
# ************************************************************************************************

@pytest.mark.usefixtures("schema_path")
def test_schema_load(schema_path):
    
    test_schema = load_schema(schema_path)

    expected_schema = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": False,
                "metadata": {}
            },
            {
                "name": "label",
                "type": "string",
                "nullable": False,
                "metadata": {}
            }
        ]
    }

    assert test_schema.is_ok(), "Error was returned when loading schema from file"
    assert test_schema.unwrap() == expected_schema, "Incorrect schema was returned from file."


def test_schema_does_not_exist():
    
    test_schema = load_schema(Path("./does_not_exist.json"))

    assert test_schema.is_err(), "Error was not returned when loading non-existant schema."
    assert isinstance(test_schema.err(), FileNotFoundError)


@pytest.mark.usefixtures("not_obj_json_path")
def test_schema_is_not_an_object(not_obj_json_path):
    
    test_schema = load_schema(not_obj_json_path)

    assert test_schema.is_err(), "Error was not returned when loading json that is not an object."
    assert isinstance(test_schema.err(), TypeError)


@pytest.mark.usefixtures("csv_path")
def test_schema_file_is_not_json(csv_path):
    
    test_schema = load_schema(csv_path)

    assert test_schema.is_err(), "Error was not returned when loading csv file."
    assert isinstance(test_schema.err(), JSONDecodeError)


# ************************************************************************************************
# read csv to dataframe tests
# ************************************************************************************************

@pytest.mark.usefixtures("spark_session", "csv_path")
def test_read_csv_without_schema(spark_session, csv_path):
    
    test_df = read_csv(spark_session, csv_path, schema=None)

    expected_df = spark_session.createDataFrame(
        [(1, "foo"),(2, "bar")], # data
        "id int, label string"  # schema
    )

    assert test_df.is_ok(), "Error was returned when reading a valid csv file into a dataframe"
    assert_df_equality(test_df.unwrap(), expected_df)


@pytest.mark.usefixtures("spark_session", "csv_path")
def test_read_csv_with_schema(spark_session, csv_path):
    
    schema = StructType() \
        .add("id", IntegerType(), True) \
        .add("label", StringType(), True)

    test_df = read_csv(spark_session, csv_path, schema)

    expected_df = spark_session.createDataFrame(
        [(1, "foo"),(2, "bar")], # data
        "id int, label string"  # schema
    )

    assert test_df.is_ok(), "Error was returned when reading a valid csv file into a dataframe"
    assert_df_equality(test_df.unwrap(), expected_df)


# @pytest.mark.usefixtures("spark_session", "csv_path")
# def test_read_csv_throws_error_when_schema_data_mismatch(spark_session, csv_path):
    
#     schema = StructType() \
#         .add("id", IntegerType(), True) \
#         .add("label", StringType(), True) \
#         .add("does_not_exist", IntegerType(), False)

#     test_df = read_csv(spark_session, csv_path, schema)

#     print(test_df.unwrap().show())

#     assert test_df.is_err(), "Error was not returned when the schema differs from the data"





# @pytest.mark.usefixtures("spark_session")
# def test_validate(spark_session):


# @pytest.mark.usefixtures("spark_session")
# def test_load(spark_session):