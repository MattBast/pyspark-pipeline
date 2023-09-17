"""extract_tests.

Test the scripts ability to read data from csv files and schemas from json files.
"""

# test imports
import pytest
from chispa.dataframe_comparer import assert_df_equality
from tests.fixtures.spark import spark_session
from tests.fixtures.schema import schema_path, not_obj_json_path
from tests.fixtures.data import csv_path

# source code imports (to be tested)
from src.main import extract, load_schema, read_csv

# standard imports
from pathlib import Path
from json import JSONDecodeError
from result import Err
from pyspark.sql.types import StructType, IntegerType, StringType

# ************************************************************************************************
# extract tests
# ************************************************************************************************

@pytest.mark.usefixtures("spark_session", "schema_path", "csv_path")
def test_extract(spark_session, schema_path, csv_path):
    
    test_df = extract(session=spark_session, schema_path=schema_path, data_path=csv_path)

    expected_df = spark_session.createDataFrame(
        [(1, "foo"),(2, "bar")], # data
        "id int, label string"  # schema
    )

    assert test_df.is_ok(), "Error was returned when extracting a dataframe"
    assert_df_equality(test_df.unwrap(), expected_df)


@pytest.mark.usefixtures("spark_session", "csv_path")
def test_schema_issues_fallback_to_schema_inference(spark_session, csv_path):
    
    test_df = extract(
        session=spark_session, 
        schema_path=Path("./does_not_exist.json"), 
        data_path=csv_path
    )

    expected_df = spark_session.createDataFrame(
        [(1, "foo"),(2, "bar")], # data
        "id int, label string"  # schema
    )

    assert test_df.is_ok(), "Error was returned when schema does not exist"
    assert_df_equality(test_df.unwrap(), expected_df)


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





# @pytest.mark.usefixtures("spark_session")
# def test_validate(spark_session):


# @pytest.mark.usefixtures("spark_session")
# def test_load(spark_session):