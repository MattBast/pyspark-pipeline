"""validate_tests.

Test the scripts ability to enforce constraints on a dataframe.
"""

# test imports
import pytest
from tests.fixtures.spark import spark_session
from tests.fixtures.schema import schema_path, not_obj_json_path
from tests.fixtures.data import csv_path

# source code imports (to be tested)
from src.main import validate

# standard imports
from pyspark.sql.types import StructType, IntegerType, StringType, FloatType


@pytest.mark.usefixtures("spark_session")
def test_good_df(spark_session):
    
    schema = StructType() \
        .add("PassengerId", IntegerType(), True) \
        .add("Survived", IntegerType(), True) \
        .add("Pclass", IntegerType(), True) \
        .add("Name", StringType(), True) \
        .add("Sex", StringType(), True) \
        .add("Age", FloatType(), True) \
        .add("Sibsp", IntegerType(), True) \
        .add("Parch", IntegerType(), True) \
        .add("Ticket", StringType(), True) \
        .add("Fare", FloatType(), True) \
        .add("Cabin", StringType(), True) \
        .add("Embarked", StringType(), True)

    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_ok(), "Error was returned when validating a good dataframe"
