"""validate_tests.

Test the scripts ability to enforce constraints on a dataframe.
"""

# test imports
import pytest
from tests.fixtures.spark import spark_session
from tests.fixtures.schema import titanic_schema

# source code imports (to be tested)
from src.main import validate, ConstraintError

# standard imports
from pyspark.sql.types import StructType, IntegerType, StringType, FloatType


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_good_df(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_ok()


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_passenger_id(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (None,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(PassengerId,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_passenger_id_not_unique(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (2,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint UniquenessConstraint(Uniqueness(List(PassengerId),None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_passenger_id_contains_negative_values(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (-1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint ComplianceConstraint(Compliance(PassengerId is non-negative,COALESCE(CAST(PassengerId AS DECIMAL(20,10)), 0.0) >= 0,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_name(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,None,"female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Name,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_ticket(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,None,7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Ticket,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_pclass(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,None,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Pclass,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_parch(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,None,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Parch,None)) returned status Failure."



@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_embarked(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86",None)
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Embarked,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_age(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",None,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Age,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_cabin(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,None,"S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Cabin,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_fare(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",None,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Fare,None)) returned status Failure."



@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_sibsp(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,None,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(SibSp,None)) returned status Failure."


@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_survived(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,None,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","female",38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Survived,None)) returned status Failure."



@pytest.mark.usefixtures("spark_session", "titanic_schema")
def test_null_value_in_sex(spark_session, titanic_schema):
    
    test_df = spark_session.createDataFrame(
        [
            (1,0,3,"Braund, Mr. Owen Harris","male",22.0,1,0,"A/5 21171",7.25,"C84","S"),
            (2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)",None,38.0,1,0,"PC 17599",71.2833,"C85","C"),
            (3,1,3,"Heikkinen, Miss. Laina","female",26.0,0,0,"STON/O2. 3101282",7.925,"C86","S")
        ],
        titanic_schema
    )

    validation_response = validate(spark_session, test_df)

    assert validation_response.is_err()
    assert isinstance(validation_response.err(), ConstraintError)
    assert validation_response.err().message == "Constraint CompletenessConstraint(Completeness(Sex,None)) returned status Failure."



