"""extract-validate-load.

This script is intended to extract a dataset, validate it against a schema and 
set of constraints and then load the dataset to a destination. If any of the 
validations fail, an error is thrown.
"""

# standard imports
import json
from json import JSONDecodeError
import os
from pathlib import Path
from typing import Any, TypedDict

# custom error classes
from result import Result, Ok, Err, as_result

# pyspark imports
os.environ["SPARK_VERSION"] = "3.3"
from pyspark.sql import SparkSession, DataFrame # noqa: E402
from pyspark.sql.types import StructType # noqa: E402
from pyspark.errors import PySparkException # noqa: E402

# deequ imports
import pydeequ # noqa: E402
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes # noqa: E402
from pydeequ.verification import VerificationSuite, VerificationResult # noqa: E402


class CheckResult(TypedDict):
	"""An alias for the dict that is returned when pydeequ checks a constraint."""

	check_status: str
	check_level: str
	constraint_status: str
	check: str
	constraint_message: str
	constraint: str


class ConstraintError(Exception):
	"""A custom error used to report that a dataframe has failed a constraint check."""

	def __init__(self, constraint: str | None, status: str | None):
		"""Create a constraunt error from the metadata returned from pydeequ.

		Keyword arguments:
		constraint -- details of what check failed
		status -- the status of the failed check
		"""

		self.constraint = str(constraint)
		self.status = str(status)

		message = \
			"Constraint " + self.constraint + \
			" returned status " + self.status + \
			"."

		super().__init__(message)


def main() -> None:
	"""Coordinates the data extraction, validation and loading."""

	match start_spark():
		case Ok(session):
			print("Spark started.")
		case Err(start_e):
			print(start_e)
			return

	match extract(session, Path("./src/schema.json"), Path("./data/titanic.csv")):
		case Ok(df):
			print("Data extracted.")
		case Err(extract_e):
			print(extract_e)
			return

	match validate(session, df):
		case Ok(None):
			print("Dataframe validated.")
		case Err(val_e):
			print(val_e)
			return


	match load(df, Path("./data/valid_titanic.csv")):
		case Ok(None):
			print("Data written to destination.")
		case Err(load_e):
			print(load_e)
			return

	shutdown_spark(session)


@as_result(PySparkException)
def start_spark() -> SparkSession:
	"""Start and return a Spark session with deequ constarints enabled."""

	session = SparkSession \
		.builder \
		.master("local[1]") \
		.appName('extract_validate_load') \
		.config("spark.jars.packages", pydeequ.deequ_maven_coord) \
		.config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
		.getOrCreate()

	# suppress any spark logs that are not errors 
	session.sparkContext.setLogLevel("ERROR")

	return session


def extract(
	session: SparkSession, 
	schema_path: Path, 
	data_path: Path
) -> Result[DataFrame, PySparkException]:
	"""Read the specified csv file into a Spark dataframe. Returns the dataframe.

	Returns PySparkException if Spark fails to create a Dataframe

	Keyword arguments:
	session -- the apark session used to create a dataframe
	schema_path -- a path that points at the tables schema
	data_path -- a path that points at the csv file of data
	"""

	# load the datasets schema
	match load_schema(schema_path):
		
		case Ok(raw_schema):
			schema = StructType.fromJson(raw_schema)
			read_result = read_csv(session, data_path, schema)

		case Err(_e):
			read_result = read_csv(session, data_path, None)
	
	# load a csv file into a dataframe and return the dataframe
	return read_result


@as_result(OSError, JSONDecodeError, TypeError)
def load_schema(schema_path: Path) -> dict[str, Any]:
	"""Read the specified json file and decode into a python dict.

	Returns OSError if the schema file fails to open.
	Returns JSONDecodeError if the schema file can't be parsed to json.

	Keyword arguments:
	schema_path -- a path that points at the tables schema
	"""

	with open(schema_path) as f:
		raw_schema = json.load(f)

	match raw_schema:
		case dict():
			return raw_schema
		case _:
			raise TypeError


def read_csv(
	session: SparkSession, 
	data_path: Path,
	schema: StructType | None
) -> Result[DataFrame, PySparkException]:
	"""Read the specified csv file into a Spark dataframe. Returns the dataframe.

	Returns PySparkException if Spark fails to create a Dataframe

	Keyword arguments:
	session -- the apark session used to create a dataframe
	data_path -- a path that points at the csv file of data
	schema -- an optional pyspark schema
	"""

	# load a csv file checking the schema as the data is loaded
	if schema:
		return Ok(session \
			.read \
			.format("csv") \
			.option("header", True) \
			.schema(schema) \
			.option("mode", "FAILFAST") \
			.load(str(data_path))
		)
	else:
		return Ok(session \
			.read \
			.format("csv") \
			.option("header", True) \
			.option("inferSchema", True) \
			.load(str(data_path))
		)


def validate(session: SparkSession, df: DataFrame) -> Result[None, ConstraintError]:
	"""Add data quality constraints to the specified dataframe.
	
	Using pydeequ to perform the checks: https://github.com/awslabs/python-deequ

    Keyword arguments:
    session -- the apark session used to create a dataframe
    df -- a spark dataframe whose data needs validating
	"""

	match check_constraints(session, df):
		
		case Ok(check_results):
			return parse_check_results(check_results)
		
		case Err(e):
			return Err(ConstraintError(constraint=e.getErrorClass(), status="Failure"))

	return Ok(None)


@as_result(PySparkException)
def check_constraints(session: SparkSession, df: DataFrame) -> list[CheckResult]:
	"""Add and check data quality constraints on the specified dataframe.
	
	Using pydeequ to perform the checks: https://github.com/awslabs/python-deequ

    Keyword arguments:
    session -- the apark session used to create a dataframe
    df -- a spark dataframe whose data needs validating
	"""

    # create the check object
	check = Check(session, CheckLevel.Error, "Review Check")

	# define the "PassengerId" checks
	# the column should contain unique values, no nulls, no negative numbers 
	# and be an integer type
	checks = check \
		.isUnique("PassengerId") \
		.isComplete("PassengerId") \
		.isNonNegative("PassengerId") \
		.hasDataType("PassengerId", ConstrainableDataTypes.Integral)
	
	# define the "Name" checks
	# the column should contain unique values and no nulls
	checks = checks \
		.isComplete("Name") \
		.isUnique("Name")

	# define the "Ticket" checks
	# the column should contain no nulls
	checks = checks \
		.isComplete("Ticket")

	# define the "Pclass" checks
	# the column should contain no nulls, be an integer and contain only the 
	# values 1, 2 and 3
	checks = checks \
		.isContainedIn("Pclass", ["1", "2", "3"]) \
		.isComplete("Pclass") \
		.hasDataType("Pclass", ConstrainableDataTypes.Integral)

	# define the "Parch" checks
	# the column should contain no nulls, no negative numbers and be an integer type
	checks = checks \
		.isComplete("Parch") \
		.isNonNegative("Parch") \
		.hasDataType("Parch", ConstrainableDataTypes.Integral)

	# define the "Embarked" checks
	# the column should contain no nulls and only the values S, C and Q
	checks = checks \
		.isContainedIn("Embarked", ["S", "C", "Q"]) \
		.isComplete("Embarked")

	# define the "Age" checks
	# the column should contain no nulls, no negative numbers and be a float type
	checks = checks \
		.isComplete("Age") \
		.isNonNegative("Age") \
		.hasDataType("Age", ConstrainableDataTypes.Fractional)

	# define the "Cabin" checks
	# the column should contain no nulls
	checks = checks \
		.isComplete("Cabin")

	# define the "Fare" checks
	# the column should contain no nulls, no negative numbers and be a float type
	checks = checks \
		.isComplete("Fare") \
		.isNonNegative("Fare") \
		.hasDataType("Fare", ConstrainableDataTypes.Fractional)

	# define the "Sibsp" checks
	# the column should contain no nulls, no negative numbers and be an integer type
	checks = checks \
		.isComplete("SibSp") \
		.isNonNegative("SibSp") \
		.hasDataType("SibSp", ConstrainableDataTypes.Integral)

	# define the "Survived" checks
	# the column should contain no nulls, be an integer type and contain only the 
	# values 0 and 1
	checks = checks \
		.isContainedIn("Survived", ["0", "1"]) \
		.isComplete("Survived") \
		.hasDataType("Survived", ConstrainableDataTypes.Integral)

	# define the "Sex" checks
	# the column should contain no nulls and only the values male and female
	checks = checks \
		.isContainedIn("Sex", ["male", "female"]) \
		.isComplete("Sex")

	# perform the checks on the dataframe
	check_result = VerificationSuite(session) \
		.onData(df) \
		.addCheck(checks) \
		.run()

	# parse the check results as a list of dicts
	check_result_json: list[CheckResult] = VerificationResult\
		.checkResultsAsJson(session, check_result)

	return check_result_json


def parse_check_results(
	check_results: list[CheckResult]
) -> Result[None, ConstraintError]:
	"""Check if any of the constraints failed.

	Raise a ConstraintError if a check failed.

    Keyword arguments:
    session -- the apark session used to create a dataframe
    df -- a spark dataframe whose data needs validating
	"""
	for constraint in check_results:
		if constraint.get("constraint_status") != "Success":
			return Err(ConstraintError(
				constraint=constraint.get("constraint"), 
				status=constraint.get("constraint_status")
			))

	return Ok(None)


def load(df: DataFrame, filepath: Path) -> Result[None, PySparkException]:
	"""Write the specified dataframe to a file.

	Keyword arguments:
	df -- a spark dataframe containing the data to be written to file
	filepath -- a path that points to the location the data will be written to
	"""

	df \
		.write \
		.format("csv") \
		.option("header", True) \
		.mode('overwrite') \
		.save(str(filepath))

	return Ok(None)


def shutdown_spark(session: SparkSession) -> None:
	"""Cleanup any resources that spark reserved."""
	session.stop()


if __name__ == "__main__":
	main()