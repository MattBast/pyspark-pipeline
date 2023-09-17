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

# custom error classes
from result import Result, Ok, Err, as_result

# pyspark imports
os.environ["SPARK_VERSION"] = "3.3"
from pyspark.sql import SparkSession, DataFrame # noqa: E402
from pyspark.sql.types import StructType # noqa: E402
from pyspark.errors import PySparkException # noqa: E402

# deequ imports
import pydeequ # noqa: E402
from pydeequ.checks import Check, CheckLevel # noqa: E402
from pydeequ.verification import VerificationSuite, VerificationResult # noqa: E402


class ConstraintError(Exception):
    """A custom error used to report that a dataframe has failed a constraint 
    check.

    Keyword arguments:
    message -- developer defind message for the error
    """

    def __init__(self, constraint: str, status: str):
        self.constraint = constraint
        self.status = status

        message = \
            "Constraint " + constraint + \
            " returned status " + status + \
            "."

        super().__init__(message)


def main() -> None:
	"""Coordinates the data extraction, validation and loading."""

	match start_spark():
		case Ok(session):
			print("Spark started.")
		case Err(e):
			print(e)
			return

	match extract(session, Path("./src/schema.json"), Path("./data/titanic.csv")):
		case Ok(df):
			print("Data extracted.")
		case Err(e):
			print(e)
			return

	match validate(session, df):
		case Ok(None):
			print("Dataframe validated.")
		case Err(e):
			print(e)
			return


	match load(df, Path("./data/valid_titanic.csv")):
		case Ok(None):
			print("Data written to destination.")
		case Err(e):
			print(e)
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


@as_result(OSError, JSONDecodeError, PySparkException)
def extract(session: SparkSession, schema_path: Path, data_path: Path) -> DataFrame:
	"""Read the specified csv file into a Spark dataframe. Returns the dataframe.

	Returns OSError if the schema file fails to open.
	Returns JSONDecodeError if the schema file can't be parsed to json.
	Returns PySparkException if Spark fails to create a Dataframe

	Keyword arguments:
	session -- the apark session used to create a dataframe
	schema_path -- a path that points at the tables schema
	data_path -- a path that points at the csv file of data
	"""

	# load the datasets schema
	with open(schema_path) as f:
		raw_schema = json.load(f)
	
	schema = StructType.fromJson(raw_schema)

	# load a csv file checking the schema as the data is loaded
	df = session \
		.read \
		.format("csv") \
		.option("header","true") \
		.schema(schema) \
		.load(str(data_path))

	return df


@as_result(Exception)
def validate(session: SparkSession, df: DataFrame) -> None:
	"""Add data quality constraints to the specified dataframe.
	
	Using pydeequ to perform the checks: https://github.com/awslabs/python-deequ

    Keyword arguments:
    session -- the apark session used to create a dataframe
    df -- a spark dataframe whose data needs validating
	"""

    # create the check object
	check = Check(session, CheckLevel.Warning, "Review Check")

	# perform the checks
	check_result = VerificationSuite(session) \
		.onData(df) \
		.addCheck(
			check.isUnique("PassengerId") \
			.isContainedIn("Survived", ["0", "1"]) \
			.isContainedIn("Pclass", ["1", "2", "3"]) \
			.isContainedIn("Sex", ["male", "female"])
		).run()

	# parse the check results as a list of dicts
	check_result_json = VerificationResult.checkResultsAsJson(session, check_result)

	# check if any of the constraints failed
	for constraint in check_result_json:
		if constraint.get("constraint_status") != "Success":
			raise ConstraintError(
				constraint=constraint.get("constraint"), 
				status=constraint.get("constraint_status")
			)


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