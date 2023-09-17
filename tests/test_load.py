"""load_tests.

Test the scripts ability to write data to a csv file.
"""

# test imports
import pytest
from tests.fixtures.spark import spark_session
from tests.fixtures.data import write_dir_path

# source code imports (to be tested)
from src.main import load

# standard imports
from pathlib import Path
from pyspark.sql.types import StructType, IntegerType, StringType

@pytest.mark.usefixtures("spark_session", "write_dir_path")
def test_load(spark_session, write_dir_path):

    test_df = spark_session.createDataFrame(
        [(1, "foo"),(2, "bar")], # data
        "id int, label string"  # schema
    )

    result = load(df=test_df, filepath=write_dir_path)

    # get list of all csv files in the test write directory
    csv_paths = [sub_path for sub_path in write_dir_path.iterdir() if sub_path.suffix == ".csv"]
    
    assert result.is_ok()
    assert len(csv_paths) == 1 or len(csv_paths) == 2

    # check content of all files read
    for path in csv_paths:
        with open(path, "r") as f:
            file_content = str(f.read())
            assert file_content == "id,label\n1,foo\n" \
                or file_content == "id,label\n2,bar\n" \
                or "id,label\n1,foo\n2,bar\n" 
