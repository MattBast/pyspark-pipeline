"""data_fixture.

Create re-usable data and data files for the tests.
"""

# test imports
import pytest

# standard imports
import os
from pathlib import Path
from typing import Generator

@pytest.fixture(scope="session")
def csv_path() -> Generator[Path, None, None]:
    """ fixture for creating a csv file and returning the path"""

    file_path = Path("./tests/data/minimal.csv")
    
    # write test data to a file
    with open(file_path, "w") as f:
        f.write("id,label\n1,foo\n2,bar")

    # make the file available to the tests
    yield file_path

    # delete the schema file during teardown
    os.remove(file_path)