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

    # define the data files path and create its parent directory if it doesn't exist
    file_path = Path("./tests/data/minimal.csv")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # write test data to a file
    with open(file_path, "w") as f:
        f.write("id,label\n1,foo\n2,bar")

    # make the file available to the tests
    yield file_path

    # delete the schema file during teardown
    os.remove(file_path)


@pytest.fixture(scope="session")
def write_dir_path() -> Generator[Path, None, None]:
    """fixture for creating a directory that tests can write files to."""

    # create the directory
    dir_path = Path("./tests/write_dir")
    dir_path.mkdir(parents=True, exist_ok=True)

    # make the file available to the tests
    yield dir_path

    # delete the schema file during teardown
    rmdir(dir_path) 


def rmdir(directory: Path):
    """equivalent to running the `rm -r` bash prompt."""

    for item in directory.iterdir():
        if item.is_dir():
            rmdir(item)
        else:
            item.unlink()

    directory.rmdir()