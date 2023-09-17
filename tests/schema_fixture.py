"""schema_fixture.

Create re-usable json files containing pyspark schemas.
"""

# test imports
import pytest

# standard imports
import json
import os
from pathlib import Path
from typing import Generator

@pytest.fixture(scope="session")
def schema_path() -> Generator[Path, None, None]:
    """ fixture for creating a json schema and returning the path"""

    schema_path = Path("./tests/schemas/minimal.json")

    # define the schema
    schema = {
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

    # convert into json
    json_schema = json.dumps(schema)

    # write schema to a file
    with open(schema_path, "w") as f:
        f.write(json_schema)

    # make the file available to the tests
    yield schema_path

    # delete the schema file during teardown
    os.remove(schema_path)


@pytest.fixture(scope="session")
def not_obj_json_path() -> Generator[Path, None, None]:
    """ fixture for creating a none object json schema and returning the path"""

    schema_path = Path("./tests/schemas/not_object.json")

    # define the schema and convert into json
    json_schema = json.dumps("id")

    # write schema to a file
    with open(schema_path, "w") as f:
        f.write(json_schema)

    # make the file available to the tests
    yield schema_path

    # delete the schema file during teardown
    os.remove(schema_path)