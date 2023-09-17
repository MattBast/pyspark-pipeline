# Pyspark practice
This repository is a simple extract-validate-load pipeline that as the name suggests, loads data from a file, checks the table against constraints and then writes the file to another file. The idea was to practice some coding best practices including testing, typing, error handling and linting. The pipeline is built using pyspark and the validations are performed using pydeequ.

> Note that this codebase is still very much a work in progress so may not work as expected yet.

## Environment setup
Spark needs to be installed on the local machine before this pipeline can be run. This tutorial was followed to produce these install instructions: https://sparkbyexamples.com/pyspark/how-to-install-pyspark-on-mac/

Start by installing Homebrew:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

And then install OpenJDK 11, Temurin and Spark:
```bash
brew install openjdk@11
brew install temurin
brew install apache-spark
```

Check to make sure Pyspark is installed:
```bash
pyspark
```

Once the python shell opens, try out this code:
```python
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
df = spark.createDataFrame(data)
df.show()
```

Now the python virtual environment can be created to isolate the pip installs. Create an environment called `pyspark_env`:
```bash
python3 -m venv .pyspark_env
```

And activate it:
```bash
source .pyspark_env/bin/activate
```

Install all the required python packages:
```bash
python3 -m pip install -r requirements.txt
```

Once you've finished developing and testing the pipeline, make sure to deactivate the environment.
```bash
deactivate
```

## Best practices
To ensure the pipeline is reliable the following practices have been followed:

### Types check
Type hints are included all the way through this codebase and they are checked by mypy. Use this command to check the types as well as the syntax in the codebase:
```bash
python3 -m mypy ./src/main.py --install-types --strict
```

### Lint
`Ruff` is used to maintain a consistent and concise codebase.
```bash
python3 -m ruff check ./src/main.py
```

### Test
Pytest is used to test the pipelines functionality. A fixture has been created to create a test spark session.
```bash
python3 -m pytest --disable-warnings
```

### Error handling
To keep error handling concise the [result](https://pypi.org/project/result/) library has been used to handle errors much like the Rust programing language does. Combined with the match statement introduced in python v3.10+ the result library encourages an "exhaustive" handling of all possible error types returned by a function.

### Data quality checks
The [pydeequ](https://pydeequ.readthedocs.io/en/latest/) library is used to add SQL like constraints to the table loaded. This checks the loaded data against these constraint rules which effectively checks the data quality. 

### Run
Use this command to run the pipeline:
```bash
python3 ./src/main.py
```