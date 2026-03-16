import pytest
import pandas as pd
import os

# Fixture to read the CSV file
@pytest.fixture(scope='session')
def read_file():
    test_dir = os.path.dirname(__file__)  # tests/
    project_root = os.path.abspath(os.path.join(test_dir, '..'))  # PyTest Introduction/
    file_path = os.path.join(project_root, 'src', 'data', 'data.csv')
    df = pd.read_csv(file_path)
    return df

# Fixture to validate the schema of the file
@pytest.fixture(scope='session',params = ['actual_schema','expected_schema'])
def validate_schema(read_file):
    actual_schema = list(read_file.columns)
    expected_schema = ['id', 'name', 'age', 'email', 'is_active']
    return actual_schema, expected_schema




# Pytest hook to mark unmarked tests with a custom mark
