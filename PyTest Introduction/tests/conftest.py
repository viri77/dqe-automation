import pytest
import pandas as pd
import os

# Fixture to read the CSV file
@pytest.fixture()
def read_file():
    test_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(test_dir, '..'))
    file_path = os.path.join(project_root, 'src', 'data', 'data.csv')
    with open(file_path, 'r') as f:
        first_line = f.readline()
    return first_line

# Fixture to validate the schema of the file


# Pytest hook to mark unmarked tests with a custom mark
