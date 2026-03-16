import pytest
import re


import os

def test_file_not_empty(read_file):
    assert not read_file.empty

@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Known duplicates")
def test_duplicates(read_file):
    duplicates = read_file[read_file.duplicated()]
    assert duplicates.empty, f"duplicated rows:\n{duplicates}"

@pytest.mark.validate_csv
def test_validate_schema(validate_schema):
    actual_schema,expected_schema = validate_schema
    assert actual_schema==expected_schema,'invalid schema'

@pytest.mark.validate_csv
@pytest.mark.skip(reason="Not implemented yet")
def test_age_column_valid(read_file):
    condition = read_file.age.between(0, 100)
    assert condition.all(),'invalid age'

@pytest.mark.validate_csv
def test_email_column_valid(read_file):
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    valid = read_file['email'].apply(lambda x: bool(re.match(pattern, str(x))))
    assert valid.all(), f"invalid email(s): {read_file['email'][~valid].tolist()}"



@pytest.mark.parametrize("id, is_active", [
    (1, False),
    (2, True)
])
def test_active_players(read_file, id, is_active):
    row = read_file[read_file['id'] == id]
    actual = row['is_active'].iloc[0]
    assert actual == is_active, f"id={id}: expected {is_active}, found {actual}"


def test_active_player(read_file):
    is_active= read_file[read_file['id'] == 2]['is_active'].iloc[0]
    assert is_active ==True,f"is active for id=2 not true"
