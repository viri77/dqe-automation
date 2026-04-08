import pytest
import re


import os

def test_file_not_empty(read_file):
    df = read_file('src/data/data.csv')
    assert not df.empty

@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Known duplicates")
def test_duplicates(read_file):
    df = read_file('src/data/data.csv')
    duplicates = df[df.duplicated()]
    assert duplicates.empty, f"duplicated rows:\n{duplicates}"

@pytest.mark.validate_csv
def test_validate_schema(read_file,validate_schema):
    df = read_file('src/data/data.csv')
    expected_schema = ['id', 'name', 'age', 'email', 'is_active']
    actual_schema = list(df.columns)
    assert actual_schema==expected_schema,'invalid schema'

@pytest.mark.validate_csv
@pytest.mark.skip(reason="Not implemented yet")
def test_age_column_valid(read_file):
    df = read_file('src/data/data.csv')
    condition = df.age.between(0, 100)
    assert condition.all(),'age less than 0 or greater than 100'

@pytest.mark.validate_csv
def test_email_column_valid(read_file):
    df = read_file('src/data/data.csv')
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    valid = df['email'].apply(lambda x: bool(re.match(pattern, str(x))))
    assert valid.all(), f"invalid email(s): {df['email'][~valid].tolist()}"



@pytest.mark.parametrize("id, is_active", [
    (1, False),
    (2, True)
])
def test_active_players(read_file, id, is_active):
    df = read_file('src/data/data.csv')
    row = df[df['id'] == id]
    actual = row['is_active'].iloc[0]
    assert actual == is_active, f"id={id}: expected {is_active}, found {actual}"


def test_active_player(read_file):
    df = read_file('src/data/data.csv')
    is_active= df[df['id'] == 2]['is_active'].iloc[0]
    assert is_active ==True,f"is active for id=2 not true"
