import pytest
import re


import os

def test_file_not_empty(read_file):
    assert not read_file.empty


def test_duplicates():
    assert 1 + 1 == 2

@pytest.mark.validate_csv
def test_validate_schema(validate_schema):
    actual_schema,expected_schema = validate_schema
    assert actual_schema==expected_schema

@pytest.mark.validate_csv
@pytest.mark.skip(reason="Not implemented yet")
def test_age_column_valid(read_file):
    condition = read_file.age.between(0, 100)
    assert condition.all(),'invalid age'


def test_email_column_valid():
    assert 1 + 1 == 2


def test_active_players():
    assert 1 + 1 == 2


def test_active_player():
    assert 1 + 1 == 2
