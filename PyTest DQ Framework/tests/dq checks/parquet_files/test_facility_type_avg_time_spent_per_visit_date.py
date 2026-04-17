"""
Description: Data Quality checks for facility_type_avg_time_spent_per_visit_date
Requirement(s): TICKET-1236
Author(s): Olha Karpenko
"""

import pytest
import pandas as pd

@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
        select f.facility_type, date(v.visit_timestamp) as visit_date, avg(v.duration_minutes) as avg_time_spent
        from visits v
        left join facilities f on f.id = v.facility_id
        group by date(v.visit_timestamp), f.facility_type
    """
    source_data = db_connection.get_data_sql(source_query)
    source_data['visit_date'] = pd.to_datetime(source_data['visit_date'])
    return source_data

@pytest.fixture(scope='module')
def target_data(parquet_reader):
    target_path = 'parquet_data/facility_type_avg_time_spent_per_visit_date'
    target_data = parquet_reader.process(target_path)
    return target_data

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    assert data_quality_library.check_dataset_is_not_empty(target_data), 'Empty target data set'


@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data,target_data)

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    data_quality_library.check_not_null_values(target_data,['facility_type',
                                                              'visit_date',
                                                              'avg_time_spent'])
#duplicates

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_duplicates(data_quality_library,target_data):
    data_quality_library.check_duplicates(target_data)



@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_data_full_data_set(target_data,source_data,data_quality_library):
    data_quality_library.check_data_full_data_set(target_data,source_data)

#columns completness
@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_data_column_completness(source_data, target_data, data_quality_library):
    data_quality_library.check_data_column_completness(source_data,target_data)

@pytest.mark.parquet_data
@pytest.mark.facility_type_avg_time_spent_per_visit_date
def test_check_data_rows_completness(source_data, target_data, data_quality_library):
    key_columns = ['facility_type', 'visit_date', 'avg_time_spent']
    data_quality_library.check_data_rows_completness(source_data,target_data,key_columns)