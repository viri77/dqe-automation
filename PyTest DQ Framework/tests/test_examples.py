"""
Description: Data Quality checks ...
Requirement(s): TICKET-1234
Author(s): Olha Karpenko
"""

import pytest
import pandas as pd


@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
        select f.facility_name,date(v.visit_timestamp ) as visit_date,min(v.duration_minutes ) as min_time_spent
        from visits v
        left join facilities f on f.id = v.facility_id
        group by date(v.visit_timestamp ),f.facility_name
    """
    source_data = db_connection.get_data_sql(source_query)
    source_data['visit_date'] = pd.to_datetime(source_data['visit_date'])
    return source_data


@pytest.fixture(scope='module')
def target_data(parquet_reader):
    target_path = 'parquet_data/facility_name_min_time_spent_per_visit_date'
    target_data = parquet_reader.process(target_path)
    return target_data


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    assert data_quality_library.check_dataset_is_not_empty(target_data),'Empty target data set'


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_count(source_data, target_data, data_quality_library):
    count_source,count_target = data_quality_library.check_count(source_data,target_data)
    assert count_source == count_target,(f'Source and target data are not equal, '
                                         f'in source: {count_source}, target: {count_target}')

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    check_nulls = data_quality_library.check_not_null_values(target_data,['facility_name',
                                                              'visit_date',
                                                              'min_time_spent'])
    assert check_nulls.empty, f'Nulls in target data set,in the fields: {check_nulls}'

#duplicates

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_duplicates_target(data_quality_library,target_data):
    duplicated_rows = data_quality_library.check_duplicates(target_data)
    cnt_duplicated = len(duplicated_rows)
    assert  duplicated_rows.empty,\
        f'Duplicates in target data set in the fields: {duplicated_rows},count duplicated_rows: {cnt_duplicated}'

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_duplicates_source(data_quality_library,source_data):
    duplicated_rows = data_quality_library.check_duplicates(source_data)
    assert  duplicated_rows.empty,f'Duplicates in source data set in the fields: {duplicated_rows}'

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_full_data_set(target_data,source_data,data_quality_library):
    assert data_quality_library.check_data_full_data_set(target_data,source_data),'Data sets are not matching'

#columns completness
@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_column_completness(source_data, target_data, data_quality_library):
    missing_columns = data_quality_library.check_data_column_completness(source_data,target_data)
    assert not missing_columns,f'Missing columns in target data set: {missing_columns}'

@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_rows_completness(source_data, target_data, data_quality_library):
    missing_rows= data_quality_library.check_data_rows_completness(source_data,target_data)
    assert missing_rows.empty,f'Missing columns in target data set: {missing_rows}'