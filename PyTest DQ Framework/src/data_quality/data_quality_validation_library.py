import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df, column_names=None):
        if column_names:
            mask = df.duplicated(subset=column_names, keep='first')
            columns_str = ', '.join(column_names)
        else:
            mask = df.duplicated(keep='first')
            columns_str = ', '.join(df.columns)
        duplicated_rows = df[mask]
        cnt_duplicated = len(duplicated_rows)
        assert duplicated_rows.empty, (
            f'Duplicates found by columns: [{columns_str}].\n'
            f'Count: {cnt_duplicated}\n'
            f'Rows:\n{duplicated_rows}'
        )
    @staticmethod
    def check_count(df1, df2):
        count_source = len(df1)
        count_target = len(df2)
        assert count_source == count_target, (f'Source and target count are not equal, '
                                              f'in source: {count_source}, target: {count_target}')

    @staticmethod
    def check_data_sets_equality(df1, df2):
        assert df1.shape[1] == df2.shape[1], "Number of columns does not match"
        assert df1.shape[0] == df2.shape[0], "Number of rows does not match"
        df1_sorted = df1.sort_values(list(df1.columns)).reset_index(drop=True)
        df2_sorted = df2.sort_values(list(df2.columns)).reset_index(drop=True)
        assert df1_sorted.equals(df2_sorted), "Data content does not match"

    @staticmethod
    def check_dataset_is_not_empty(df):
        return not df.empty

    @staticmethod
    def check_not_null_values(df, column_names=None):
        if column_names:
            null_rows = df[df[column_names].isnull().any(axis=1)]
        else:
            null_rows = df[df.isnull().any(axis=1)]
        assert null_rows.empty, f'Nulls in target data set,in the fields: {null_rows}'

    @staticmethod
    def check_data_column_availability(source_data, target_data):
        missing_columns = set(source_data.columns) - set(target_data.columns)
        assert not missing_columns, f'Missing columns in target data set: {missing_columns}'

    @staticmethod
    def check_data_rows_completness(source_data, target_data,key_columns):
        merged = source_data.merge(target_data, on=key_columns, how='left', indicator=True)
        missing_rows = merged[merged['_merge'] == 'left_only']
        assert missing_rows.empty, f'Missing columns in target data set: {missing_rows}'
