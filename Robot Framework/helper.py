import base64
import struct

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path


def read_html_table(table_element):
    """Read Plotly table trace from a Selenium WebElement into a pandas DataFrame.

    Expects a WebElement pointing to the Plotly graph div. Uses JavaScript to
    retrieve the embedded figure data and extracts the first 'table' trace.
    Column names are normalised to snake_case.
    """
    driver = table_element.parent
    figure_data = driver.execute_script("return arguments[0].data;", table_element)

    if not figure_data:
        raise ValueError("No Plotly data found on element")

    table_trace = next((t for t in figure_data if t.get("type") == "table"), None)
    if not table_trace:
        raise ValueError("No Plotly 'table' trace found in figure data")

    headers = table_trace.get("header", {}).get("values", [])
    cell_values = table_trace.get("cells", {}).get("values", [])

    if not headers:
        headers = [f"col_{i}" for i in range(len(cell_values))]

    df = pd.DataFrame(dict(zip(headers, cell_values)))
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df


def read_parquet_data(folder, filter_date=None, window_days=7):
    """Read a partitioned Parquet dataset with optional date filtering.

    folder: path to the root of the partitioned Parquet dataset.
    filter_date: YYYY-MM-DD string. When provided, rows are filtered to the
                 ``window_days``-day window ending on this date (inclusive).
    window_days: number of days in the rolling window (default 7).
    """
    dataset = pq.ParquetDataset(folder)
    df = dataset.read().to_pandas()

    if filter_date:
        end_date = pd.Timestamp(filter_date)
        start_date = end_date - pd.Timedelta(days=window_days - 1)
        visit_dates = pd.to_datetime(df["visit_date"])
        df = df[(visit_dates >= start_date) & (visit_dates <= end_date)]

    partition_cols = [c for c in df.columns if "partition" in c.lower()]
    return df.drop(columns=partition_cols).reset_index(drop=True)


def compare_dataframes(df1, df2):
    """Compare two DataFrames for an exact match after normalising types and sort order.

    Timestamps are converted to 'YYYY-MM-DD' strings; floats are rounded to 2 dp.
    Both DataFrames are sorted by all columns so row order does not matter.

    Returns a tuple (is_match: bool, message: str | None).
    message is None when the DataFrames are identical, otherwise contains a
    human-readable description of the differences.
    """
    df1 = _normalize_df(df1.copy())
    df2 = _normalize_df(df2.copy())

    if df1.shape != df2.shape:
        return False, (
            f"Shape mismatch — df1: {df1.shape}, df2: {df2.shape}\n\n"
            f"df1 sample:\n{df1.head(5).to_string()}\n\n"
            f"df2 sample:\n{df2.head(5).to_string()}"
        )

    if set(df1.columns) != set(df2.columns):
        return False, (
            f"Column mismatch — df1: {list(df1.columns)}, df2: {list(df2.columns)}"
        )

    cols = sorted(df1.columns.tolist())
    df1 = df1[cols].sort_values(cols).reset_index(drop=True)
    df2 = df2[cols].sort_values(cols).reset_index(drop=True)

    if df1.equals(df2):
        return True, None

    try:
        diff = df1.compare(df2, result_names=("df1", "df2"), align_axis=0)
        return False, f"Differences found:\n{diff.to_string()}"
    except Exception as exc:
        return False, f"DataFrames differ (comparison error: {exc})"


def rename_dataframe_columns(df, **column_mapping):
    """Return a copy of df with columns renamed according to column_mapping.

    Robot Framework usage:
        ${df}=    Rename Dataframe Columns    ${df}    old_name=new_name
    """
    return df.rename(columns=column_mapping)


def read_html_pie_chart(element):
    """Read Plotly pie trace from a Selenium WebElement into a pandas DataFrame.

    Returns a DataFrame with columns: facility_type, min_avg_time_spent.
    Handles Plotly's base64-encoded typed array format for values.
    """
    driver = element.parent
    figure_data = driver.execute_script("return arguments[0].data;", element)

    if not figure_data:
        raise ValueError("No Plotly data found on element")

    pie_trace = next((t for t in figure_data if t.get("type") == "pie"), None)
    if not pie_trace:
        raise ValueError("No Plotly 'pie' trace found in figure data")

    labels = pie_trace.get("labels", [])
    values = pie_trace.get("values", [])

    if isinstance(values, dict) and "bdata" in values:
        values = _decode_plotly_typed_array(values)

    return pd.DataFrame({"facility_type": labels, "min_avg_time_spent": values})


def compute_min_avg_by_group(df, group_col="facility_type", value_col="avg_time_spent"):
    """Return a DataFrame with the minimum value per group.

    Returns columns: {group_col}, min_avg_time_spent.
    """
    result = df.groupby(group_col)[value_col].min().reset_index()
    result.columns = [group_col, "min_avg_time_spent"]
    return result


def get_file_url(path):
    """Convert an absolute or relative file path to a file:// URL.

    Resolves the path relative to the current working directory, making it safe
    to use with both absolute paths and Robot Framework ${CURDIR}-based paths.
    """
    return Path(path).resolve().as_uri()


def _decode_plotly_typed_array(typed_array):
    """Decode a Plotly typed array dict {"dtype": "f8", "bdata": "..."} to a list."""
    fmt_map = {"f8": "d", "f4": "f", "i4": "i", "i2": "h", "u1": "B"}
    fmt_char = fmt_map.get(typed_array.get("dtype", "f8"), "d")
    raw = base64.b64decode(typed_array["bdata"])
    n = len(raw) // struct.calcsize(fmt_char)
    return list(struct.unpack(f"<{n}{fmt_char}", raw))


def _normalize_df(df):
    """Convert timestamp columns to 'YYYY-MM-DD' strings and round floats to 2 dp."""
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].round(2)
    return df
