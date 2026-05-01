*** Settings ***
Documentation     Validates that the HTML report table matches the Parquet dataset.
...               Test passes when data is identical; fails with a diff when mismatches exist.
Library           SeleniumLibrary
Library           helper.py


*** Variables ***
${REPORT_FILE}      ${CURDIR}/generated_report/report.html
${PARQUET_FOLDER}   ${CURDIR}/parquet_data/facility_type_avg_time_spent_per_visit_date
${FILTER_DATE}      2026-04-16
${BROWSER}          Chrome


*** Test Cases ***
Validate HTML Table Matches Parquet Data
    [Documentation]    Opens the HTML report in Chrome, reads the Plotly table into a
    ...                DataFrame, reads the Parquet dataset filtered by ${FILTER_DATE},
    ...                and asserts that both DataFrames are identical.
    [Teardown]    Close Browser

    ${file_url}=    Get File Url    ${REPORT_FILE}
    Open Browser    ${file_url}    ${BROWSER}
    Maximize Browser Window

    Wait Until Element Is Visible    css:.plotly-graph-div    timeout=15s
    ${table_element}=    Get WebElement    css:.plotly-graph-div

    ${html_df}=    Read Html Table    ${table_element}
    ${html_df}=    Rename Dataframe Columns    ${html_df}    average_time_spent=avg_time_spent

    ${parquet_df}=    Read Parquet Data    ${PARQUET_FOLDER}    ${FILTER_DATE}

    ${match}    ${diff}=    Compare Dataframes    ${html_df}    ${parquet_df}

    IF    not ${match}
        Fail    Data mismatch between HTML table and Parquet dataset:\n${diff}
    END
