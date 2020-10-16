import time
import numpy as np
import pandas as pd
from tqdm import tqdm
from src.raw_data import get_single_year
from src.process_data import (
    get_reference_columns,
    split_column_types,
    rename_status_and_unit_columns,
)
from src.summary_plots import (
    plot_measurement_summary,
    plot_status_summary,
    plot_unit_summary,
)

SLEEP_DURATION = 0.75


def preview_data(
    site_id,
    start_year,
    end_year,
    nrows=8,
    header_lines=4,
    fixed_url="https://uk-air.defra.gov.uk/data_files/site_data/",
    sep="_",
    file_format="csv",
):
    """
    Returns a sample of air pollution data from uk-air.defra.gov.uk/ for input location and a list of available years.

    The function attempts to read data from the URL defined with function arguments for all years between input
    'start_year' and 'end_year'. A sample of data from the most recent available year is returned as a pandas DataFrame.
    The function also returns a list of all years (between 'start_year' and 'end_year') for which data was successfully
    located at the defined URL. An assertion error is thrown if no data is successfully accessed at the URL.

    Parameters
    ----------
    site_id : str
        The consistent identifier associated with the air pollution measurement site of interest e.g. 'OX8'.
    start_year : int
        The earliest year of air pollution data to be returned for the chosen measurement site e.g. '2015'.
    end_year : int
        The latest year of air pollution data to be returned for the chosen measurement site e.g. '2018'.
    nrows : int
        The number of rows to be returned in the preview DataFrame; default value = 8.
    header_lines : int
        The row number (in raw .csv file) to use as column titles in the output DataFrame; default value = 4.
    fixed_url : str
        The consistent URL string to which 'site_id' and 'year' are appended in order to fetch data from the Defra site;
        by default, the fixed URL is: 'https://uk-air.defra.gov.uk/data_files/site_data/'.
    sep : str
        The consistent separator that splits 'site_id' and 'year' in URLs targeted to download data from the Defra site;
        by default, the separator is an underscore: '_'.
    file_format : str
        The file format to be downloaded from the Defra site; defines the file extension appended to the end of the
        queried request URL; by default, data is available and downloaded as 'csv' files.

    Returns
    -------
    preview_data : pandas.DataFrame
        Returns preview sample of air pollution data as a pandas DataFrame if available, otherwise returns None.
    valid_years : list of int
        A list of those years for which data was successfully downloaded.
    """
    preview_data = None
    preview_year = None
    years_of_interest = list(
        reversed(np.arange(start_year, end_year + 1))
    )  # Sorts list reverse chronologically
    valid_years = years_of_interest.copy()

    # Loops through each individual year of interest
    for indv_year in tqdm(years_of_interest, desc="Generating data preview: "):
        single_year = get_single_year(
            site_id,
            indv_year,
            nrows=nrows,
            header_lines=header_lines,
            fixed_url=fixed_url,
            sep=sep,
            file_format=file_format,
        )

        # if data cannot be downloaded for a particular year it is removed from the list of valid years
        if single_year is None:
            valid_years.remove(indv_year)
        elif preview_data is None:
            preview_data = single_year.copy()
            preview_year = indv_year
        else:
            pass  # once a set of preview data has been defined, it is not re-defined on subsequent loops

        time.sleep(
            SLEEP_DURATION
        )  # creates interval between requests to uk-air.defra.gov.uk

    assert preview_data is not None, (
        f"Could not read data from: {fixed_url} "
        f"for any years: {start_year} - {end_year}\n"
        f"Check the URL to ensure location code, years and file extension are all valid!"
    )

    print(f"Preview data sampled from {preview_year}")

    return preview_data, valid_years


def monitoring_site_summary(
    site_id,
    years_of_interest,
    header_lines=4,
    fixed_url="https://uk-air.defra.gov.uk/data_files/site_data/",
    sep="_",
    file_format="csv",
    status_str="status",
    unit_str="unit",
    status_offset=-1,
    unit_offset=-2,
):
    """
    Returns a dictionary containing each individual year's full set of air pollution data and summaries comparing years.

    Dictionary keys are the single year of interest, provided as an integer; corresponding value is that year's full
    air pollution DataFrame. Three summary tables are returned comparing measurements, statuses and units across
    years of interest respectively.

    Parameters
    ----------
    site_id : str
        The consistent identifier associated with the air pollution measurement site of interest e.g. 'OX8'.
    years_of_interest : list of int
        The years of data that are of interest, data for each available year in this list is downloaded and summarised.
    header_lines : int
        The row number (in raw .csv file) to use as column titles in the output DataFrame; default value = 4.
    fixed_url : str
        The consistent URL string to which 'site_id' and 'year' are appended in order to fetch data from the Defra site;
        by default, the fixed URL is: 'https://uk-air.defra.gov.uk/data_files/site_data/'.
    sep : str
        The consistent separator that splits 'site_id' and 'year' in URLs targeted to download data from the Defra site;
        by default, the separator is an underscore: '_'.
    file_format : str
        The file format to be downloaded from the Defra site; defines the file extension appended to the end of the
        queried request URL; by default, data is available and downloaded as 'csv' files.
    status_str : str
        The consistent string appearing in all metadata column titles that contain status information for a
        corresponding measurement column; default value = 'status'.
    unit_str : str
        The consistent string appearing in all metadata column titles that contain the unit associated with a
        corresponding measurement column; default value = 'unit'.
    status_offset : int
        The relative offset between measurement column position and its corresponding status column position;
        default value = -1
        i.e. the measurement column appears directly before its corresponding status column
    unit_offset : int
        The relative offset between measurement column position and its corresponding unit column position;
        default value = -2
        i.e. the measurement column appears two columns before its corresponding unit column

    Returns
    -------
    data_dict : dict of pandas.DataFrame
        A dictionary containing each year's full set of air pollution data, accessed using a year of data as the key.
    measurement_summary : pandas.DataFrame
        A summary DataFrame reporting presence/absence of different measurement columns for all years of data.
    status_summary : pandas.DataFrame
        A summary DataFrame reporting consistency/missingness within status columns for all years of data.
    unit_summary : pandas.DataFrame
        A summary DataFrame reporting consistency/missingness within unit columns for all years of data.
    """
    reference_cols = get_reference_columns(
        site_id,
        years_of_interest,
        status_str=status_str,
        unit_str=unit_str,
        status_offset=status_offset,
        unit_offset=unit_offset,
    )  # extracts a set of reference columns from a single

    measurement_cols, status_cols, unit_cols = split_column_types(
        reference_cols, status_str=status_str, unit_str=unit_str
    )  # from all reference columns, returns separate lists of measurement, status and unit columns

    year_col_title = "Data Set (Year)"  # defines the column title for column holding data years in summary tables

    # creates empty summary table to hold 'missingness' data for each measurement column and year
    measurement_summary = create_empty_summary(
        measurement_cols, years_of_interest, year_col_title
    )

    # creates empty summary table to hold 'missingness' and consistency data for each status column and year
    status_summary = create_empty_summary(
        status_cols, years_of_interest, year_col_title
    )

    # creates empty summary table to hold 'missingness' and consistency data for each unit column and year
    unit_summary = create_empty_summary(unit_cols, years_of_interest, year_col_title)

    # creates empty DataFrames to store count of unique status/unit values for each column-year combination
    unique_status_counts = create_empty_summary(
        status_cols, years_of_interest, year_col_title
    )
    unique_unit_counts = create_empty_summary(
        unit_cols, years_of_interest, year_col_title
    )

    data_dict = (
        {}
    )  # creates an empty dictionary that will be filled with full DataFrames of data for each year

    for idx, indv_year in tqdm(
        enumerate(years_of_interest),
        desc="Generating summary tables: ",
        total=len(years_of_interest),
    ):
        single_year = get_single_year(
            site_id,
            indv_year,
            header_lines=header_lines,
            fixed_url=fixed_url,
            sep=sep,
            file_format=file_format,
        )  # downloads full data set for single year of data (or returns null if no data found at specified URL)
        if single_year is None:
            # where data could not be accessed, marks summary DataFrames accordingly with: 'no data'
            measurement_summary = mark_invalid_year(
                measurement_summary, indv_year, idx, year_col_title
            )
            status_summary = mark_invalid_year(
                status_summary, indv_year, idx, year_col_title
            )
            unit_summary = mark_invalid_year(
                unit_summary, indv_year, idx, year_col_title
            )

            # where data could not be accessed, marks summary count DataFrames accordingly with: '0.0'
            unique_status_counts = mark_invalid_year(
                unique_status_counts, indv_year, idx, year_col_title, count_only=True
            )
            unique_unit_counts = mark_invalid_year(
                unique_unit_counts, indv_year, idx, year_col_title, count_only=True
            )
        else:
            single_year = rename_status_and_unit_columns(
                single_year,
                status_str=status_str,
                unit_str=unit_str,
                status_offset=status_offset,
                unit_offset=unit_offset,
            )  # where data could be accessed, renames metadata columns linking to corresponding measurement column

            single_year_cols = single_year.columns.tolist()

            sy_measurement_cols, sy_status_cols, sy_unit_cols = split_column_types(
                single_year_cols, status_str=status_str, unit_str=unit_str
            )  # from all reference cols for a single year, returns separate lists of measurement, status and unit cols

            measurement_summary = fill_measurement_summary_row(
                measurement_summary, indv_year, idx, sy_measurement_cols, year_col_title
            )
            status_summary = fill_status_summary_row(
                status_summary,
                indv_year,
                idx,
                single_year,
                sy_status_cols,
                year_col_title,
            )
            unit_summary = fill_unit_summary_row(
                unit_summary, indv_year, idx, single_year, sy_unit_cols, year_col_title
            )

            unique_status_counts = fill_status_summary_row(
                unique_status_counts,
                indv_year,
                idx,
                single_year,
                sy_status_cols,
                year_col_title,
                count_only=True,
            )
            unique_unit_counts = fill_unit_summary_row(
                unique_unit_counts,
                indv_year,
                idx,
                single_year,
                sy_unit_cols,
                year_col_title,
                count_only=True,
            )

            data_dict[indv_year] = single_year

        time.sleep(
            SLEEP_DURATION
        )  # creates interval between requests to uk-air.defra.gov.uk

    # fills NaN values in summary tables where reference year's columns did not cover all possible columns across years
    measurement_summary.fillna(value=False, inplace=True)
    unique_status_counts.replace(to_replace=False, value=0.0, inplace=True)
    unique_status_counts.fillna(value=0.0, inplace=True)
    unique_unit_counts.replace(to_replace=False, value=0.0, inplace=True)
    unique_unit_counts.fillna(value=0.0, inplace=True)

    # plots heatmap representations of the summary tables
    plot_measurement_summary(measurement_summary, year_col_title)
    plot_status_summary(unique_status_counts, year_col_title)
    plot_unit_summary(unique_unit_counts, year_col_title)

    return data_dict, measurement_summary, status_summary, unit_summary


def create_empty_summary(summary_cols, years_of_interest, year_col_title):
    """
    Returns empty summary table to hold 'missingness' and consistency data for an input set of columns.

    Parameters
    ----------
    summary_cols : list of str
        The full list of column titles for which a summary DataFrame will be created.
    years_of_interest : list of int
        The years of data that are of interest, used to define how many rows are required in the summary DataFrame.
    year_col_title : str
        The title of the summary DataFrame's column reporting years of interest.

    Returns
    -------
    summary_df : pandas.DataFrame
        A DataFrame with the first column reporting each year of interest; subsequent columns correspond to each input
        column and contain value = False for all rows; this summary df can later be populated by looping through years
        of interest and their available columns: updating values in the summary df to True where that combination of
        year and column title exists.
    """
    summary_dict = {year_col_title: "blank"}
    missing_column_placeholders = {col: False for col in summary_cols}
    summary_dict.update(missing_column_placeholders)
    summary_df = pd.DataFrame(
        summary_dict, index=[idx for idx in range(len(years_of_interest))]
    )

    return summary_df


def mark_invalid_year(
    input_summary, invalid_year, row_idx, year_col_title, count_only=False
):
    """
    Returns summary table updated to report 'no data' for measurement/status/unit columns for an input 'invalid_year'.

    Parameters
    ----------
    input_summary : pandas.DataFrame
        A partially-filled summary DataFrame for measurement, status or unit columns.
    invalid_year : int
        The year for which air pollution data could not be accessed, this year will be marked as unavailable in the
        summary DataFrame.
    row_idx : int
        The row index corresponding to the invalid/inaccessible year of data in the summary DataFrame.
    year_col_title : str
        The title of the summary DataFrame's column reporting years of interest.
    count_only : bool
        Boolean flag controlling output type, False populates each column for the invalid input year of data with
        the 'invalid_year_str' string; True instead populates the summary table elements with an integer value of zero;
        default argument value = False.

    Returns
    -------
    output_summary : pandas.DataFrame
        If 'count_only' = False, the input summary DataFrame is updated to report 'no data' for all possible
        measurement/status/unit columns in the invalid/inaccessible year of data. If 'count_only' = True, the input
        summary DataFrame is updated to report zero as an integer instead: this is used for status/unit summary plots.
    """
    invalid_year_str = "no data"  # defines consistent value for elements in summary table where year of data is missing
    output_summary = input_summary.copy()
    if count_only:
        output_summary.loc[row_idx, :] = 0.0
    else:
        output_summary.loc[row_idx, :] = invalid_year_str
    output_summary.loc[row_idx, year_col_title] = invalid_year

    return output_summary


def fill_measurement_summary_row(
    input_summary, year, row_idx, measurement_cols, year_col_title
):
    """
    Using the measurement columns for a single year of data, populates that year's row in the measurement summary table.

    Parameters
    ----------
    input_summary : pandas.DataFrame
        A partially-filled summary DataFrame for measurement columns.
    year : int
        The year to be filled in the summary DataFrame.
    row_idx : int
        The row index corresponding to the input year for which summary data will be entered.
    measurement_cols : str
        A list of all measurement column titles appearing in the data set for the single input year.
    year_col_title : str
        The title of the summary DataFrame's column reporting years of interest.

    Returns
    -------
    output_summary : pandas.DataFrame
        The input summary DataFrame updated to report 'True' for all measurement columns that are present in the input
        year's data set.
    """
    output_summary = input_summary.copy()
    output_summary.loc[row_idx, year_col_title] = year

    # adds/updates measurement columns extracted from the current year's data set in the summary table as True (present)
    for col in measurement_cols:
        output_summary.loc[row_idx, col] = True

    return output_summary


def fill_status_summary_row(
    input_summary,
    year,
    row_idx,
    single_year_data,
    status_cols,
    year_col_title,
    count_only=False,
):
    """
    Using the status columns for a single year of data, populates that year's row in the status summary table.

    Parameters
    ----------
    input_summary : pandas.DataFrame
        A partially-filled summary DataFrame for status columns.
    year : int
        The year to be filled in the summary DataFrame.
    row_idx : int
        The row index corresponding to the input year for which summary data will be entered.
    single_year_data : pandas.DataFrame
        The full air pollution data set for the input year.
    status_cols : str
        A list of all status column titles appearing in the data set for the single input year.
    year_col_title : str
        The title of the summary DataFrame's column reporting years of interest.
    count_only : bool
        Boolean flag controlling output type, True outputs the unique value count (excl. NaN) for each status column in
        the input year of data; False outputs verbose information quoting consistent column values and presence/absence
        of NaN values; default argument value = False.

    Returns
    -------
    output_summary : pandas.DataFrame
        The input summary DataFrame updated for the input year to report (when count_only=False):
            - The single status value observed in a column (if only a single value is observed in that column)
            - The number of different status values observed in a column (if > 1 values are observed in that column)
            - The presence of NaN values in otherwise single-valued columns
        When count_only=True, DataFrame is updated for the input year to report the number of unique status values in
        each column.
    """
    output_summary = input_summary.copy()
    output_summary.loc[row_idx, year_col_title] = year
    for col in status_cols:
        unique_count = single_year_data[col].nunique(dropna=True)
        if count_only:
            output_summary.loc[row_idx, col] = float(unique_count)
        else:
            unique_count_with_nan = single_year_data[col].nunique(dropna=False)
            if unique_count == 1 and unique_count_with_nan == 2:
                single_real_value = single_year_data.loc[
                    single_year_data[col].first_valid_index(), col
                ]  # extracts the first non-NaN value from the column
                output_summary.loc[row_idx, col] = f"{single_real_value}  (+ NaNs)"
            elif unique_count == 1 and unique_count_with_nan == 1:
                output_summary.loc[row_idx, col] = single_year_data.loc[0, col]
            else:
                output_summary.loc[row_idx, col] = f"{unique_count} different values"

    return output_summary


def fill_unit_summary_row(
    input_summary,
    year,
    row_idx,
    single_year_data,
    unit_cols,
    year_col_title,
    count_only=False,
):
    """
    Using the unit columns for a single year of data, populates that year's row in the unit summary table.

    Parameters
    ----------
    input_summary : pandas.DataFrame
        A partially-filled summary DataFrame for unit columns.
    year : int
        The year to be filled in the summary DataFrame.
    row_idx : int
        The row index corresponding to the input year for which summary data will be entered.
    single_year_data : pandas.DataFrame
        The full air pollution data set for the input year.
    unit_cols : str
        A list of all unit column titles appearing in the data set for the single input year.
    year_col_title : str
        The title of the summary DataFrame's column reporting years of interest.
    count_only : bool
        Boolean flag controlling output type, True outputs the unique value count (excl. NaN) for each unit column in
        the input year of data; False outputs verbose information quoting consistent column values and presence/absence
        of NaN values; default argument value = False.

    Returns
    -------
    output_summary : pandas.DataFrame
        The input summary DataFrame updated for the input year to report (when count_only=False):
            - The single unit value observed in a column (if only a single value is observed in that column)
            - The number of different unit values observed in a column (if > 1 values are observed in that column)
            - The presence of NaN values in otherwise single-valued columns
        When count_only=True, DataFrame is updated for the input year to report the number of unique unit values in
        each column.
    """
    output_summary = input_summary.copy()
    output_summary.loc[row_idx, year_col_title] = year
    for col in unit_cols:
        unique_count = single_year_data[col].nunique(dropna=True)
        if count_only:
            output_summary.loc[row_idx, col] = float(unique_count)
        else:
            unique_count_with_nan = single_year_data[col].nunique(dropna=False)
            if unique_count == 1 and unique_count_with_nan == 2:
                single_real_value = single_year_data.loc[
                    single_year_data[col].first_valid_index(), col
                ]  # extracts the first non-NaN value from the column
                output_summary.loc[row_idx, col] = f"{single_real_value}  (+ NaNs)"
            elif unique_count == 1 and unique_count_with_nan == 1:
                output_summary.loc[row_idx, col] = single_year_data.loc[0, col]
            else:
                output_summary.loc[row_idx, col] = f"{unique_count} different values"

    return output_summary
