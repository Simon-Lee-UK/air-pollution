import time
import numpy as np
import pandas as pd
from tqdm import tqdm
from src.raw_data import get_single_year
from src.process_data import get_reference_columns, split_column_types

sleep_time = 0.75


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
    years_of_interest = list(reversed(np.arange(start_year, end_year + 1)))  # Sorts list reverse chronologically
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

        time.sleep(sleep_time)  # creates interval between requests to uk-air.defra.gov.uk

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
    Desc

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
        Desc
    measurement_summary : pandas.DataFrame
        Desc
    status_summary : pandas.DataFrame
        Desc
    unit_summary : pandas.DataFrame
        Desc
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

    # creates empty summary table to hold 'missingness' data for each measurement column and year
    m_summary_dict = {"Data (Year)": "blank"}
    indv_year_measurements = {m_col: False for m_col in measurement_cols}
    m_summary_dict.update(indv_year_measurements)
    measurement_summary = pd.DataFrame(
        m_summary_dict, index=[idx for idx in range(len(years_of_interest))]
    )

    # creates empty summary table to hold 'missingness' and consistency data for each status column and year
    s_summary_dict = {"Data (Year)": "blank"}
    indv_year_statuses = {s_col: False for s_col in status_cols}
    s_summary_dict.update(indv_year_statuses)
    status_summary = pd.DataFrame(
        s_summary_dict, index=[idx for idx in range(len(years_of_interest))]
    )

    # creates empty summary table to hold 'missingness' and consistency data for each unit column and year
    u_summary_dict = {"Data (Year)": "blank"}
    indv_year_units = {u_col: False for u_col in unit_cols}
    u_summary_dict.update(indv_year_units)
    unit_summary = pd.DataFrame(
        u_summary_dict, index=[idx for idx in range(len(years_of_interest))]
    )

    return None
