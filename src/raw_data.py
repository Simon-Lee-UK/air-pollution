import time
import numpy as np
import pandas as pd
from tqdm import tqdm

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
        A list of those years for which data was successfully downloaded
    """
    preview_data = None
    preview_year = None
    years_of_interest = list(reversed(np.arange(start_year, end_year + 1)))
    valid_years = years_of_interest.copy()
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

        if single_year is None:
            valid_years.remove(indv_year)
        elif preview_data is None:
            preview_data = single_year.copy()
            preview_year = indv_year
        else:
            pass

        time.sleep(sleep_time)

    assert preview_data is not None, (
        f"Could not read data from: {fixed_url} "
        f"for any years: {start_year} - {end_year}\n"
        f"Check the URL to ensure location code, years and file extension are all valid!"
    )

    print(f"Preview data sampled from {preview_year}")

    return preview_data, valid_years


def get_single_year(
    site_id,
    year,
    nrows=None,
    header_lines=4,
    fixed_url="https://uk-air.defra.gov.uk/data_files/site_data/",
    sep="_",
    file_format="csv",
):
    """
    Downloads air pollution data from uk-air.defra.gov.uk/ for the input location and year.

    If data is found at the URL defined with the function arguments, raw data is returned as a pandas DataFrame.
    Where data cannot be found, a warning message is displayed to the user quoting the invalid URL and the function
    returns None.

    Parameters
    ----------
    site_id : str
        The consistent identifier associated with the air pollution measurement site of interest e.g. 'OX8'.
    year : int
        The year of air pollution data to be returned for the chosen measurement site e.g. '2015'.
    nrows : None or int
        The number of rows to be returned in the output DataFrame; default value = None (i.e. returns all rows).
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
    single_year : pandas.DataFrame (or None)
        Returns raw air pollution data as a pandas DataFrame if available, otherwise returns None.
    """
    data_url = f"{fixed_url}{site_id}{sep}{year}.{file_format}"
    try:
        single_year = pd.read_csv(data_url, header=header_lines, nrows=nrows)
        return single_year
    except:
        print(
            f"-------\n"
            f"WARNING\n"
            f"-------\n"
            f"Could not read data from: {data_url}\n"
            f"Check the URL to ensure location code, year and file extension are all valid!"
        )
        return None
