import pandas as pd


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
    Downloads air pollution data from uk-air.defra.gov.uk for the input location and year.

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
