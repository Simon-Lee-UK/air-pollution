import time
import pandas as pd
from src.raw_data import get_single_year

sleep_duration = 0.75


def get_reference_columns(
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
    Returns descriptive column titles for a reference year of air pollution data at the defined measurement site.

    All possible years of interest should be provided to the function in case any years of data are unavailable. The
    reference columns are extracted from the most recent available year of data. The function returns None if it cannot
    download any of the specified data sets. Column titles containing metadata are renamed from their raw defaults so
    it is clear which measurement column their data relates to.

    Parameters
    ----------
    site_id : str
        The consistent identifier associated with the air pollution measurement site of interest e.g. 'OX8'.
    years_of_interest : list of int
        The years of data that are of interest, reference column titles are extracted from the first valid year of data.
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
    reference_cols : list of str
        A list of descriptive column titles extracted from the most recently available year of data.

    Raises
    ------
    ValueError
        Raised when the URLs specified by function arguments lead to zero successful data downloads.

    """
    nrows = 1  # defines the number of rows to be read-in for each data set, to extract column titles: only require one
    years_of_interest.sort(reverse=True)
    for indv_year in years_of_interest:
        reference_year = get_single_year(
            site_id,
            indv_year,
            nrows=nrows,
            header_lines=header_lines,
            fixed_url=fixed_url,
            sep=sep,
            file_format=file_format,
        )
        if reference_year is not None:
            reference_year = rename_status_and_unit_columns(
                reference_year,
                status_str=status_str,
                unit_str=unit_str,
                status_offset=status_offset,
                unit_offset=unit_offset,
            )  # renames metadata columns
            reference_cols = reference_year.columns.tolist()
            return reference_cols
        else:
            time.sleep(
                sleep_duration
            )  # creates interval between requests to uk-air.defra.gov.uk

    raise ValueError(
        f"Could not read data from: {fixed_url} "
        f"for any years: {years_of_interest[-1]} - {years_of_interest[0]}\n"
        f"Check the URL to ensure location code, years and file extension are all valid!"
    )


def split_column_types(reference_cols, status_str="status", unit_str="unit"):
    """
    Splits input list of column titles into separate lists for measurement columns, status columns and unit columns.

    Parameters
    ----------
    reference_cols : List of str
        A list of all column titles for an air pollution DataFrame.
    status_str : str
        The consistent string appearing in all metadata column titles that contain status information for a
        corresponding measurement column; default value = 'status'.
    unit_str : str
        The consistent string appearing in all metadata column titles that contain the unit associated with a
        corresponding measurement column; default value = 'unit'.

    Returns
    -------
    measurement_cols : list of str
        A list of column titles for those columns containing measurement values.
    status_cols : list of str
        A list of column titles for those columns containing status information related to a corresponding measurement
        column's values.
    unit_cols : list of str
        A list of column titles for those columns containing the measurement unit for a corresponding measurement
        column's values.
    """
    status_cols = [col for col in reference_cols if status_str in col]
    unit_cols = [col for col in reference_cols if unit_str in col]
    measurement_cols = [
        col for col in reference_cols if col not in status_cols and col not in unit_cols
    ]
    return measurement_cols, status_cols, unit_cols


def rename_status_and_unit_columns(
    input_df, status_str="status", unit_str="unit", status_offset=-1, unit_offset=-2
):
    """
    Updates the names of all metadata columns in a raw data set, linking them to their associated measurement column.

    Parameters
    ----------
    input_df : pandas.DataFrame
        Raw air pollution data with non-descriptive metadata column titles from uk-air.defra.gov.uk
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
    output_df : pandas.DataFrame
        Raw air pollution data with metadata column titles renamed to include the measurement type they correspond to.
    """
    output_df = input_df.copy()
    input_col_list = input_df.columns.tolist()
    output_col_list = [
        generate_metadata_column_title(
            input_col_list,
            idx,
            col,
            status_str=status_str,
            unit_str=unit_str,
            status_offset=status_offset,
            unit_offset=unit_offset,
        )
        for idx, col in enumerate(input_col_list)
    ]
    output_df.columns = output_col_list

    return output_df


def generate_metadata_column_title(
    col_list,
    input_col_idx,
    input_col_title,
    status_str="status",
    unit_str="unit",
    status_offset=-1,
    unit_offset=-2,
):
    """
    Updates the name of a status or unit column, linking it to its associated measurement column.

    Parameters
    ----------
    col_list : list of str
        The full list of column titles for the DataFrame for which a single metadata column is being updated.
    input_col_idx : int
        The positional index of the input column to apply the title update to.
    input_col_title : str
        The original title of the input column to be updated.
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
    output_col_title : str
        The new title of the input column; only updated if input column is a metadata column (status or unit).
    """
    if status_str in input_col_title:
        output_col_title = col_list[input_col_idx + status_offset] + " " + status_str
    elif unit_str in input_col_title:
        output_col_title = col_list[input_col_idx + unit_offset] + " " + unit_str
    else:
        output_col_title = input_col_title

    return output_col_title
