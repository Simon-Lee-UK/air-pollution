

def get_data(location, years_of_interest):
    """Docstring
    """
    multi_download_delay = 1.5

    for idx, indv_year in enumerate(years_of_interest):
        single_year = get_single_year_raw(location, year=indv_year)
        single_year = column_consistency(single_year)

        # single_year_cols = single_year.columns.tolist()
        # inspect_columns.loc[idx, "Data (Year)"] = indv_year
        # for col in query_columns:
        #     if col in single_year_cols:
        #         inspect_columns.loc[idx, col] = True

        processed_year = single_year.pipe(column_conversion).pipe(datetime_conversion)
        if idx == 0:
            air_pollution = processed_year.copy()
        else:
            air_pollution = air_pollution.append(processed_year, ignore_index=True)
        time.sleep(multi_download_delay)  # creates interval between requests to uk-air.defra.gov.uk

    return air_pollution
