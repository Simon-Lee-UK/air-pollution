import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns

sns.set(font_scale=1.4)  # default text size

missingness_cmap_colours = [
    "#2E332F",
    "#93DF9D",
]  # defines two colours to illustrate available vs. missing data
missingness_cmap = LinearSegmentedColormap.from_list(
    "Custom", missingness_cmap_colours, len(missingness_cmap_colours)
)  # uses the available/missing data colours to define a new custom colourmap

unique_count_cmap_colours = [
    "#2E332F",
    "#93DF9D",
    "#896EC4",
]  # defines three colours to illustrate 0, 1, >1 unique values in a particular combination of column and year
unique_count_cmap = LinearSegmentedColormap.from_list(
    "Custom", unique_count_cmap_colours, len(unique_count_cmap_colours)
)  # uses the 0 unique, 1, >1 colours to define a new custom colourmap


def plot_measurement_summary(measurement_summary, year_col_title):
    """
    Function visualises a measurement summary DataFrame, highlighting presence/absence of data columns in each year.

    Parameters
    ----------
    measurement_summary : pandas.DataFrame
        A complete summary DataFrame containing missingness data for measurement columns across a range of years.
        i.e. the measurement summary DataFrame returned by 'monitoring_site_summary' (src.inspect_site).
    year_col_title
        The title of the summary DataFrame's column reporting years of interest.
    """
    plot_df = measurement_summary.copy()
    plot_df.set_index(year_col_title, inplace=True)
    plot_df.sort_index(inplace=True)
    plot_df.columns.name = "Measurement Column"
    plot_df = plot_df.transpose()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.tick_params(axis="both", labelsize=13)
    sns.heatmap(
        plot_df,
        cmap=missingness_cmap,
        square=True,
        linewidths=1,
        # cbar_kws={"shrink": 0.5},
    )
    ax.set_title("Available Measurement Columns per Year", fontsize=18)
    colorbar = ax.collections[0].colorbar
    colorbar.set_ticks([0.25, 0.75])
    colorbar.set_ticklabels(["Data missing", "Data available"])

    return


def plot_status_summary(unique_status_counts, year_col_title):
    """
    Function visualises a status summary DataFrame, highlighting number of unique values in each status column and year.

    Parameters
    ----------
    unique_status_counts : pandas.DataFrame
        A complete summary DataFrame containing missingness/consistency data for status columns across a range of years.
        i.e. the status count DataFrame of same name generated within 'monitoring_site_summary' (src.inspect_site).
    year_col_title
        The title of the summary DataFrame's column reporting years of interest.
    """
    plot_df = unique_status_counts.copy()
    plot_df.set_index(year_col_title, inplace=True)
    plot_df.sort_index(inplace=True)
    plot_df.columns.name = "Status Column"
    plot_df.clip(upper=2, inplace=True)
    plot_df = plot_df.transpose()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.tick_params(axis="both", labelsize=13)
    sns.heatmap(
        plot_df,
        cmap=unique_count_cmap,
        square=True,
        linewidths=1,
        # cbar_kws={"shrink": 0.5},
    )
    ax.set_title("Unique Status Column Values per Year", fontsize=18)
    colorbar = ax.collections[0].colorbar
    colorbar.set_ticks([0.33, 1, 1.66])
    colorbar.set_ticklabels(["Data missing", "Single value", "Multiple values"])

    return


def plot_unit_summary(unique_unit_counts, year_col_title):
    """
    Function visualises a unit summary DataFrame, highlighting number of unique values in each unit column and year.

    Parameters
    ----------
    unique_unit_counts : pandas.DataFrame
        A complete summary DataFrame containing missingness/consistency data for unit columns across a range of years.
        i.e. the unit count DataFrame of same name generated within 'monitoring_site_summary' (src.inspect_site).
    year_col_title
        The title of the summary DataFrame's column reporting years of interest.
    """
    plot_df = unique_unit_counts.copy()
    plot_df.set_index(year_col_title, inplace=True)
    plot_df.sort_index(inplace=True)
    plot_df.columns.name = "Unit Column"
    plot_df.clip(upper=2, inplace=True)
    plot_df = plot_df.transpose()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.tick_params(axis="both", labelsize=13)
    sns.heatmap(
        plot_df,
        cmap=unique_count_cmap,
        square=True,
        linewidths=1,
        # cbar_kws={"shrink": 0.5},
    )
    ax.set_title("Unique Unit Column Values per Year", fontsize=18)
    colorbar = ax.collections[0].colorbar
    colorbar.set_ticks([0.33, 1, 1.66])
    colorbar.set_ticklabels(["Data missing", "Single value", "Multiple values"])

    return
