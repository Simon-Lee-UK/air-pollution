import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns

sns.set(font_scale=1.4)  # default text size
missingness_cmap_colours = ["#2E332F", "#93DF9D"]  # defines two colours to illustrate available vs. missing data
missingness_cmap = LinearSegmentedColormap.from_list(
    "Custom", missingness_cmap_colours, len(missingness_cmap_colours)
)  # uses the available/missing data colours to define a new custom colourmap


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
        plot_df, cmap=missingness_cmap, square=True, linewidths=1, cbar_kws={"shrink": 0.5}
    )
    ax.set_title("Available Measurement Columns per Year", fontsize=18)
    colorbar = ax.collections[0].colorbar
    colorbar.set_ticks([0.25, 0.75])
    colorbar.set_ticklabels(["Data missing", "Data available"])

    return
