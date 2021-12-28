from pathlib import Path
import datetime
from functools import partial
import pandas as pd

from drama.process import Process
from drama.models.task import TaskResult
from drama.core.model import SimpleTabularDataset


def execute(pcs: Process):
    """
    Extraction of statistical data for each station for each hidrologic year
    Args:
        pcs (Process)
    Parameters:

    Inputs:
         TTabularDataSet (Simple Dataset): Time series data representing precipitation or temperature data
    Outputs:
        TabularDataSet (Simple Dataset): Statistical analysis dataset

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    for (key, msg) in pcs.poll_from_upstream():
        if key == "SimpleTabularDataset":
            input_file = msg
        elif key == "SimpleTabularDatasetMax":
            input_file = msg
        elif key == "SimpleTabularDatasetMin":
            input_file = msg

    input_file_resource = input_file["resource"]
    input_file_delimiter = input_file["delimiter"]

    local_file_path = pcs.storage.get_file(input_file_resource)

    # create dataframe
    df = pd.read_csv(local_file_path, sep=input_file_delimiter)

    # format to datetime
    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y-%m-%d")
    stations = df.columns.drop("DATE")

    # statistical data of interest
    interest = [
        "Hidrologic Year",
        "Station",
        "Year Mean",
        "Year Maximum",
        "Year minimum",
        "Year Collected Data",
        "Year Empty Data",
        "Year Collected Data (Percentage)",
        "Year Empty Data (Percentage)",
    ]

    # create output dataframe
    output_df = pd.DataFrame(columns=interest)

    i = 1
    for station in stations:
        min_year = df["DATE"].min().year
        max_year = df["DATE"].max().year

        # Filtering for hidrologic year
        for year in range(min_year, max_year):
            start = f"{year}-10-1"
            end = f"{year + 1}-9-30"
            filtered_df = df.loc[(df["DATE"] >= start) & (df["DATE"] <= end)]

            # Calculating the total value of the year
            year_total = filtered_df[station].sum()
            # Calculating the year mean
            year_mean = filtered_df[station].mean()
            # Calculating the year maximum
            year_max = filtered_df[station].max()
            # Calculating the year minimum
            year_min = filtered_df[station].min()
            # Counting the number of rows
            n_rows = len(filtered_df.index)
            # Counting the number of empty rows
            empty_rows = filtered_df[station].isnull().sum()
            # Calculating the percentage of empty rows
            empty_per = (empty_rows / n_rows) * 100

            # Updating dataframe for each hidrologic year
            output_df.loc[i, "Station"] = station
            output_df.loc[i, "Hidrologic Year"] = f"{year}/{year + 1}"
            output_df.loc[i, "Sum of the Year"] = year_total
            output_df.loc[i, "Year Mean"] = year_mean
            output_df.loc[i, "Year Maximum"] = year_max
            output_df.loc[i, "Year minimum"] = year_min
            output_df.loc[i, "Year Collected Data"] = n_rows - empty_rows
            output_df.loc[i, "Year Empty Data"] = empty_rows
            output_df.loc[i, "Year Empty Data (Percentage)"] = empty_per
            output_df.loc[i, "Year Collected Data (Percentage)"] = 100 - empty_per
            i += 1

    # prepare output for the time series output
    out_csv = Path(pcs.storage.local_dir, "StatisticalData.csv")
    output_df.to_csv(out_csv, index=False, sep=input_file_delimiter)

    # send time to remote storage
    dfs_dir_output = pcs.storage.put_file(out_csv)

    # send to downstream
    out_csv = SimpleTabularDataset(resource=dfs_dir_output, delimiter=";", file_format=".csv")
    pcs.to_downstream(out_csv)

    return TaskResult(files=[dfs_dir_output])
