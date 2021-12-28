from pathlib import Path

import pandas as pd
import datetime
from numpy import NaN

from drama.process import Process
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
from dataclasses import dataclass


@dataclass
class SimpleTabularDatasetMin(SimpleTabularDataset):
    pass


@dataclass
class SimpleTabularDatasetMax(SimpleTabularDataset):
    pass


def execute(pcs: Process):
    """
    Convert temperature data in matrix form to time series data
    Args:
        pcs (Process)
    Parameters:

    Inputs:
         InputFile (TempFile): Excel database with the data in matrix form
    Outputs:
        TabularDataSet (Simple Dataset): Time series representing the minimum temperatures
        TabularDataSet (Simple Dataset): Time series representing the maximum temperatures

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file = inputs["TempFile"][0]
    input_file_resource = input_file["resource"]

    local_file_path = pcs.storage.get_file(input_file_resource)

    # create dataframe
    matrix_df = pd.read_excel(local_file_path, engine="openpyxl")
    # change column name if it is wrong
    if "AﾑO" in matrix_df.columns:
        matrix_df.rename(columns={"AﾑO": "AÑO"}, inplace=True)

    # divide by 10 to obtain temperatures in the right scale
    matrix_df.loc[:, "TMAX1":"TMIN31"] = matrix_df.loc[:, "TMAX1":"TMIN31"].div(10)
    # obtain minimum and maximum year of recorded data
    min_year = matrix_df["AÑO"].min()
    max_year = matrix_df["AÑO"].max()

    # create auxiliary dataframe containing info of extreme years
    min_df = matrix_df.where(matrix_df["AÑO"] == min_year)
    max_df = matrix_df.where(matrix_df["AÑO"] == max_year)

    # set conditions so that the time series starting and ending point point
    # coincides with the hidrologic year in Andalusia (from 1 October to 30 September)
    if int(min_df["MES"].min()) <= 10 and int(max_df["MES"].max() >= 9):
        start_date = f"{int(min_year) - 1}-10-1"
        end_date = f"{int(max_year) + 1}-9-30"
        dates_pd = pd.date_range(start=start_date, end=end_date, freq="D")
    elif int(min_df["MES"].min()) <= 10:
        start_date = f"{int(min_year) - 1}-10-1"
        end_date = f"{int(max_year)}-9-30"
        dates_pd = pd.date_range(start=start_date, end=end_date, freq="D")
    elif int(max_df["MES"].max()) >= 9:
        start_date = start_date = f"{int(min_year)}-10-1"
        end_date = f"{int(max_year) + 1}-9-30"
        dates_pd = pd.date_range(start=start_date, end=end_date, freq="D")

    # create dataframe where the time series data is to be stored
    final_max_pd = pd.DataFrame(columns=["DATE"])
    final_max_pd["DATE"] = dates_pd
    final_max_pd = final_max_pd.set_index("DATE")

    final_min_pd = pd.DataFrame(columns=["DATE"])
    final_min_pd["DATE"] = dates_pd
    final_min_pd = final_min_pd.set_index("DATE")

    # get the stations where the data is obtained
    stations = matrix_df["NOMBRE"].unique()

    # create a dataframe to each station that will be used to get the final output
    for station in stations:
        partial_tmax = pd.DataFrame(columns=[station])
        partial_tmin = pd.DataFrame(columns=[station])

        # Get the values for each station, so that the columns labelled as 'P1',....'P31' corresponds
        # to the day of the month and record these values and assign each value in the time series output
        data_station = matrix_df[matrix_df["NOMBRE"] == station]
        for index, row in data_station.iterrows():
            values_tmax = row["TMAX1":"TMAX31"].to_dict()
            values_tmin = row["TMIN1":"TMIN31"].to_dict()
            for key, val in values_tmax.items():
                try:
                    datetime_index = datetime.datetime(int(row["AÑO"]), int(row["MES"]), int(key.replace("TMAX", "")))
                    partial_tmax.loc[datetime_index] = val
                except ValueError:
                    None
            for key, val in values_tmin.items():
                try:
                    datetime_index = datetime.datetime(int(row["AÑO"]), int(row["MES"]), int(key.replace("TMIN", "")))
                    partial_tmin.loc[datetime_index] = val
                except ValueError:
                    None
        # join the dataframe created for each station to the final one
        final_max_pd = final_max_pd.join(partial_tmax)
        final_min_pd = final_min_pd.join(partial_tmin)

    # drop columns with NaN values
    if NaN in final_max_pd.columns:
        final_max_pd = final_max_pd.drop(NaN, axis="columns")

    if NaN in final_min_pd.columns:
        final_min_pd = final_min_pd.drop(NaN, axis="columns")

    # prepare output for the time series output

    out_csv = Path(pcs.storage.local_dir, "MinTempTimeSeries.csv")

    final_min_pd.to_csv(out_csv, sep=";")

    # send time to remote storage

    dfs_dir_min_output = pcs.storage.put_file(out_csv)

    # send to downstream

    min_temp_output = SimpleTabularDatasetMin(resource=dfs_dir_min_output, delimiter=";", file_format=".csv")

    pcs.to_downstream(min_temp_output)

    # prepare output for the time series output

    out_csv = Path(pcs.storage.local_dir, "MaxTempTimeSeries.csv")

    final_max_pd.to_csv(out_csv, sep=";")

    # send time to remote storage

    dfs_dir_max_output = pcs.storage.put_file(out_csv)

    # send to downstream

    max_temp_output = SimpleTabularDatasetMax(resource=dfs_dir_max_output, delimiter=";", file_format=".csv")

    pcs.to_downstream(max_temp_output)

    return TaskResult(files=[dfs_dir_min_output, dfs_dir_max_output])
