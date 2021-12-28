from pathlib import Path

import pandas as pd
import datetime
import pyhomogeneity


from numpy import NaN
from pyhomogeneity.pyhomogeneity import pettitt_test, buishand_range_test, snht_test
from sklearn.linear_model import LinearRegression

from drama.process import Process
from drama.models.task import TaskResult
from drama.core.model import SimpleTabularDataset
from dataclasses import dataclass


@dataclass
class SimpleTabularDatasetSeries(SimpleTabularDataset):
    pass


@dataclass
class SimpleTabularDatasetTest(SimpleTabularDataset):
    pass


def execute(
    pcs: Process,
    start_date: str,
    end_date: str,
    target_station: str,
    analysis_stations: list,
    priorize: str = "r2",
    tests: list = ["pettit", "shnt", "buishand"],
):

    """
    Completition of precipitation time series using a linear regression
    Args:
        pcs (Process)
    Parameters:
        start_date (str): Time series starting date.
        end_date (str): Time series ending date.
        target_station (str): Station that is desired to be completed.
        analysis_stations (list): Stations that will be used to complete the target station.
        priorize (str): Value to priorize for the completion of the series
                    Values are 'r2','slope','pair'
        tests (list): Homogeneity tests to perform
                    Values that can be included in the list are 'pettit','snht','buishand'.

    Inputs:
         TabularDataSet (Simple Dataset): Precipitation Time series to complete
    Outputs:
        TabularDataSet (Simple Dataset): Precipitation Time series completed
        TabularDataSet (SimpleTabularDatasetSeries): Linear regression fitting between stations
        TabularDataSet (SimpleTabularDatasetTest): Homogeneity Test for the completition

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file = inputs["SimpleTabularDataset"][0]
    input_file_resource = input_file["resource"]
    input_file_delimiter = input_file["delimiter"]

    local_file_path = pcs.storage.get_file(input_file_resource)

    # checking errors
    if priorize != "r2" and priorize != "slope" and priorize != "pair":
        raise ValueError("Enter a valid criterion to priorize the completition")

    if "pettit" and "shnt" and "buishand" not in tests:
        raise ValueError("Enter a valid homogeneity test")

    # create dataframe
    df = pd.read_csv(local_file_path, sep=input_file_delimiter)

    # format to datetime
    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y-%m-%d")

    filtered_df = df.loc[(df["DATE"] >= start_date) & (df["DATE"] <= end_date)]

    analysis_df = pd.DataFrame(index=["R2", "Slope", "Intercept", "Pair of data"], columns=analysis_stations)

    series_completition = pd.DataFrame(columns=["DATE"])
    series_completition["DATE"] = filtered_df["DATE"]
    series_completition = series_completition.set_index("DATE")

    # Linear regression between the stations
    for station in analysis_stations:
        join_df = filtered_df[[target_station, station]]
        shared_data = len(join_df.dropna())
        join_df = join_df.dropna()
        X = join_df[station].values.reshape(-1, 1)
        y = join_df[target_station].values.reshape(-1, 1)
        regr = LinearRegression()
        regr.fit(X, y)

        # We store the different coefficient of regression between the target station
        # and the stations that will be used to complete the series
        analysis_df.loc["R2", station] = regr.score(X, y)
        analysis_df.loc["Slope", station] = regr.coef_[0][0]
        analysis_df.loc["Intercept", station] = regr.intercept_[0]
        analysis_df.loc["Pair of data", station] = shared_data

        # Then, we take the values of each station
        completition = filtered_df[["DATE", station]].dropna()
        completition = completition.set_index("DATE")
        station_fit = completition[station].values.reshape(-1, 1)
        station_pred = regr.predict(station_fit)
        completition[station] = station_pred
        series_completition = series_completition.join(completition)

    # sort stations according to the priorization criterion
    if priorize == "r2":
        analysis_df = analysis_df.sort_values("R2", axis=1, ascending=False)
    elif priorize == "slope":
        analysis_df = analysis_df.sort_values("Slope", axis=1, ascending=False)
    elif priorize == "pairs":
        analysis_df = analysis_df.sort_values("Pair of Data", axis=1, ascending=False)

    best_stations = list(analysis_df.columns)

    # dataframe to store the best completition
    target_completition = filtered_df[["DATE", target_station]]
    target_completition = target_completition.set_index("DATE")

    # completing the target station until now more empty values remain
    rows_to_complete = target_completition[target_station].isna().sum().tolist()

    while rows_to_complete != 0:
        for station in best_stations:
            target_completition.loc[
                target_completition[target_station].isna(), target_station
            ] = series_completition.loc[target_completition[target_station].isna(), station].values
            rows_to_complete = target_completition[target_station].isna().sum().tolist()

    intercepts = analysis_df.loc["Intercept"].values

    # correcting errors of the completition
    for num in intercepts:
        target_completition = target_completition.replace(num, 0)

    target_completition = target_completition.round(3)

    # values for the different homogeneity tests
    tests_out = [
        "Homogeneity",
        "Change Point Location",
        "P-value",
        "Maximum test Statistics",
        "Average between change point",
    ]

    # dataframe for the homogeneity tests
    tests_df = pd.DataFrame(index=tests_out)

    # computing the homogeneity tests
    if "pettit" in tests:
        [homogeneity, change_point, p_value, U, mu] = pyhomogeneity.pettitt_test(
            target_completition, alpha=0.5, sim=10000
        )
        tests_df.loc["Homogeneity", "Pettit Test"] = homogeneity
        tests_df.loc["Change Point Location", "Pettit Test"] = change_point
        tests_df.loc["P-value", "Pettit Test"] = p_value
        tests_df.loc["Maximum test Statistics", "Pettit Test"] = U
        tests_df.loc["Average between change point", "Pettit Test"] = mu

    if "shnt" in tests:
        [homogeneity, change_point, p_value, T, mu] = pyhomogeneity.snht_test(target_completition, alpha=0.5, sim=10000)
        tests_df.loc["Homogeneity", "SNHT Test"] = homogeneity
        tests_df.loc["Change Point Location", "SNHT Test"] = change_point
        tests_df.loc["P-value", "SNHT Test"] = p_value
        tests_df.loc["Maximum test Statistics", "SNHT Test"] = T
        tests_df.loc["Average between change point", "SNHT Test"] = mu
    if "buishand" in tests:
        [homogeneity, change_point, p_value, R, mu] = pyhomogeneity.buishand_range_test(
            target_completition, alpha=0.5, sim=10000
        )
        tests_df.loc["Homogeneity", "Buishand Test"] = homogeneity
        tests_df.loc["Change Point Location", "Buishand Test"] = change_point
        tests_df.loc["P-value", "Buishand Test"] = p_value
        tests_df.loc["Maximum test Statistics", "Buishand Test"] = R
        tests_df.loc["Average between change point", "Buishand Test"] = mu

    # prepare output for the analsys between stations
    out_csv = Path(pcs.storage.local_dir, "StationsAnalysis.csv")
    analysis_df.to_csv(out_csv, sep=input_file_delimiter)

    # send time to remote storage
    dfs_dir_analysis = pcs.storage.put_file(out_csv)

    # send to downstream
    analysis_csv = SimpleTabularDataset(resource=dfs_dir_analysis, delimiter=input_file_delimiter, file_format=".csv")
    pcs.to_downstream(analysis_csv)

    # prepare output for the series completed
    out_csv = Path(pcs.storage.local_dir, f"{target_station}_completed.csv")
    target_completition.to_csv(out_csv, sep=input_file_delimiter)

    # send time to remote storage
    dfs_dir_series = pcs.storage.put_file(out_csv)

    # send to downstream
    series_csv = SimpleTabularDatasetSeries(resource=dfs_dir_series, delimiter=input_file_delimiter, file_format=".csv")
    pcs.to_downstream(series_csv)

    # prepare output for the homegeneity test
    out_csv = Path(pcs.storage.local_dir, "HomogeneityTests.csv")
    tests_df.to_csv(out_csv, sep=input_file_delimiter)

    # send time to remote storage
    dfs_dir_test = pcs.storage.put_file(out_csv)

    # send to downstream
    test_csv = SimpleTabularDatasetTest(resource=dfs_dir_test, delimiter=input_file_delimiter, file_format=".csv")
    pcs.to_downstream(test_csv)

    return TaskResult(files=[dfs_dir_analysis, dfs_dir_series, dfs_dir_test])
