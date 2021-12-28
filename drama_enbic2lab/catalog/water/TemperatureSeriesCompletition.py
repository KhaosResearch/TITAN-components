from pathlib import Path

import pandas as pd
import datetime
import pyhomogeneity


from numpy import NaN
from pyhomogeneity.pyhomogeneity import pettitt_test, buishand_range_test, snht_test
from sklearn.linear_model import LinearRegression

from drama.process import Process
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
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
    Completition of min and max temperature time series using a linear regression
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
         TabularDataSet (Simple Dataset): Max Temperature time series to complete
         TabularDataSet (Simple Dataset): Min Temperature time series to complete
    Outputs:
        TabularDataSet (SimpleTabularDatasetSeries): Precipitation Time series completed
        TabularDataSet (Simple Dataset): Linear regression fitting between stations
        TabularDataSet (SimpleTabularDatasetTest): Homogeneity Test for the completition

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file_one = inputs["SimpleTabularDatasetMax"][0]
    input_file_resource_one = input_file_one["resource"]
    input_file_delimiter_one = input_file_one["delimiter"]

    input_file_two = inputs["SimpleTabularDatasetMin"][0]
    input_file_resource_two = input_file_two["resource"]
    input_file_delimiter_two = input_file_two["delimiter"]

    local_file_path_one = pcs.storage.get_file(input_file_resource_one)
    local_file_path_two = pcs.storage.get_file(input_file_resource_two)

    # checking errors
    if priorize != "r2" and priorize != "slope" and priorize != "pair":
        raise ValueError("Enter a valid criterion to priorize the completition")

    if "pettit" and "shnt" and "buishand" not in tests:
        raise ValueError("Enter a valid homogeneity test")

    # read datasets
    df_max = pd.read_csv(local_file_path_one, sep=input_file_delimiter_one)
    df_min = pd.read_csv(local_file_path_two, sep=input_file_delimiter_two)

    # format to datetime
    df_max["DATE"] = pd.to_datetime(df_max["DATE"], format="%Y-%m-%d")
    df_min["DATE"] = pd.to_datetime(df_min["DATE"], format="%Y-%m-%d")

    # create list of dataframes to iterate
    df_list = [df_max, df_min]

    # create date range from the starting to the ending date
    dates_pd = pd.date_range(start=start_date, end=end_date, freq="D")

    # output dataframe
    out_df = pd.DataFrame(columns=["DATE", target_station + "(MAX)", target_station + "(MIN)"])
    out_df["DATE"] = dates_pd
    out_df = out_df.set_index("DATE")

    # set the starting temperature to the maximum temperature
    temp = "(MAX)"

    # iterate through both dataframes
    for df in df_list:

        # filter the data by the desired dates
        filtered_df = df.loc[(df["DATE"] >= start_date) & (df["DATE"] <= end_date)]

        # dataframe to store the regression performance between the stations
        analysis_df = pd.DataFrame(
            index=["R2", "Slope", "Intercept", "Pair of data"],
            columns=analysis_stations,
        )

        # dataframe with the all the completitions
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

            # if the analysis station is have no data, remove it from the analysis
            if len(X) == 0:
                analysis_df = analysis_df.drop(station, axis=1)
                pass

            # performing the linear regression and storing the completition and the performance
            else:
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

        # updating the output dataframe
        out_df[target_station + temp] = target_completition[target_station]

        # change the temp to min
        temp = "(MIN)"

    # check the consistency of the completition so that the max temp is not lower or equal to the min temp
    # and change it so that they have at least 2 degrees of difference
    aux_df = out_df.loc[out_df[target_station + "(MAX)"] <= out_df[target_station + "(MIN)"]]
    out_df = out_df.round()

    if len(aux_df) != 0:
        for i, row in aux_df.iterrows():
            avg = round(row.mean())
            row[target_station + "(MAX)"] = avg + 1
            row[target_station + "(MIN)"] = avg - 1
        out_df.loc[i] = aux_df.loc[i]

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
    t = "(MAX)"
    for column in out_df.columns:
        to_test = out_df.loc[:, column]

        if "pettit" in tests:
            [homogeneity, change_point, p_value, U, mu] = pyhomogeneity.pettitt_test(to_test, alpha=0.5, sim=10000)
            tests_df.loc["Homogeneity", "Pettit Test" + t] = homogeneity
            tests_df.loc["Change Point Location", "Pettit Test" + t] = change_point
            tests_df.loc["P-value", "Pettit Test" + t] = p_value
            tests_df.loc["Maximum test Statistics", "Pettit Test" + t] = U
            tests_df.loc["Average between change point", "Pettit Test" + t] = mu

        if "shnt" in tests:
            [homogeneity, change_point, p_value, T, mu] = pyhomogeneity.snht_test(to_test, alpha=0.5, sim=10000)
            tests_df.loc["Homogeneity", "SNHT Test" + t] = homogeneity
            tests_df.loc["Change Point Location", "SNHT Test" + t] = change_point
            tests_df.loc["P-value", "SNHT Test" + t] = p_value
            tests_df.loc["Maximum test Statistics", "SNHT Test" + t] = T
            tests_df.loc["Average between change point", "SNHT Test" + t] = mu
        if "buishand" in tests:
            [
                homogeneity,
                change_point,
                p_value,
                R,
                mu,
            ] = pyhomogeneity.buishand_range_test(to_test, alpha=0.5, sim=10000)
            tests_df.loc["Homogeneity", "Buishand Test" + t] = homogeneity
            tests_df.loc["Change Point Location", "Buishand Test" + t] = change_point
            tests_df.loc["P-value", "Buishand Test" + t] = p_value
            tests_df.loc["Maximum test Statistics", "Buishand Test" + t] = R
            tests_df.loc["Average between change point", "Buishand Test" + t] = mu

        t = "(MIN)"

    # prepare output for the analysis between stations
    out_csv = Path(pcs.storage.local_dir, "StationsAnalysis.csv")
    analysis_df.to_csv(out_csv, sep=input_file_delimiter_one)

    # send time to remote storage
    dfs_dir_analysis = pcs.storage.put_file(out_csv)

    # send to downstream
    analysis_csv = SimpleTabularDataset(
        resource=dfs_dir_analysis, delimiter=input_file_delimiter_one, file_format=".csv"
    )
    pcs.to_downstream(analysis_csv)

    # prepare output for the series completed
    out_csv = Path(pcs.storage.local_dir, f"{target_station}_completed.csv")
    out_df.to_csv(out_csv, sep=input_file_delimiter_one)

    # send time to remote storage
    dfs_dir_series = pcs.storage.put_file(out_csv)

    # send to downstream
    series_csv = SimpleTabularDatasetSeries(
        resource=dfs_dir_series, delimiter=input_file_delimiter_one, file_format=".csv"
    )
    pcs.to_downstream(series_csv)

    # prepare output for the homegeneity test
    out_csv = Path(pcs.storage.local_dir, "HomogeneityTests.csv")
    tests_df.to_csv(out_csv, sep=input_file_delimiter_one)

    # send time to remote storage
    dfs_dir_test = pcs.storage.put_file(out_csv)

    # send to downstream
    test_csv = SimpleTabularDatasetTest(resource=dfs_dir_test, delimiter=input_file_delimiter_one, file_format=".csv")
    pcs.to_downstream(test_csv)

    return TaskResult(files=[dfs_dir_analysis, dfs_dir_series, dfs_dir_test])
