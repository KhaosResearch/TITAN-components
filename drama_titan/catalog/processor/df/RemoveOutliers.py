from math import isnan
from pathlib import Path
from typing import Dict

import dask.dataframe as dd
import numpy as np
import pandas as pd
import scipy as sp
from drama.core.model import SimpleTabularDataset
from drama.process import Process
from scipy.spatial.distance import mahalanobis


def execute(
    pcs: Process, label: str, ignore_columns: list = (), outlier_threshold: float = 0.7, n_partitions: int = 4,
):
    """
    Remove outliers from a tabular dataset based on the Mahalanobis distance.

    Args:
        pcs (Process)

    Parameters:
        label (str): Classification column name.
        ignore_columns (list): Columns to ignore while calculating the Mahalanobis distance. Defaults to None.
        outlier_threshold (float): Percentile of the original dataset to keep. Defaults to 0.7.
        n_partitions (int): Number of partitions the dataset is going to be split on.
            Partitions are executed in parallel. More partitions worse results,
            but faster compute time.  Defaults to 4.

    Inputs:
        TabularDataset: Input dataset

    Outputs:
        SimpleTabularDataset: Dataset

    Produces:
        no_outliers.csv (Path)

    Author:
        Khaos Research
    """
    if outlier_threshold >= 1 or outlier_threshold <= 0:
        raise ValueError("The outlier threshold must be between 0 an 1.")
    outlier_threshold = 100 * outlier_threshold

    def _mahalanobis(df: pd.DataFrame, label: str):
        """
        Compute Mahalanobis distance for a given dataset.
        """
        features = list(df.columns.values)
        if label in features:
            features.remove(label)
        else:
            raise ValueError(f"Label '{label}' is not part of the headers: {features}")

        for label in ignore_columns:
            if label in features:
                features.remove(label)
            else:
                pcs.warn([f"Column '{label}' is not part of the dataset"])

        classes = list(df[label].unique())
        df = df.fillna(df.mean())

        df_no_classes = df[features]

        IC = np.zeros((len(classes), df_no_classes.shape[1], df_no_classes.shape[1]))
        mean = np.zeros((len(classes), df_no_classes.shape[1]))

        for i, class_name in enumerate(classes):
            IC[i, :, :] = df_no_classes[df[label] == class_name].cov().values
            IC[i, :, :][np.isnan(IC[i, :, :])] = 1
            IC[i, :, :] = sp.linalg.pinv(IC[i, :, :])
            mean[i, :] = df_no_classes[df[label] == class_name].mean().values

        m = np.zeros(df.shape[0])
        m_class = [[] for _ in range(len(classes))]

        for i in range(df.shape[0]):
            row_class = classes.index(df.iloc[i, df.columns.get_loc(label)])
            m[i] = mahalanobis(df_no_classes.values[i, :], mean[row_class, :], IC[row_class, :, :]) ** 2
            m_class[row_class].append(m[i])

        cutoffs = [(np.nanpercentile(m_class[classes.index(class_)], outlier_threshold)) for class_ in classes]
        cutoffs = [0 if isnan(x) else x for x in cutoffs]
        outliers = [m[row] >= cutoffs[classes.index(df.iloc[row][label])] for row in range(len(m))]

        return outliers

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file: Dict[SimpleTabularDataset] = inputs["TabularDataset"][0]
    input_file_delimiter = input_file["delimiter"]
    input_file_resource = input_file["resource"]

    local_file_file = pcs.storage.get_file(input_file_resource)

    # read tabular dataset
    df = pd.read_csv(local_file_file, delimiter=input_file_delimiter)

    df = dd.from_pandas(df, npartitions=n_partitions)
    result = df.map_partitions(_mahalanobis, label=label, meta=(None, "int32")).compute()

    outliers = []
    for value in result:
        outliers += value

    df = df.compute()
    df_clean = df.drop(df[outliers].index)

    out_csv = Path(pcs.storage.local_dir, "no_outliers.csv")
    df_clean.to_csv(out_csv, index=False)

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_csv)

    # send to downstream
    output_simple_tabular_dataset = SimpleTabularDataset(resource=dfs_dir, delimiter=",", file_format=".csv")
    pcs.to_downstream(output_simple_tabular_dataset)

    return {"output": output_simple_tabular_dataset, "resource": dfs_dir}
