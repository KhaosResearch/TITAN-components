from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process
from sklearn.model_selection import StratifiedShuffleSplit


@dataclass
class STDatasetTrain(SimpleTabularDataset):
    pass


@dataclass
class STDatasetTest(SimpleTabularDataset):
    pass


def execute(pcs: Process, label: str, proportion: float = 0.7):
    """
    Horizontal tabular dataset split shuffle.

    Args:
        pcs (Process)

    Parameters:
        label (str): Classification column name
        proportion (float): Proportion of axis to returns . Defaults to 0.7.

    Inputs:
        TabularDataset: Input dataset

    Outputs:
        STDatasetTrain (SimpleTabularDataset): Train dataset
        STDatasetTest (SimpleTabularDataset): Test dataset

    Produces:
        train.csv (Path)
        test.csv (Path)

    Author:
        Khaos Research
    """
    # read inputs
    inputs = pcs.get_from_upstream()

    input_file: Dict[SimpleTabularDataset] = inputs["TabularDataset"][0]
    input_file_delimiter = input_file["delimiter"]
    input_file_resource = input_file["resource"]

    local_file_file = pcs.storage.get_file(input_file_resource)

    # read tabular dataset
    df = pd.read_csv(local_file_file, delimiter=input_file_delimiter)

    # create train and test subset
    features = list(df.columns.values)
    features.remove(label)

    x = df[features].values
    y = df[label].values

    sss = StratifiedShuffleSplit(n_splits=1, test_size=proportion)

    for train_index, test_index in sss.split(x, y):
        x_train, x_test = x[train_index], x[test_index]
        y_train, y_test = y[train_index], y[test_index]

    train = np.hstack((x_train, np.atleast_2d(y_train).T))
    test = np.hstack((x_test, np.atleast_2d(y_test).T))

    train_subset = pd.DataFrame.from_records(train, columns=list(df.columns.values))
    tests_subset = pd.DataFrame.from_records(test, columns=list(df.columns.values))

    out_train_csv = Path(pcs.storage.local_dir, "train.csv")
    train_subset.to_csv(out_train_csv, index=False)

    out_test_csv = Path(pcs.storage.local_dir, "test.csv")
    tests_subset.to_csv(out_test_csv, index=False)

    # send to remote storage
    dfs_dir_train = pcs.storage.put_file(out_train_csv)
    dfs_dir_test = pcs.storage.put_file(out_test_csv)

    # send to downstream
    output_train_simple_tabular_dataset = STDatasetTrain(resource=dfs_dir_train, delimiter=",", file_format=".csv")
    pcs.to_downstream(output_train_simple_tabular_dataset)

    output_test_simple_tabular_dataset = STDatasetTest(resource=dfs_dir_test, delimiter=",", file_format=".csv")
    pcs.to_downstream(output_test_simple_tabular_dataset)

    return {
        "output": [output_train_simple_tabular_dataset, output_test_simple_tabular_dataset,],
        "resource": [dfs_dir_train, dfs_dir_test],
    }
