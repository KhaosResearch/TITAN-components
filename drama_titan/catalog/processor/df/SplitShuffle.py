from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process


@dataclass
class STDatasetTrain(SimpleTabularDataset):
    pass


@dataclass
class STDatasetTest(SimpleTabularDataset):
    pass


def execute(pcs: Process, proportion: float = 0.7):
    """
    Horizontal tabular dataset split shuffle.

    Args:
        pcs (Process)

    Parameters:
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
    inputs = pcs.get_from_upstream()

    # read inputs
    input_file: Dict[SimpleTabularDataset] = inputs["TabularDataset"][0]
    input_file_delimiter = input_file["delimiter"]
    input_file_resource = input_file["resource"]

    local_file_file = pcs.storage.get_file(input_file_resource)

    # read tabular dataset
    df = pd.read_csv(local_file_file, delimiter=input_file_delimiter)

    # create train and tests subset
    pcs.info([f"Sampling input dataset"])

    train_subset = df.sample(frac=proportion)
    test_subset = df.drop(train_subset.index)

    out_train_csv = Path(pcs.storage.local_dir, "train.csv")
    train_subset.to_csv(out_train_csv, index=False)

    out_test_csv = Path(pcs.storage.local_dir, "test.csv")
    test_subset.to_csv(out_test_csv, index=False)

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
