import pickle
from pathlib import Path
from typing import Dict

import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process

from drama_titan.model import MlModel


def execute(pcs: Process, *kwargs):
    """
    Generic classifier predict stage.

    Args:
        pcs (Process)

    Parameters:
        None

    Inputs:
        TabularDataset (SimpleTabularDataset): Dataset
        MlModel (MlModel): Machine learning classifier model

    Outputs:
        SimpleTabularDataset: Predict dataset

    Produces:
        predict.csv (Path)

    Author:
        Khaos Research
    """
    input_dataset: Dict[SimpleTabularDataset] = {}
    input_model: Dict[MlModel] = {}

    for key, msg in pcs.poll_from_upstream():
        if key == "TabularDataset":
            input_dataset = msg
        if key == "MlModel":
            input_model = msg

    input_dataset_resource = input_dataset["resource"]
    local_dataset_file = pcs.storage.get_file(input_dataset_resource)

    input_dataset_delimiter = input_dataset["delimiter"]

    input_model_resource = input_model["resource"]
    local_model_file = pcs.storage.get_file(input_model_resource)

    input_model_label = input_model["label"]

    # read tabular dataset
    dataset = pd.read_csv(local_dataset_file, delimiter=input_dataset_delimiter)

    # remove label column
    pcs.info([f"Removing label from features values"])

    features = list(dataset.columns.values)
    features.remove(input_model_label)

    # create dataframe and predict label
    df = dataset[features]

    with open(local_model_file, "rb") as handle:
        model = pickle.load(handle)
        y_pred = model.predict(df)

    df[input_model_label] = y_pred

    pcs.info([f"Saving to disk"])

    out_csv = Path(pcs.storage.local_dir, "predict.csv")
    df.to_csv(out_csv, index=False)

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_csv)

    # send to downstream
    dataset_out = SimpleTabularDataset(resource=dfs_dir, delimiter=",", file_format=".csv")
    pcs.to_downstream(dataset_out)

    return {"output": dataset_out, "resource": dfs_dir}
