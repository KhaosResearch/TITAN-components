from collections import Counter
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process
from keras.models import load_model

from drama_titan.model import MlModel


def run_classifier(batch, model):
    batch = np.array(batch)
    pred = model.predict(batch)
    class_pred = np.argmax(pred, axis=1)
    counts = Counter(class_pred)

    # voting
    v = list(counts.values())
    k = list(counts.keys())

    batch_fit = k[v.index(max(v))]
    return batch_fit


def classify(dataset_file: str, model_file: str, time_window: int, stride: int, batch_size: int, ignore_columns: list):
    output_sequence = []

    df = pd.read_csv(dataset_file)
    df.drop(ignore_columns, axis="columns", inplace=True)

    data = df.values
    model = load_model(model_file)

    offset = 0

    current_batch = []
    while (offset + time_window) < data.shape[0]:
        current_batch.append(data[offset : (offset + time_window)])
        if len(current_batch) == batch_size:
            output_sequence.append(run_classifier(current_batch, model))
            current_batch = []
        offset += stride

    if len(current_batch) > 0:
        output_sequence.append(run_classifier(current_batch, model))

    return np.array(output_sequence)


def execute(pcs: Process, time_window: int = 1000, stride: int = 10, batch_size: int = 1, ignore_columns=None, *kwargs):
    """
    Generic classifier predict stage for algorithms using a sliding window.

    Args:
        pcs (Process)

    Parameters:
        time_window (int): Size of the sliding window. Defaults to 1000.
        stride (int): Movement of the sliding window. Defaults to 10.
        ignore_columns (list): List of column names to ignore during training. Defaults to None.
        batch_size (int): Batch size for training. Defaults to 1.

    Inputs:
        TabularDataset: Dataset
        MlModel: Machine learning classifier model

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

    input_model_label = input_model["label"]
    input_model_resource = input_model["resource"]
    local_model_file = pcs.storage.get_file(input_model_resource)

    if ignore_columns is None:
        ignore_columns = []
    ignore_columns.append(input_model_label)

    # read tabular dataset
    pcs.info(f"Classifying input dataset using a sliding window of size {time_window}")

    out = classify(local_dataset_file, local_model_file, time_window, stride, batch_size, ignore_columns)
    df = pd.DataFrame(list(zip(out)), columns=["Class"])

    pcs.info([f"Saving to disk"])

    out_csv = Path(pcs.storage.local_dir, "predict.csv")
    df.to_csv(out_csv, index=False)

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_csv)

    # send to downstream
    dataset_out = SimpleTabularDataset(resource=dfs_dir, delimiter=",", file_format=".csv")
    pcs.to_downstream(dataset_out)

    return {"output": dataset_out, "resource": dfs_dir}
