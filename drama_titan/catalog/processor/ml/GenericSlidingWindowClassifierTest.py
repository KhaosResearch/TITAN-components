from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process
from keras.models import load_model
from sklearn.metrics import (
    confusion_matrix,
    accuracy_score,
    classification_report,
)

from drama_titan.model import MlModel


@dataclass
class ConfusionMatrix(SimpleTabularDataset):
    pass


@dataclass
class Scores(SimpleTabularDataset):
    pass


@dataclass
class ClassificationReport(SimpleTabularDataset):
    pass


def read_data_with_sliding_window(
    time_window_: int, stride_: int, file: str, delimiter: str, classification_label: str, ignore_columns: list = []
):

    # Read dataset
    train_df = pd.read_csv(file, delimiter=delimiter)
    train_df.drop(ignore_columns, axis=1, inplace=True)

    x = []
    y = []

    # remove label column
    features = list(train_df.columns.values)
    features.remove(classification_label)
    data = train_df[features]

    label_values = train_df[classification_label].unique()
    class_index = -1
    for class_ in label_values:
        class_index += 1
        offset = 0
        data_subset = data[train_df[classification_label] == class_].values
        rows = data_subset.shape[0]
        while (offset + time_window_) < rows:
            sample = data_subset[offset : (offset + time_window_)]
            x.append(sample)
            class_value = np.zeros(len(label_values))
            class_value[class_index] = 1.0
            y.append(class_value)
            offset += stride_
    x = np.array(x)
    y = np.array(y)
    return x, y


def execute(pcs: Process, time_window: int = 1000, stride: int = 10, ignore_columns=None):
    """
    Generic classifier test stage for algorithms using a sliding window.

    Args:
        pcs (Process)

    Parameters:
        time_window (int): Size of the sliding window. Defaults to 1000.
        stride (int): Movement of the sliding window. Defaults to 10.
        ignore_columns (list): List of column names to ignore during training. Defaults to None.

    Inputs:
        TabularDataset: Dataset
        MlModel: Machine learning classifier model

    Outputs:
        ConfusionMatrix (SimpleTabularDataset)
        Scores (SimpleTabularDataset)
        ClassificationReport (SimpleTabularDataset)

    Produces:
        ClassifierTestConfusionMatrix.csv (Path)
        ClassifierTestScore.csv (Path)
        ClassifierTestReport.csv (Path)

    Author:
        Khaos Research
    """
    if ignore_columns is None:
        ignore_columns = []

    input_dataset: Dict[SimpleTabularDataset] = {}
    input_model: Dict[MlModel] = {}

    # read inputs
    for key, msg in pcs.poll_from_upstream():
        if key == "TabularDataset":
            input_dataset = msg
        if key == "MlModel":
            input_model = msg

    input_dataset_delimiter = input_dataset["delimiter"]
    input_dataset_resource = input_dataset["resource"]
    local_dataset_file = pcs.storage.get_file(input_dataset_resource)

    input_model_label = input_model["label"]
    input_model_resource = input_model["resource"]
    local_model_file = pcs.storage.get_file(input_model_resource)

    # read tabular dataset and create x and y dataframes
    pcs.info(f"Reading dataset using a sliding window of size {time_window}")

    x, y_true = read_data_with_sliding_window(
        time_window, stride, local_dataset_file, input_dataset_delimiter, input_model_label, ignore_columns
    )

    pcs.info("Loading model")

    model = load_model(local_model_file)
    y_pred = model.predict(x)

    y_true = y_true.argmax(axis=1)
    y_pred = y_pred.argmax(axis=1)

    # compute confusion matrix
    labels = list(set(y_true))
    conf_matrix = confusion_matrix(y_true, y_pred, labels=labels)
    conf_matrix_df = pd.DataFrame.from_records(conf_matrix).reset_index()

    pcs.info([f"Saving to disk"])

    out_csv = Path(pcs.storage.local_dir, "ClassifierTestConfusionMatrix.csv")
    conf_matrix_df.to_csv(out_csv, index=False)

    # send to remote storage
    dfs_dir_conf_matrix = pcs.storage.put_file(out_csv)

    # send to downstream
    dataset_conf_matrix = ConfusionMatrix(resource=dfs_dir_conf_matrix, delimiter=",", file_format=".csv")
    pcs.to_downstream(dataset_conf_matrix)

    # compute accuracy score
    score = accuracy_score(y_true, y_pred)
    score_data = {"Accuracy Score": [score]}
    score_df = pd.DataFrame.from_dict(score_data)

    pcs.info([f"Saving to disk"])

    out_csv = Path(pcs.storage.local_dir, "ClassifierTestScore.csv")
    score_df.to_csv(out_csv, index=False)

    # send to remote storage
    dfs_dir_accuracy = pcs.storage.put_file(out_csv)

    # send to downstream
    dataset_score = Scores(resource=dfs_dir_accuracy, delimiter=",", file_format=".csv")
    pcs.to_downstream(dataset_score)

    # compute classification report
    report = classification_report(y_true, y_pred, target_names=list(set(y_true)), output_dict=True)
    report_df = pd.DataFrame(report).transpose().reset_index()

    pcs.info([f"Saving to disk"])

    out_csv = Path(pcs.storage.local_dir, "ClassifierTestReport.csv")
    report_df.to_csv(out_csv, index=False)

    # send to remote storage
    dfs_dir_report = pcs.storage.put_file(out_csv)

    # send to downstream
    dataset_report = ClassificationReport(resource=dfs_dir_report, delimiter=",", file_format=".csv")
    pcs.to_downstream(dataset_report)

    return {
        "output": [dataset_conf_matrix, dataset_score, dataset_report],
        "resource": [dfs_dir_conf_matrix, dfs_dir_accuracy, dfs_dir_report],
    }
