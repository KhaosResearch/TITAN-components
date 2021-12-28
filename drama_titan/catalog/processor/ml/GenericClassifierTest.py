import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Type, Dict

import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process
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


def execute(pcs: Process):
    """
    Generic classifier test stage.

    Args:
        pcs (Process)

    Parameters:
        None

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
    input_dataset: Dict[SimpleTabularDataset] = {}
    input_model: Dict[MlModel] = {}

    # read inputs
    for key, msg in pcs.poll_from_upstream():
        if key == "TabularDataset":
            input_dataset = msg
        if key == "MlModel":
            input_model = msg

    input_dataset_resource = input_dataset["resource"]
    input_dataset_delimiter = input_dataset["delimiter"]

    local_dataset_file = pcs.storage.get_file(input_dataset_resource)

    input_model_resource = input_model["resource"]
    input_model_label = input_model["label"]

    local_model_file = pcs.storage.get_file(input_model_resource)

    # read tabular dataset
    test_df = pd.read_csv(local_dataset_file, delimiter=input_dataset_delimiter)

    # remove label column
    pcs.info([f"Removing label from features values"])
    features = list(test_df.columns.values)
    features.remove(input_model_label)

    # create x and y_true dataframe for tests stage
    x = test_df[features]
    y_true = test_df[input_model_label]

    with open(local_model_file, "rb") as handle:
        model = pickle.load(handle)
        y_pred = model.predict(x)

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
