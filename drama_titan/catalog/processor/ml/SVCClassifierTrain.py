import pickle
from pathlib import Path
from typing import Dict

import pandas as pd
from drama.process import Process
from sklearn.svm import SVC

from drama_titan.model import MlModelSVC, TabularDataset


def execute(pcs: Process, label: str):
    """
    Support Vector Machine classifier training stage.

    Args:
        pcs (Process)

    Parameters:
        label (str): Classification column name

    Inputs:
        Dataset (SimpleTabularDataset): Input dataset

    Outputs:
        MlModelSVC: Machine learning SVM model

    Produces:
        svc_model.bin.pickle (Path)

    Author:
        Khaos Research
    """
    # read inputs
    inputs = pcs.get_from_upstream()

    input_file: Dict[TabularDataset] = inputs["Dataset"][0]
    input_file_resource = input_file["resource"]
    input_file_delimiter = input_file["delimiter"]

    local_input_file = pcs.storage.get_file(input_file_resource)

    # read tabular dataset
    train_df = pd.read_csv(local_input_file, delimiter=input_file_delimiter)

    # remove label column
    features = list(train_df.columns.values)
    features.remove(label)

    # create x and y dataframe for training stage
    x = train_df[features]
    y = train_df[label]

    # train model
    dtc = SVC(gamma="auto")
    dtc.fit(x, y)

    # send to remote storage
    out_model = Path(pcs.storage.local_dir, "svc_model.bin.pickle")

    with open(out_model, "wb") as handle:
        pickle.dump(dtc, handle, protocol=pickle.HIGHEST_PROTOCOL)

    dfs_dir = pcs.storage.put_file(out_model)

    # send to downstream
    param = dtc.get_params()

    model = MlModelSVC(
        resource=dfs_dir,
        label=label,
        C=param["C"],
        cache_size=param["cache_size"],
        class_weight=str(param["class_weight"]),
        coef0=param["coef0"],
        decision_function_shape=param["decision_function_shape"],
        degree=param["degree"],
        gamma=str(param["gamma"]),
        kernel=param["kernel"],
        max_iter=param["max_iter"],
        probability=param["probability"],
        shrinking=param["shrinking"],
        tol=param["tol"],
    )

    pcs.to_downstream(model)

    return {"output": model, "resource": dfs_dir}
