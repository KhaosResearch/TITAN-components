import pickle
from pathlib import Path
from typing import Dict

import pandas as pd
from drama.process import Process
from sklearn.ensemble import RandomForestClassifier

from drama_titan.model import MlModelRF, TabularDataset


def execute(pcs: Process, label: str, n_estimators: int = 100):
    """
    Random forest classifier training stage.

    Args:
        pcs (Process)

    Parameters:
        label (str): Classification column name
        n_estimators (int): Number of trees in the forest to use. Defaults to 100.

    Inputs:
        TabularDataset: Input dataset

    Outputs:
        MlModelRF: Machine learning random forest model

    Produces:
        rf_model.bin.pickle (Path)

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
    rf = RandomForestClassifier(n_estimators=n_estimators)
    rf.fit(x, y)

    # send to remote storage
    out_model = Path(pcs.storage.local_dir, "rf_model.bin.pickle")

    with open(out_model, "wb") as handle:
        pickle.dump(rf, handle, protocol=pickle.HIGHEST_PROTOCOL)

    dfs_dir = pcs.storage.put_file(out_model)

    # send to downstream
    param = rf.get_params()

    model = MlModelRF(
        resource=dfs_dir,
        label=label,
        bootstrap=param["bootstrap"],
        class_weight=str(param["class_weight"]),
        criterion=param["criterion"],
        max_depth=str(param["max_depth"]),
        max_features=param["max_features"],
        max_leaf_nodes=str(param["max_leaf_nodes"]),
        min_impurity_decrease=param["min_impurity_decrease"],
        min_impurity_split=str(param["min_impurity_split"]),
        min_samples_leaf=param["min_samples_leaf"],
        min_samples_split=param["min_samples_split"],
        min_weight_fraction_leaf=param["min_weight_fraction_leaf"],
        n_estimators=param["n_estimators"],
        n_jobs=param["n_jobs"],
        oob_score=param["oob_score"],
        random_state=str(param["random_state"]),
        verbose=param["verbose"],
        warm_start=param["warm_start"],
    )

    pcs.to_downstream(model)

    return {"output": model, "resource": dfs_dir}
