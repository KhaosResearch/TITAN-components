import pickle
from pathlib import Path
from typing import Dict

import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process
from sklearn.tree import DecisionTreeClassifier

from drama_titan.model import MlModelDTC


def execute(pcs: Process, label: str):
    """
    Decision tree classifier training stage.

    Args:
        pcs (Process)

    Parameters:
        label (str): Classification column name

    Inputs:
        TabularDataset: Input dataset

    Outputs:
        MlModelDTC: Machine learning DT model

    Produces:
        dtc_model.bin.pickle (Path)

    Author:
        Khaos Research
    """
    # read inputs
    inputs = pcs.get_from_upstream()

    input_file: Dict[SimpleTabularDataset] = inputs["TabularDataset"][0]
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
    dtc = DecisionTreeClassifier()
    dtc.fit(x, y)

    # send to remote storage
    out_model = Path(pcs.storage.local_dir, "dtc_model.bin.pickle")

    with open(out_model, "wb") as handle:
        pickle.dump(dtc, handle, protocol=pickle.HIGHEST_PROTOCOL)

    dfs_dir = pcs.storage.put_file(out_model)

    # send to downstream
    param = dtc.get_params()

    model = MlModelDTC(
        resource=dfs_dir,
        label=label,
        class_weight=str(param["class_weight"]),
        criterion=param["criterion"],
        max_depth=str(param["max_depth"]),
        max_features=str(param["max_features"]),
        max_leaf_nodes=str(param["max_leaf_nodes"]),
        min_impurity_decrease=param["min_impurity_decrease"],
        min_impurity_split=str(param["min_impurity_split"]),
        min_samples_leaf=param["min_samples_leaf"],
        min_samples_split=param["min_samples_split"],
        min_weight_fraction_leaf=param["min_weight_fraction_leaf"],
        splitter=param["splitter"],
    )

    pcs.to_downstream(model)

    return {"output": model, "resource": dfs_dir}
