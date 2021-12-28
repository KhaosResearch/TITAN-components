import pickle
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process
from sklearn.neighbors import KNeighborsClassifier

from drama_titan.model import MlModelKNN


def execute(pcs: Process, label: str, n_neighbors: int = -1):
    """
    KNN classifier training stage.

    Args:
        pcs (Process)

    Parameters:
        label (str): Classification column name
        n_neighbors (int): Number of neighbors to use. Defaults to -1.

    Inputs:
        TabularDataset: Input dataset

    Outputs:
        MlModelKNN: Machine learning KNN model

    Produces:
        knn_model.bin.pickle (Path)

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

    if n_neighbors == -1:
        n_neighbors = len(np.unique(y))

    # train model
    kn = KNeighborsClassifier(n_neighbors=n_neighbors)
    kn.fit(x, y)

    # send to remote storage
    out_model = Path(pcs.storage.local_dir, "knn_model.bin.pickle")

    with open(out_model, "wb") as handle:
        pickle.dump(kn, handle, protocol=pickle.HIGHEST_PROTOCOL)

    dfs_dir = pcs.storage.put_file(out_model)

    # send to downstream
    param = kn.get_params()

    model = MlModelKNN(
        resource=dfs_dir,
        n_neighbors=param["n_neighbors"],
        label=label,
        algorithm=param["algorithm"],
        leaf_size=param["leaf_size"],
        metric=param["metric"],
        metric_params=str(param["metric_params"]),
        p=param["p"],
        n_jobs=str(param["n_jobs"]),
        weights=param["weights"],
    )

    pcs.to_downstream(model)

    return {"output": model, "resource": dfs_dir}
