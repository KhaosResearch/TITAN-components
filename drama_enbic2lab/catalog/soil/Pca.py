from pathlib import Path
import pandas as pd
import numpy as np

from matplotlib import *

from sklearn.decomposition import PCA

from drama.process import Process
from drama.models.task import TaskResult

from drama.core.model import SimpleTabularDataset


def _dimension_reduction(percentage_variance: list, variance_explained: int):
    cumulative_variance = 0
    n_components = 0
    while cumulative_variance < variance_explained:
        cumulative_variance += percentage_variance[n_components]
        n_components += 1
    return n_components


def execute(pcs: Process, variance_explained: int = 75, whiten: bool = True, number_components: int = 0):
    """
    Perform a Principal Component Analysis and Dimension Reduction
    Args:
        pcs (Process)
    Parameters:
        variance_explained(int): The total variace that is want it to be explained by the Principal Components.
            Not needed if we set the number of components. Default to 75 %.
        whiten(bool): Choose if the vectors are multiplied by the square root of n_samples and then divided
        by the singular values to ensure uncorrelated outputs with unit component-wise variances.
            Default to True
        number_components(int): Number of components desired after the dimension reduction.
            Default to 0 (Not to use this parameter)

    Inputs:
         TabularDataSet (Simple Dataset): CSV file
    Outputs:
        TabularDataSet (Simple Dataset): CSV with the data normalized

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file = inputs["SimpleTabularDataset"][0]
    input_file_resource = input_file["resource"]
    input_file_delimiter = input_file["delimiter"]

    local_file_path = pcs.storage.get_file(input_file_resource)

    # Creating the dataframe from the normalized data
    try:
        df = pd.read_csv(local_file_path, sep=input_file_delimiter)
    except:
        raise ValueError("The input file format is not correct")

    # Create PCA object
    if number_components == 0:
        pca = PCA(whiten=whiten)
    # PCA object if a number of components is set
    else:
        pca = PCA(n_components=number_components, whiten=whiten)

    # Fitting and transforming the data
    pca.fit(df)
    pca_data = pca.transform(df)

    # Calculating the percentage of variation that each principal component accounts for
    per_var = np.round(pca.explained_variance_ratio_ * 100, decimals=1)

    # If the number of components is not specified we calculate it according to the explained variance
    if number_components == 0:
        number_components = _dimension_reduction(per_var, variance_explained)

    pca_data = pca_data[:, :number_components]

    pc_labels = ["PC" + str(x) for x in range(1, number_components + 1)]

    pca_df = pd.DataFrame(data=pca_data, columns=pc_labels)

    # prepare output for the time series output
    out_csv = Path(pcs.storage.local_dir, "PCA.csv")
    pca_df.to_csv(out_csv, sep=input_file_delimiter, index=False)

    # send time to remote storage
    dfs_dir_output = pcs.storage.put_file(out_csv)

    # send to downstream
    out_csv = SimpleTabularDataset(resource=dfs_dir_output, delimiter=input_file_delimiter, file_format=".csv")
    pcs.to_downstream(out_csv)

    return TaskResult(files=[dfs_dir_output])
