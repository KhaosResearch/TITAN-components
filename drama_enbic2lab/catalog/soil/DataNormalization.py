import pandas as pd

from matplotlib import *
from sklearn.preprocessing import StandardScaler

from drama.process import Process
from drama.models.task import TaskResult
from drama.core.model import SimpleTabularDataset


def execute(pcs: Process):
    """
    Normalized a given data, each factor have mean 0 and standard deviation 1.
    Args:
        pcs (Process)
    Parameters:

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

    # Creating the dataframe and dropping null values to avoid errors
    try:
        df = pd.read_csv(local_file_path, sep=input_file_delimiter).dropna()
    except:
        raise ValueError("The file input is not in the valid format")

    # Factors in the output
    factors = df.columns.values.tolist()

    # Centering and scaling the data so that the means for each factor are 0 and the standard deviation are 1
    scaled_data = StandardScaler().fit_transform(df)

    # Create output dataframe
    scaled_df = pd.DataFrame(scaled_data, columns=factors)

    # prepare output for the time series output
    out_csv = Path(pcs.storage.local_dir, "DataNormalized.csv")
    scaled_df.to_csv(out_csv, sep=input_file_delimiter, index=False)

    # send time to remote storage
    dfs_dir_output = pcs.storage.put_file(out_csv)

    # send to downstream
    out_csv = SimpleTabularDataset(resource=dfs_dir_output, delimiter=input_file_delimiter, file_format=".csv")
    pcs.to_downstream(out_csv)

    return TaskResult(files=[dfs_dir_output])
