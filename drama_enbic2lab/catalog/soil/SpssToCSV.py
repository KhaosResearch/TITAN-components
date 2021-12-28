from pathlib import Path
import pandas as pd

from drama.process import Process

from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult


def execute(pcs: Process, drop_index=True):

    """
    Convert the SSPS file into a CSV file
    Args:
        pcs (Process)
    Parameters:
        drop_index(bool): Drop index of tabular dataset
    Inputs:
         TempFile (TempFile): SPSS file
    Outputs:
        TabularDataSet (Simple Dataset): Convertion of the input file in CSV

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file = inputs["TempFile"][0]
    input_file_resource = input_file["resource"]

    local_file_path = pcs.storage.get_file(input_file_resource)

    # Create the dataframe
    try:
        df = pd.read_spss(local_file_path)
    except:
        raise ValueError("The format of the file is not valid")

    # Dropping the unname columns to obtain just the factors of analysis
    if "Unnamed: 0" in df.columns and drop_index:
        df = df.drop("Unnamed: 0", axis=1)

    # prepare the output
    out_csv = Path(pcs.storage.local_dir, "Data.csv")
    df.to_csv(out_csv, index=False, sep=";")

    # send time to remote storage
    dfs_dir_output = pcs.storage.put_file(out_csv)

    # send to downstream
    out_csv = SimpleTabularDataset(resource=dfs_dir_output, delimiter=";", file_format=".csv")
    pcs.to_downstream(out_csv)

    return TaskResult(files=[dfs_dir_output])
