from pathlib import Path
from typing import Dict

import pandas as pd
from drama.core.model import SimpleTabularDataset
from drama.process import Process


def execute(pcs: Process, join_label: str = "class", drop_join_column: bool = True):
    """
    Merges to datasets into one by column.

    Args:
        pcs (Process)

    Parameters:
        join_label (str): Common column name. Defaults to "class".
        drop_join_column (bool): Whenever drop the joined column. Defaults to True.

    Inputs:
        TabularDataset (SimpleTabularDataset): Input dataset A
        TabularDatasetB (SimpleTabularDataset): Input dataset B

    Outputs:
        SimpleTabularDataset: Joined dataset

    Produces:
        merged.csv (Path)

    Author:
        Khaos Research
    """
    input_df_one: Dict[SimpleTabularDataset] = {}
    input_df_two: Dict[SimpleTabularDataset] = {}

    # read inputs
    for key, msg in pcs.poll_from_upstream():
        if key == pcs.inputs["TabularDataset"]:
            input_df_one = msg
        if key == pcs.inputs["TabularDatasetB"]:
            input_df_two = msg

    input_df_one_delimiter = input_df_two["delimiter"]
    input_df_one_resource = input_df_one["resource"]

    local_df_one_file = pcs.storage.get_file(input_df_one_resource)

    input_df_two_delimiter = input_df_two["delimiter"]
    input_df_two_resource = input_df_two["resource"]

    local_df_two_file = pcs.storage.get_file(input_df_two_resource)

    # read datasets
    df1 = pd.read_csv(local_df_one_file, delimiter=input_df_one_delimiter)
    df2 = pd.read_csv(local_df_two_file, delimiter=input_df_two_delimiter)

    df = pd.merge(df1, df2, how="inner", on=join_label)

    if drop_join_column:
        df = df.drop(join_label, 1)

    pcs.info([f"Saving to disk"])

    out_csv = Path(pcs.storage.local_dir, "merged.csv")
    df.to_csv(out_csv, index=False)

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_csv)

    # send to downstream
    output_simple_tabular_dataset = SimpleTabularDataset(resource=dfs_dir, delimiter=",", file_format=".csv")
    pcs.to_downstream(output_simple_tabular_dataset)

    return {"output": output_simple_tabular_dataset, "resource": dfs_dir}
