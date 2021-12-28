from pathlib import Path

import pandas as pd
from drama.process import Process

from drama_titan.model import TabularDataset


def execute(
    pcs: Process,
    url: str,
    delimiter: str = ",",
    line_delimiter: str = "",
    has_header: bool = True,
    header_names: list = None,
    header_types: list = None,
):
    """
    Imports a tabular dataset from an online resource given its url.

    Args:
        pcs (Process)

    Parameters:
        url (str): Public accessible resource
        delimiter (str): Line column delimiter. Defaults to ",".
        line_delimiter (str): Line delimiter. Defaults to None.
        has_header (bool): Whenever the dataset has header line. Defaults to True.
        header_names (list): Column names. Defaults to None.
        header_types (list): Column types. Default to None.

    Inputs:
        None

    Outputs:
        TabularDataset: Dataset

    Produces:
        out.csv (Path): Output dataset.
        
    Author:
        Khaos Research
    """
    filename = url.split("/")[-1]
    out_csv = Path(pcs.storage.local_dir, filename)

    df = pd.read_csv(
        url,
        sep=delimiter,
        header=0 if has_header else None,
        names=header_names if header_names and not has_header else None,
        dtype=dict(zip(header_names, header_types)) if header_names and header_types else None,
    )
    df.to_csv(out_csv, index=False)

    rows, cols = df.shape

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_csv, "out.csv")

    # send to downstream
    output_tabular_dataset = TabularDataset(
        resource=dfs_dir,
        has_header=has_header,
        delimiter=delimiter,
        line_delimiter=line_delimiter,
        number_of_columns=cols,
        number_of_lines=rows,
    )
    pcs.to_downstream(output_tabular_dataset)

    return {"output": output_tabular_dataset, "resource": dfs_dir}
