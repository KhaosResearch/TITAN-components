from pathlib import Path

import pandas as pd
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import ExcelDataset


def execute(
    pcs: Process,
    code: str,
    summary_sheet: str = "Resumen",
    data_columns: str = "A:AQ",
):
    """
    Extract summary from template excel file.
    Args:
        pcs (Process)
    Parameters:
        summary_sheet (str): Name of the xlsx sheet that includes the summay. Defaults to 'Resumen'
        data_columns (str): Range of columns to include in format <initial>:<end> using excel column names. Defaults to 'A:AQ'
    Inputs:
        TempFile: Input data xlsx template
    Outputs:
        ExcelDataset (ExcelDataset): Excel database with the summary only
    Produces:
        output.xlsx (Path)
    Author:
        Khaos Research
    """
    # read inputs
    inputs = pcs.get_from_upstream()

    input_xlsx = inputs["TempFile"][0]
    input_xlsx_resource = input_xlsx["resource"]

    local_xlsx_path = pcs.storage.get_file(input_xlsx_resource)

    # Read input file
    pcs.info([f"Reading input file {local_xlsx_path}"])
    xlsx_sheet = pd.read_excel(local_xlsx_path, sheet_name=summary_sheet, usecols=data_columns)

    # Removing Nan data
    xlsx_sheet = xlsx_sheet.dropna(how="all")

    # Inserting code column
    xlsx_sheet.insert(0, "Código", [code] * len(xlsx_sheet))

    # creates `output.xlsx`
    out_path = Path(pcs.storage.local_dir, "output.xlsx")
    xlsx_sheet.to_excel(out_path, index=False)

    if not out_path.is_file():
        raise FileNotFoundError(f"'{out_path}' is missing")

    pcs.info([f"Created file {out_path}"])

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_path)

    # send to downstream
    output_xlsx = ExcelDataset(resource=dfs_dir)
    pcs.to_downstream(output_xlsx)

    return TaskResult(files=[dfs_dir])
