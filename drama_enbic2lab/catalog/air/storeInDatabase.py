from pathlib import Path

import pandas as pd
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import ExcelDataset


def execute(
    pcs: Process,
    summary_sheet: str = "Resumen",
    data_columns: str = "B:AO",
):
    """
    Stores the summary of an xlsx template file into a xlsx databse
    Args:
        pcs (Process)
    Parameters:
        summary_sheet (str): Name of the xlsx sheet that includes the summay. Defaults to 'Resumen'
        data_columns (str): Range of columns to include in format <initial>:<end> using excel column names. Defaults to 'B:AO'
    Inputs:
        ExcelDataset: Input data xlsx template
        TempFile: Data base xlsx file
    Outputs:
        ExcelDataset (ExcelDataset): Excel database with the new data appended
    Produces:
        output.xlsx (Path)
    Author:
        Khaos Research
    """
    # read inputs
    input_xlsx_template = {}
    input_xlsx_db = {}

    for key, msg in pcs.poll_from_upstream():
        if key == "ExcelDataset":
            input_xlsx_template = msg
        if key == "TempFile":
            input_xlsx_db = msg

    input_xlsx_template_resource = input_xlsx_template["resource"]
    input_xlsx_db_resource = input_xlsx_db["resource"]

    local_xlsx_template_path = pcs.storage.get_file(input_xlsx_template_resource)
    local_xlsx_db_path = pcs.storage.get_file(input_xlsx_db_resource)

    # Read input file
    pcs.info([f"Reading input file {local_xlsx_template_path}"])
    xlsx_template_sheet = pd.read_excel(local_xlsx_template_path, sheet_name=summary_sheet, usecols=data_columns)

    # Removing Nan data
    xlsx_template_sheet = xlsx_template_sheet.dropna(how="all")

    # Read database
    pcs.info([f"Reading input file {local_xlsx_db_path}"])
    xlsx_db_sheet = pd.read_excel(local_xlsx_db_path)

    # Removing Nan data
    xlsx_db_sheet = xlsx_db_sheet.dropna(how="all")

    # Merging sheets
    xlsx_sheet = pd.concat([xlsx_template_sheet, xlsx_db_sheet], ignore_index=True)

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
