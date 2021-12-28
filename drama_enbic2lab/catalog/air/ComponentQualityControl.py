from pathlib import Path

from drama.core.model import SimpleTabularDataset, TempFile
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Png


def execute(
    pcs: Process,
    window: int = 2,
    perc_miss: int = 20,
    ps_method: str = "percentage",
    percent: int = 95,
    result: str = "plot",
):
    """
    Selecting only good quality data
    Choose which pollen types are going to be analyzed
    Args:
        pcs (Process)
    Parameters:
        window (int): An integer value bigger or equal to 1
        perc_miss (int): An integer value between 0 and 100
        ps_method (str): A character string specifying the method applied to
            calculate the pollen season
        percent (int): An integer value between 0 and 100
        result (str): A character string specifying the format of the results
            (table or plot)

    Inputs:
        InputFile (TempFile)
    Outputs:
        TabularDataSet (Simple Dataset)
        Image (Png)

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file = inputs["TempFile"][0]
    input_file_resource = input_file["resource"]

    local_file_path = pcs.storage.get_file(input_file_resource)

    # test
    print("Prueba de funcionamiento")

    # prepare output for the time series output
    out_csv = Path(pcs.storage.local_dir, "QualityControl.csv")

    try:
        open(out_csv, "a").close()
    except OSError:
        print("Failed creating the file")
    else:
        print("File created")

    # send time to remote storage
    dfs_dir_csv = pcs.storage.put_file(out_csv)

    # send to downstream
    csv_output = SimpleTabularDataset(resource=dfs_dir_csv, delimiter=",", file_format=".csv")

    pcs.to_downstream(csv_output)

    # prepare output for the time series output

    out_png = Path(pcs.storage.local_dir, "QualityControl.png")

    try:
        open(out_png, "a").close()
    except OSError:
        print("Failed creating the file")
    else:
        print("File created")

    # send time to remote storage
    dfs_dir_png = pcs.storage.put_file(out_png)

    # send to downstream
    png_output = Png(resource=dfs_dir_png)

    pcs.to_downstream(png_output)

    return TaskResult(files=[dfs_dir_csv, dfs_dir_png])
