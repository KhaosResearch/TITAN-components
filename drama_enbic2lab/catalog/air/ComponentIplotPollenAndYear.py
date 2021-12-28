from pathlib import Path

from drama.core.model import SimpleTabularDataset, TempFile
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Png


def execute(pcs: Process, year: int, pollen: str):
    """
    Interactive graphical representation
    Interactive graphs to make discussion with the data in real time
    Args:
        pcs (Process)
    Parameters:
        year (int): An integer value specifying the year to display. This is a
            mandatory argument
        pollen (str): A character string with the name of the particle to show.
            This character must match with the name of a column in the input
            database. This is a mandatory argument

    Inputs:
        InputFile (TempFile)
    Outputs:
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
    out_png = Path(pcs.storage.local_dir, "IplotPollenYear.png")

    try:
        open(out_png, "a").close()
    except OSError:
        print("Failed creating the file")
    else:
        print("File created")

    # send time to remote storage
    dfs_dir_output = pcs.storage.put_file(out_png)

    # send to downstream
    analysis_output = Png(resource=dfs_dir_output)

    pcs.to_downstream(analysis_output)

    return TaskResult(files=[dfs_dir_output])
