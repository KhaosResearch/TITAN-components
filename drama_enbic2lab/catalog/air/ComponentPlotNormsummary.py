import shutil
from pathlib import Path

import docker
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Pdf


def execute(
    pcs: Process,
    pollen: str,
    mave: int = 1,
    normalized: bool = False,
    interpolation: bool = True,
    intMethod: str = "lineal",
    colorPlot: str = "orange2",
    axisname: str = "Pollen grains / m3",
):
    """
    User must select the pollen type that will be shown and the moving average
    size
    Graphical representation of average, maximum and minimum pollen
    concentrations by type
    Args:
        pcs (Process)
    Parameters:
        pollen (str): A character string with the name of the particle to show.
            This character must match with the name of a column in the input
            database. This is a mandatory argument
        mave (int): An integer value specifying the order of the moving average
            applied to the data
        normalized (bool): A logical value specifying if the visualization shows
            real pollen data (normalized = FALSE) or the percentage of every day
            over the whole pollen season (normalized = TRUE)
        interpolation (bool): A logical value specifying if the visualization
            shows the gaps in the inputs data (interpolation = FALSE) or if an
            interpolation method is used for filling the gaps
            (interpolation = TRUE)
        intMethod (str): A character string with the name of the interpolation
            method to be used. The implemented methods that may be used are:
            "lineal", "movingmean", "tseries" or "spline"
        colorPlot (str): A character string. The argument defines the color to
            fill the plot
        axisname (str): A character string specifying the title of the y axis

    Inputs:
        InputFile (TempFile)
    Outputs:
        Image (Pdf)

    Produces:

    Author:
        Khaos Research
    """

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file = inputs["TempFile"][0]
    input_file_resource = input_file["resource"]

    local_file_path = Path(pcs.storage.get_file(input_file_resource))
    local_component_path = Path(pcs.storage.local_dir)

    try:
        shutil.copyfile(local_file_path, Path(local_component_path, local_file_path.name))
    except IsADirectoryError:
        pcs.debug([f"{local_file_path} fail copy to {local_component_path}. Destination is a directory"])
    except PermissionError:
        pcs.debug(["Permission denied"])
    except:
        pcs.debug([f"{local_file_path} fail copy to {local_component_path}"])

    image_name = "enbic2lab/air/plot_normsummary"

    # get docker image
    client = docker.from_env()
    container = client.containers.run(
        image=image_name,
        volumes={local_component_path: {"bind": "/usr/local/src/data", "mode": "rw"}},
        command=f'-i data/{local_file_path.name} --pollen "{pollen}" --mave {mave} '
        f"--normalized {normalized} --interpolation {interpolation} "
        f'--intMethod "{intMethod}" --colorPlot "{colorPlot}" --axisname "{axisname}"',
        detach=True,
        tty=True,
    )

    r = container.wait()
    logs = container.logs()
    if logs:
        pcs.debug([logs.decode("utf-8")])

    container.stop()
    container.remove(v=True)

    # prepare output for the time series output
    out_pdf = Path(local_component_path, "plot_normsummary.pdf")

    # send time to remote storage
    if not out_pdf.is_file():
        raise FileNotFoundError(f"{out_pdf} is missing")

    dfs_dir_pdf = pcs.storage.put_file(out_pdf)

    # send to downstream
    pdf_output = Pdf(resource=dfs_dir_pdf)

    pcs.to_downstream(pdf_output)

    return TaskResult(files=[dfs_dir_pdf])
