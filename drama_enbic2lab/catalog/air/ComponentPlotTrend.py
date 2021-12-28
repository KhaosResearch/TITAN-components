from pathlib import Path

from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Png


def execute(
    pcs: Process,
    interpolation: bool = True,
    intMethod: str = "lineal",
    exportPlot: bool = True,
    exportFormat: str = "pdf",
    exportResult: bool = False,
    method: str = "percentage",
):
    """
    Produces high number of trend plots. Should be stored or displayed by
    pollen type
    Graphical representation of trends in some main pollen season variables
    Args:
        pcs (Process)
    Parameters:
        interpolation (bool): A logical value specifying if the visualization
            shows the gaps in the inputs data (interpolation = FALSE) or if an
            interpolation method is used for filling the gaps
            (interpolation = TRUE)
        intMethod (str): A character string with the name of the interpolation
            method to be used. The implemented methods that may be used are:
            "lineal", "movingmean", "tseries" or "spline"
        exportPlot (bool): A logical value specifying if a plot will be exported
            or not. If FALSE graphical results will only be displayed in the
            active graphics window. If TRUE graphical results will be displayed
            in the active graphics window and also one pdf/png file will be
            saved within the plot_AeRobiology directory automatically created
            in the working directory
        exportFormat (str): A character string specifying the format selected
            to save the plot. The implemented formats that may be used are:
            "pdf" or "png"
        exportResult (bool): A logical value. If export.result = TRUE, a table
            is exported with the extension .xlsx, in the directory
            table_AeRobiology
        method (str): A character string specifying the method applied to
            calculate the pollen season and the main seasonal parameters. The
            implemented methods that can be used are: "percentage", "logistic",
            "moving", "clinical" or "grains". By default, method = "percentage"
            (perc = 95%)

    Inputs:
        TabularDataSet (Simple Dataset)
    Outputs:
        Image (Png)

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

    # test
    print("Prueba de funcionamiento")

    # prepare output for the time series output
    out_png = Path(pcs.storage.local_dir, "PlotTrend.png")

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

    return TaskResult(files=[dfs_dir_png])
