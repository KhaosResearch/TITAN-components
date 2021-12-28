from pathlib import Path

from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Png


def execute(
    pcs: Process,
    pollenType: str,
    year: int,
    days: int = 30,
    filCol: str = "turquoise4",
    axisname: str = "Pollen grains / m ^ 3",
    intMethod: str = "lineal",
):
    """
    Arguments must be coherent with the calculate_ps() arguments. Run separately
    for each pollen type
    Graphical representation
    Args:
        pcs (Process)
    Parameters:
        pollenType (str): A character string specifying the name of the pollen
            type which will be plotted. The name must be exactly the same that
            appears in the column name
        year (int): An integer value specifying the season to be plotted. The
            season does not necessary fit a natural year
        days (int): An integer value specifying the number of days beyond each
            side of the main pollen season that will be represented
        filCol (str): A character string specifying the name of the color to fill
            the main pollen season (Galan et al., 2017) in the plot
        axisname (str): A character string or an expression specifying the y axis
            title of the plot
        intMethod (str): A character string specifying the method selected to
            apply the interpolation method in order to complete the pollen series.
            The implemented methods that may be used are: 'lineal', 'movingmean',
            'spline' or 'tseries'

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
    out_png = Path(pcs.storage.local_dir, "PlotPs.png")

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
