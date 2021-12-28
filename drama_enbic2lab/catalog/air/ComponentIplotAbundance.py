from pathlib import Path

from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Png


def execute(
    pcs: Process,
    yStart: int,
    yEnd: int,
    interpolation: bool = True,
    intMethod: str = "lineal",
    colBar: str = "#E69F00",
    typePlot: str = "static",
    result: str = "plot",
    exportPlot: bool = False,
    exportFormat: str = "pdf",
    nTypes: int = 15,
    exclude: str = "",
):
    """
    Users must choose the number of pollen types they want to plot. Can be
    interactive
    Graphical representation of relative abundance of each pollen type
    Args:
        pcs (Process)
    Parameters:
        yStart (int): An integer value specifying the period selected to calculate
            relative abundances of the pollen types (start year _ end year). If
            y.start and y.end are not specified (NULL), the entire database will
            be used to generate the pollen calendar
        interpolation (bool): A logical value. If FALSE the interpolation of the
            pollen data is not applicable. If TRUE an interpolation of the pollen
            series will be applied to complete the gaps with no data before the
            calculation of the pollen season
        intMethod (str): A character string specifying the method selected to
            apply the interpolation method in order to complete the pollen series.
            The implemented methods that may be used are: 'lineal', 'movingmean',
            'spline' or 'tseries'
        colBar (str): A character string specifying the color of the bars to
            generate the graph showing the relative abundances of the pollen types
        typePlot (str): A character string specifying the type of plot selected
            to show the plot showing the relative abundance of the pollen types.
            The implemented types that may be used are: static generates a static
            ggplot object and dynamic generates a dynamic plotly object
        result (str): A character string specifying the output for the function.
            The implemented outputs that may be obtained are: 'plot' and 'table'
        exportPlot (bool): A logical value specifying if a plot saved in the
            working directory will be required or not. If FALSE graphical results
            will only be displayed in the active graphics window. If TRUE
            graphical results will be displayed in the active graphics window
            and also a pdf or png file (according to the export.format argument)
            will be saved within the plot_AeRobiology directory automatically
            created in the working directory
        exportFormat (str): A character string specifying the format selected to
            save the plot showing the relative abundance of the pollen types.
            The implemented formats that may be used are: 'pdf' and 'png'. This
            argument is applicable only for 'static' plots
        nTypes (int): An integer value specifying the number of the most abundant
            pollen types that must be represented in the plot of the relative
            abundance
        exclude (str): A character string vector with the names of the pollen
            types to be excluded from the plot

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
    out_png = Path(pcs.storage.local_dir, "IplotAbundance.png")

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
