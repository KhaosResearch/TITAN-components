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
    quantil: int = 0.75,
    significant: int = 0.05,
    split: bool = True,
    result: str = "table",
):
    """
    Users must choose the number, the significant level
    Graphical representation of the pollen trends
    Args:
        pcs (Process)
    Parameters:
        interpolation (bool): A logical value specifying if the visualization
            shows the gaps in the inputs data (interpolation = FALSE) or if an
            interpolation method is used for filling the gaps
            (interpolation = TRUE)
        intMethod (str): A character string with the name of the interpolation
            method to be used. The implemented methods that may be used are:
            'lineal', 'movingmean', 'tseries' or 'spline'
        exportPlot (bool): A logical value specifying if a plot will be exported
            or not. If FALSE graphical results will only be displayed in the
            active graphics window. If TRUE graphical results will be displayed
            in the active graphics window and also one pdf/png file will be saved
            within the plot_AeRobiology directory automatically created in the
            working directory
        exportFormat (str): A character string specifying the format selected
            to save the plot. The implemented formats that may be used are:
            'pdf' or 'png'
        exportResult (bool): A logical value. If export.result = TRUE, a table
            is exported with the extension .xlsx, in the directory
            table_AeRobiology. This table has the information about the slope
            'beta coefficient of a lineal model using as predictor the year and
            as dependent variable one of the main pollen season indexes'. The
            information is referred to the main pollen season indexes: Start Date,
            Peak Date, End Date and Pollen Integral
        method (str): A character string specifying the method applied to
            calculate the pollen season and the main seasonal parameters. The
            implemented methods that can be used are: 'percentage', 'logistic',
            'moving', 'clinical' or 'grains'
        quantil (int): An integer value (between 0 and 1) indicating the quantile
            of data to be displayed in the graphical output of the function.
            quantil = 1 would show all the values, however a lower quantile will
            exclude the most extreme values of the sample. To split the parameters
            using a different sampling units (e.g. dates and pollen concentrations)
            can be used low vs high values of quantil argument (e.g. 0.5 vs 1).
            Also can be used an extra argument: split
        significant (int): An integer value indicating the significant level to
            be considered in the linear trends analysis. This p level is
            displayed in the graphical output of the function
        split (bool): A logical argument. If split = TRUE, the plot is separated
            in two according to the nature of the variables (i.e. dates or
            pollen concentrations)
        result (str): A character object with the definition of the object to be
            produced by the function. If result == 'plot', the function returns
            a list of objects of class ggplot2; if result == 'table', the
            function returns a data.frame

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
    out_png = Path(pcs.storage.local_dir, "AnalyseTrend.png")

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
