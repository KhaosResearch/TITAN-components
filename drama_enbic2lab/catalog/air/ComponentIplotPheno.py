from pathlib import Path

from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Png


def execute(
    pcs: Process,
    method: str = "percentage",
    nTypes: int = 15,
    thDay: int = 100,
    perc: int = 95,
    defSeason: str = "natural",
    reduction: bool = False,
    redLevel: int = 0.90,
    derivative: int = 5,
    man: int = 11,
    thMa: int = 5,
    nClinical: int = 5,
    windowClinical: int = 7,
    windowGrains: int = 5,
    thPollen: int = 10,
    thSum: int = 100,
    type: str = "none",
    interpolation: bool = True,
    intMethod: str = "lineal",
    typePlot: str = "static",
    exportPlot: bool = False,
    exportFormat: str = "pdf",
):
    """
    Can be interactive
    Graphical representation of phenological parameters
    Args:
        pcs (Process)
    Parameters:
        method (str): A character string specifying the method applied to
            calculate the pollen season and the main parameters. The implemented
            methods that can be used are: "percentage", "logistic", "moving",
            "clinical" or "grains"
        nTypes (int): An integer value specifying the number of the most
            abundant pollen types that must be represented in the pollen calendar
        thDay (int): A numeric value in order to calculate the number of days
            when this level is exceeded for each year and each pollen type.
            This value will be obtained in the results of the function
        perc (int): A numeric value ranging 0_100. This argument is valid only
            for method = 'percentage'. This value represents the percentage of
            the total annual pollen included in the pollen season, removing
            (100_perc)/2% of the total pollen before and after of the pollen
            season
        defSeason (str): Ac haracter string specifying the method for selecting
            the best annual period to calculate the pollen season. The pollen
            seasons may occur within thenatural year or otherwise may occur
            between two years which determines thebest annual period considered.
            The implemented options that can be used are: 'natural', 'interannual'
            or 'peak'
        reduction (bool): This argument is valid only for the 'logistic' method.
            If FALSE the reduction of the pollen data is not applicable.
            If TRUEa reduction ofthe peaks above a certain level
            (red.level argument) will be carried out beforethe definition of the
            pollen season
        redLevel (int): A numeric value ranging 0_1 specifying the percentile
            used as level to reducethe peaks of the pollen series before the
            definition of the pollen season. This argument is valid only for
            the 'logistic' method
        derivative (int): An integer value belonging to options of 4, 5 or 6
            specifying the derivative that will be applied to calculate the
            asymptotes which determines the pollen season using the 'logistic'
            method
        man (int): An integer value specifying the order of the moving average
            applied to calculate the pollen season using the 'moving' method.
            This argument is valid only for the 'moving' method
        thMa (int): A numeric value specifying the threshold used for the
            "moving" method for defining the beginning and the end of the pollen
            season. This argument is valid only for the "moving" method
        nClinical (int): An integer value specifying the number of days which
            must exceeda given threshold (th.pollen argument) for defining the
            beginning and the end of the pollen season. This argument is valid
            only for the "clinical" method
        windowClinical (int): An integer value specifying the time window during
            which the conditions must be evaluated for defining the beginning
            and the end of the pollen season using the "clinical" method. This
            argument is valid only for the "clinical" method
        windowGrains (int): An integer value specifying the time window during
            which the conditions must be evaluated for defining the beginning
            and the end of the pollen season using the "grains" method. This
            argument is valid only for the "grains" method
        thPollen (int): A numeric value specifying the threshold that must be
            exceeded during a given number of days (n.clinical or window.grains
            arguments) for defining the beginning and the end of the pollen
            season using the "clinical" or "grains" methods. This argument is
            valid only for the "clinical" or "grains" methods
        thSum (int): A numeric value specifying the pollen threshold that must
            be exceeded by the sum of daily pollen during a given number of days
            (n.clinical argument) exceeding a given daily threshold (th.pollen
            argument) for defining the beginning and the end of the pollen season
            using the "clinical" method. This argument is valid only for the
            "clinical" method
        type (str): A character string specifying the parameters considered
            according to a specific pollen type for calculating the pollen
            season using the "clinical" method. The implemented pollen types
            that may be used are: "birch", "grasses", "cypress", "olive" or
            "ragweed". As result for selecting any of these pollen types the
            parameters n.clinical, window.clinical, th.pollen and th.sum will be
            automatically adjusted for the "clinical" method. If no pollen types
            are specified (type = "none"), these parameters will be considered
            by the user. This argument is valid only for the "clinical" method
        interpolation (bool): A logical value. If FALSE the interpolation of the
            pollen data is not applicable. If TRUE an interpolation of the
            pollen series will be applied to complete the gaps with no data
            before the calculation of the pollen season
        intMethod (str): A character string specifying the method selected to
            apply the interpolation method in order to complete the pollen series.
            The implemented methods that may be used are: "lineal", "movingmean",
            "spline" or "tseries"
        typePlot (str): A character string specifying the type of plot selected
            to show the phenological plot. The implemented types that may be used
            are: "static" generates a static ggplot object and "dynamic"
            generates a dynamic plotly object
        exportPlot (bool): A logical value specifying if a phenological plot
            saved in the working directory will be required or not. If FALSE
            graphical results will only be displayed in the active graphics
            window. If TRUE graphical results will be displayed in the active
            graphics window and also a pdf or png file (according to the
            export.format argument) will be saved within the plot_AeRobiology
            directory automatically created in the working directory. This
            argument is applicable only for "static" plots
        exportFormat (str): A character string specifying the format selected
            to save the phenological plot. The implemented formats that may be
            used are: "pdf" and "png". This argument is applicable only for
            "static" plots

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
    out_png = Path(pcs.storage.local_dir, "IplotPheno.png")

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
