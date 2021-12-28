from pathlib import Path

from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
from drama.process import Process

from drama_enbic2lab.model import Png


def execute(
    pcs: Process,
    yStart: int,
    yEnd: int,
    method: str = "heatplot",
    nTypes: int = 15,
    startMonth: int = 1,
    perc1: float = 80,
    perc2: float = 99,
    thPollen: float = 1,
    averageMethod: str = "avg_before",
    period: str = "daily",
    methodClasses: str = "exponential",
    nClasses: int = 5,
    classes: float = (25, 50, 100, 300),
    color: str = "green",
    interpolation: bool = True,
    intMethod: str = "lineal",
    naRemove: bool = True,
    result: str = "plot",
    exportPlot: bool = False,
    exportFormat: str = "pdf",
    legendName: str = "Pollen / m3",
):
    """
    Users must choose the method according to their preferences
    Graphical representation of the pollen calendar
    Args:
        pcs (Process)
    Parameters:
        yStart (int): An integer value specifying the period selected to
            calculate the pollen calendar (start year _ end year). If y.start and
            y.end are not specified (NULL), the entire database will be used to
            generate the pollen calendar
        yEnd (int): An integer value specifying the period selected to calculate
            the pollen calendar (start year _ end year). If y.start and y.end are
            not specified (NULL), the entire database will be used to generate
            the pollen calendar
        method (str): A character string specifying the method applied to
            calculate and generate the pollen calendar.
            The implemented methods that can be used are: 'heatplot',
            'violinplot' or 'phenological'
        nTypes (int): An integer value specifying the number of the most
            abundant pollen types that must be represented in the pollen calendar
        startMonth (int): An integer value ranging 1_12 specifying the number of
            the month (January_December) when the beginning of the pollen
            calendar must be considered. This argument is only applicable for the
            'heatplot' method with 'daily' period, for the 'phenological' method
            with 'avg_before' average.method, and for the 'violinplot' method,
            and the rest of methods only may be generatedfrom the January
        perc1 (float): A numeric value ranging 0_100. These arguments are valid
            only for the 'phenological' method. These values represent the
            percentage of the total annual pollen included in the pollen season,
            removing (100_percentage)/2% of the total pollen before and after of
            the pollen season. Two percentages must be specified because of the
            definition of the 'main pollination period' (perc1) and 'early/late
            pollination' (perc2) based on the 'phenological' method proposed by
            Werchan et al. (2018)
        perc2 (float): A numeric value ranging 0_100. These arguments are valid
            only for the 'phenological' method. These values represent the
            percentage of the total annual pollen included in the pollen season,
            removing (100_percentage)/2% of the total pollen before and after of
            the pollen season. Two percentages must be specified because of the
            definition of the 'main pollination period' (perc1) and 'early/late
            pollination' (perc2) based on the 'phenological' method proposed by
            Werchan et al. (2018)
        thPollen (float): A numeric value specifying the minimum threshold of the
            average pollen concentration which will be used to generate the pollen
            calendar. Days below this threshold will not be considered. For the
            'phenological' method this value limits the 'possible occurrence'
            period as proposed by Werchan et al. (2018)
        avg_before (str): A character string specifying the moment of the
            application of the average. This argument is valid only for the
            'phenological' method. The implemented methods that can be used are:
            'avg_before' or 'avg_after'. 'avg_before' produces the average to the
            daily concentrations and then pollen season are calculated for all
            pollen types, this method is recommended as it is a more concordant
            method with respect to the rest of implemented methods. Otherwise,
            'avg_after' determines the pollen season for all years and all pollen
            types, and then a circular average is calculated for the start_dates
            and end_dates
        period (str): A character string specifying the interval time considered
            to generate the pollen calendar. This argument is valid only for the
            'heatplot' method. The implemented periods that can be used are: 'daily'
            or 'weekly'. 'daily' selection produces a pollen calendar using daily
            averages during the year and 'weekly' selection produces a pollen
            calendar using weekly averages during the year
        methodClasses (str): A character string specifying the method to define
            the classes used for classifying the average pollen concentrations
            to generate the pollen calendar. This argument is valid only for the
            'heatplot' method.
        nClasses (int): An integer value specifying the number of classes that
            will be used for classifying the average pollen concentrations to
            generate the pollen calendar. This argument is valid only for the
            'heatplot' method and the classification by method.classes = 'custom'
        classes (float): A numeric vector specifying the threshold established
            to define the different classes that will be used for classifying the
            average pollen concentrations to generate the pollen calendar. This
            argument is valid only for the 'heatplot' method and the classification
            by method.classes = 'custom'
        color (str): A character string specifying the color to generate the graph
            showing the pollen calendar. This argument is valid only for the
            'heatplot' method. The implemented color palettes to generate the
            pollen calendar are: 'green', 'red', 'blue', 'purple' or 'black'.
        interpolation (bool): A logical value. If FALSE the interpolation of the
            pollen data is not applicable. If TRUE an interpolation of the pollen
            series will be applied to complete the gaps before the calculation
            of the pollen calendar
        intMethod (str): A character string specifying the method selected to
            apply the interpolation method in order to complete the pollen series.
            The implemented methods that may be used are: 'lineal', 'movingmean',
            'spline' or 'tseries'.
        naRemove (bool): A logical value specifying if na values must be remove
            for the pollen calendar or not.
        result (str): A character string specifying the output for the function.
            The implemented outputs that may be obtained are: 'plot' and 'table'
        exportPlot (bool): A logical value specifying if a plot with the pollen
            calendar saved in the working directory will be required or not. If
            FALSE graphical results will only be displayed in the active graphics
            window. If TRUE graphical results will be displayed in the active
            graphics window and also a pdf file will be saved within the
            plot_AeRobiology directory automatically created in the working
            directory
        exportFormat (str): A character string specifying the format selected to
            save the pollen calendar plot. The implemented formats that may be
            used are: 'pdf' and 'png'
        legendName (str): A character string specifying the title of the legend

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
    out_png = Path(pcs.storage.local_dir, "PollenCalendar.png")

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
