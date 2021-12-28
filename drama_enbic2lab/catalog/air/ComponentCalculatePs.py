from pathlib import Path

from drama.core.model import CompressFile, SimpleTabularDataset
from drama.models.task import TaskResult
from drama.process import Process


def execute(
    pcs: Process,
    method: str,
    thDay: int = 100,
    perc: float = 95,
    defSeason: str = "natural",
    reduction: bool = False,
    redLevel: float = 0.9,
    derivative: int = 5,
    man: int = 11,
    thMa: float = 5,
    nClinical: int = 5,
    windowClinical: int = 7,
    windowGrains: int = 5,
    thPollen: float = 10,
    thSum: float = 100,
    type: str = "none",
    interpolation: bool = True,
    intMethod: str = "lineal",
    maxDays: int = 30,
    result: str = "table",
    plot: bool = True,
    exportPlot: bool = False,
    exportResult: bool = False,
):
    """
    Election between the different methods implemented
    Main pollen season calculation
    Args:
        pcs (Process)
    Parameters:
        method (str): A character string specifying the method applied to
            calculate the pollen season and the main parameters. Options:
            percentage, logistic, moving, clinical or grains
        thDay (int): An integer value. The number of days whose pollen
            concentration is bigger than this threshold is calculated for each
            year and pollen type
        perc (float): This argument is valid only for method = 'percentage'.
            This value represents the percentage of the total annual pollen
            included in the pollen season, removing (100_perc)/2% of the total
            pollen before and after of the pollen season
        defSeason (str): A character string specifying the method for selecting
            the best annual periodto calculate the pollen season. Options:
            'natural', 'interannual' or 'peak'
        reduction (bool): This argument is valid only for the 'logistic' method.
            If FALSE the reduction of the pollen data is not applicable. If TRUE
            a reduction of the peaks above a certain level (red.level argument)
            will be carried out before the definition of the pollen season
        redLevel (float): A numeric value ranging 0_1 specifying the percentile
            used as level to reduce the peaks of the pollen series before the
            definition of the pollen season. This argument is valid only for the
            'logistic' method
        derivative (int): A numeric (integer) value belonging to options of 4,
            5 or 6 specifying the derivative that will be applied to calculate
            the asymptotes which determines the pollen season using the 'logistic'
            method. This argument is valid only for the 'logistic' method
        man (int): An integer value specifying the order of the moving average
            applied to calculate the pollen season using the 'moving' method.
            This argument is valid only for the 'moving' method
        thMa (float): A numeric value specifying the threshold used for the
            'moving' method for defining the beginning and the end of the pollen
            season
        nClinical (int): An integer value specifying the number of days which
            must exceed a given threshold (th.pollen argument) for defining the
            beginning and the end of the pollen season. This argument is valid
            only for the 'clinical' method
        windowClinical (int): An integer value specifying the time window during
            which the conditions must be evaluated for defining the beginning and
            the end of the pollen season using the 'clinical' method. This
            argument is valid only for the 'clinical' method
        windowGrains (int): An integet value specifying the time window during
            which the conditions must be evaluated for defining the beginning and
            the end of the pollen season using the 'grains' method. This argument
            is valid only for the 'grains'
        thPollen (float): A numeric value specifying the threshold that must be
            exceeded during a given number of days (n.clinical or window.grains
            arguments) for defining the beginning and the end of the pollen
            season using the 'clinical' or 'grains' methods
        thSum (float): A numeric value specifying the pollen threshold that must
            be exceeded by the sum of daily pollen during a given number of days
            (n.clinical argument) exceeding a given daily threshold (th.pollen
            argument) for defining the beginning and the end of the pollen season
            using the 'clinical' method
        type (str): A character string specifying the parameters considered
            according to a specific pollen type for calculating the pollen season
            using the 'clinical' method. The implemented pollen types that may be
            used are: 'birch', 'grasses', 'cypress', 'olive' or 'ragweed'. As
            result for selecting any of these pollen types the parameters
            n.clinical, window.clinical, th.pollen and th.sum will be
            automatically adjusted for the 'clinical' method. If no pollen types
            are specified (type = 'none'), these parameters will be considered
            by the user
        interpolation (bool): A logical value. If FALSE the interpolation of the
            pollen data is not applicable. If TRUE an interpolation of the pollen
            series will be applied to complete the gaps with no data before the
            calculation of the pollen season
        intMethod (str): A character string specifying the method selected to
            apply the interpolation method in order to complete the pollen series.
            The implemented methods that may be used are: 'lineal', 'movingmean',
            'spline' or 'tseries'
        maxDays (int): An integer value specifying the maximum number of
            consecutive days with missing data that the algorithm is going to
            interpolate. If the gap is bigger than the argument value, the gap
            will not be interpolated
        result (str): A character string specifying the output for the function.
            The implemented outputs that may be obtained are: 'table' and 'list'
        plot (bool): A logical value specifying if a set of plots based on the
            definition of the pollen season will be shown in the plot history or
            not. If FALSE graphical results will not be shown. If TRUE a set of
            plots will be shown in the plot history
        exportPlot (bool): A logical value specifying if a set of plots based
            on the definition of the pollen season will be saved in the workspace.
            If TRUE a pdf file for each pollen type showing graphically the
            definition of the pollen season for each studied year will be saved
            within the plot_AeRobiology directory created in the working directory
        exportResult (bool): A logical value specifying if a excel file including
            all parameters for the definition of the pollen season saved in the
            working directoty will be required or not. If FALSE the results will
            not exported. If TRUE the results will be exported as xlsx file
            including all parameters calculated from the definition of the pollen
            season within the table_AeRobiology directory created in the working
            directory

    Inputs:
        TabularDataSet (Simple Dataset)
    Outputs:
        TabularDataSet (Simple Dataset)

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
    out_csv = Path(pcs.storage.local_dir, "CalculatePs.csv")

    try:
        open(out_csv, "a").close()
    except OSError:
        print("Failed creating the file")
    else:
        print("File created")

    # send time to remote storage
    dfs_dir_csv = pcs.storage.put_file(out_csv)

    # send to downstream
    csv_output = SimpleTabularDataset(resource=dfs_dir_csv, delimiter=input_file_delimiter, file_format=".csv")

    pcs.to_downstream(csv_output)

    # prepare output for the time series output
    out_zip = Path(pcs.storage.local_dir, "CalculatePs.zip")

    try:
        open(out_zip, "a").close()
    except OSError:
        print("Failed creating the file")
    else:
        print("File created")

    # send time to remote storage
    dfs_dir_zip = pcs.storage.put_file(out_zip)

    # send to downstream
    zip_output = CompressFile(resource=dfs_dir_zip, file_format=".zip")

    pcs.to_downstream(zip_output)

    return TaskResult(files=[dfs_dir_csv, dfs_dir_zip])
