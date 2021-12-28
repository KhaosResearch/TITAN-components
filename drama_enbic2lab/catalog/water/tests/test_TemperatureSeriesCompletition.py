import shutil
import unittest
import datetime
import openpyxl
from pathlib import Path
from unittest.mock import MagicMock


from drama.storage import LocalStorage
from drama.models.task import TaskResult

from drama_enbic2lab.catalog.water.TemperatureSeriesCompletition import execute
from drama_enbic2lab.catalog.water.tests import RESOURCES
from drama.core.model import SimpleTabularDataset


class TemperatureSeriesCompletitionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_TemperatureSeriesCompletition"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_max = shutil.copy(Path(RESOURCES, "MaxTempTimeSeries.csv"), storage.local_dir)

        dataset_min = shutil.copy(Path(RESOURCES, "MinTempTimeSeries.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(
            return_value={
                "SimpleTabularDatasetMax": [{"resource": dataset_max, "delimiter": ";"}],
                "SimpleTabularDatasetMin": [{"resource": dataset_min, "delimiter": ";"}],
            }
        )

    def test_integration(self):
        # execute func
        target = "QUESADA (FUENTE DEL PINO)"
        analysis = ["POZO ALCON (PRADOS DE CUENCA)", "POZO ALCON (EL HORNICO)"]

        data = execute(
            pcs=self.pcs,
            start_date="1918-10-01",
            end_date="1921-09-30",
            target_station=target,
            analysis_stations=analysis,
            priorize="r2",
        )

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "MaxTempTimeSeries.csv").is_file())
        self.assertTrue(Path(self.pcs.storage.local_dir, "MinTempTimeSeries.csv").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "StationsAnalysis.csv").is_file())

        self.assertTrue(Path(self.pcs.storage.local_dir, "QUESADA (FUENTE DEL PINO)_completed.csv").is_file())

        self.assertTrue(Path(self.pcs.storage.local_dir, "HomogeneityTests.csv").is_file())

        # read the output files to assert that the output is valid
        # Statistical Analysis output
        with Path(self.pcs.storage.local_dir, "StationsAnalysis.csv").open() as fin:
            analysis_csv = fin.read()

        # Reading a part of the series completition
        with Path(self.pcs.storage.local_dir, "QUESADA (FUENTE DEL PINO)_completed.csv").open() as fin:
            stations_csv = fin.readlines()
            stations_csv = stations_csv[0:5]
            stations_csv = "\n".join(stations_csv)

        # Reading the test expect a row of changing values
        with Path(self.pcs.storage.local_dir, "HomogeneityTests.csv").open() as fin:
            homogeneity_csv = fin.readlines()
            homogeneity_csv.pop(3)
            homogeneity_csv = "\n".join(homogeneity_csv)

        self.assertMultiLineEqual(
            """;POZO ALCON (PRADOS DE CUENCA);POZO ALCON (EL HORNICO)
R2;0.7230266865366834;0.6345301549269828
Slope;0.9136721656290226;0.8859968377006191
Intercept;2.1374858964977284;0.6814643731407326
Pair of data;1012;1065
""",
            analysis_csv,
        )

        self.assertMultiLineEqual(
            """DATE;QUESADA (FUENTE DEL PINO)(MAX);QUESADA (FUENTE DEL PINO)(MIN)

1918-10-01;27.0;10.0

1918-10-02;24.0;7.0

1918-10-03;8.0;5.0

1918-10-04;12.0;8.0
""",
            stations_csv,
        )

        self.assertMultiLineEqual(
            """;Pettit Test(MAX);SNHT Test(MAX);Buishand Test(MAX);Pettit Test(MIN);SNHT Test(MIN);Buishand Test(MIN)

Homogeneity;True;True;True;True;True;True

Change Point Location;1919-04-12;1921-07-02;1919-04-17;1919-05-11;1919-05-02;1919-05-11

Maximum test Statistics;74749.0;91.58466594513553;6.386205210540054;113529.0;160.29725800228667;5.416684790840124

Average between change point;mean(mu1=12.185567010309278, mu2=19.245011086474502);mean(mu1=17.18389662027833, mu2=27.066666666666666);mean(mu1=12.296482412060302, mu2=19.25975473801561);mean(mu1=1.6905829596412556, mu2=7.373424971363115);mean(mu1=1.560747663551402, mu2=7.346938775510204);mean(mu1=1.6905829596412556, mu2=7.373424971363115)
""",
            homogeneity_csv,
        )

        # assert output data is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
