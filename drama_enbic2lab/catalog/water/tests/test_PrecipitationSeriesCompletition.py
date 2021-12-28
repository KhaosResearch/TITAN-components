import shutil
import unittest
import datetime
import openpyxl
from pathlib import Path
from unittest.mock import MagicMock


from drama.storage import LocalStorage

from drama_enbic2lab.catalog.water.PrecipitationSeriesCompletition import execute
from drama_enbic2lab.catalog.water.tests import RESOURCES
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult


class PrecipitationSeriesCompletitionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_PrecipitationSeriesCompletition"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset = shutil.copy(Path(RESOURCES, "PrecipitationTimeSeries.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(
            return_value={"SimpleTabularDataset": [{"resource": dataset, "delimiter": ";"}]}
        )

    def test_integration(self):
        # execute func
        target = "GALAROZA"
        analysis = [
            "JABUGO",
            "CORTEGANA",
            "ARACENA",
            "ALAJAR",
        ]

        data = execute(
            pcs=self.pcs,
            start_date="1974-10-01",
            end_date="2018-09-30",
            target_station=target,
            analysis_stations=analysis,
            priorize="r2",
        )

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "PrecipitationTimeSeries.csv").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "StationsAnalysis.csv").is_file())

        self.assertTrue(Path(self.pcs.storage.local_dir, "GALAROZA_completed.csv").is_file())

        self.assertTrue(Path(self.pcs.storage.local_dir, "HomogeneityTests.csv").is_file())

        # read the output files to assert that the output is valid
        # Statistical Analysis output
        with Path(self.pcs.storage.local_dir, "StationsAnalysis.csv").open() as fin:
            analysis_csv = fin.read()

        # Reading a part of the series completition
        with Path(self.pcs.storage.local_dir, "GALAROZA_completed.csv").open() as fin:
            stations_csv = fin.readlines()
            stations_csv = stations_csv[0:5]
            stations_csv = "\n".join(stations_csv)

        # Reading the test expect a row of changing values
        with Path(self.pcs.storage.local_dir, "HomogeneityTests.csv").open() as fin:
            homogeneity_csv = fin.readlines()
            homogeneity_csv.pop(3)
            homogeneity_csv = "\n".join(homogeneity_csv)

        # assert output file content is valid
        self.assertMultiLineEqual(
            """;CORTEGANA;JABUGO;ALAJAR;ARACENA
R2;0.7916787787077669;0.7583943241421834;0.7500962935385755;0.6406512984427926
Slope;0.8280246244031414;0.8331775957055094;0.7571185122212427;0.8122808767180277
Intercept;0.27731560129936517;0.3904445474328986;0.28355964264510725;0.5831304304302005
Pair of data;14484;14174;14662;8888
""",
            analysis_csv,
        )

        # assert output file content is valid
        self.assertMultiLineEqual(
            """DATE;GALAROZA

1974-10-01;0.0

1974-10-02;0.0

1974-10-03;0.0

1974-10-04;0.0
""",
            stations_csv,
        )

        self.assertMultiLineEqual(
            """;Pettit Test;SNHT Test;Buishand Test

Homogeneity;True;True;True

Change Point Location;1995-10-18;1979-04-13;1979-04-13

Maximum test Statistics;2400577.0;13.175310545767186;1.8241159893698085

Average between change point;mean(mu1=2.523104968782518, mu2=2.7167806274603366);mean(mu1=3.383756038647343, mu2=2.5368645855012137);mean(mu1=3.383756038647343, mu2=2.5368645855012137)
""",
            homogeneity_csv,
        )

        # assert output data is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
