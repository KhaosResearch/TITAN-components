import shutil
import unittest
import datetime
import openpyxl
from pathlib import Path
from unittest.mock import MagicMock


from drama.storage import LocalStorage

from drama_enbic2lab.catalog.water.DataExtraction import execute
from drama_enbic2lab.catalog.water.tests import RESOURCES
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult


class DataExtractionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_DataExtraction"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset = shutil.copy(Path(RESOURCES, "PrecipitationTimeSeries.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.inputs = {"SimpleTabularDataset": "d"}
        self.pcs.poll_from_upstream = MagicMock(
            return_value=iter(
                [
                    ("d", {"resource": dataset, "delimiter": ";"}),
                ]
            )
        )

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "PrecipitationTimeSeries.csv").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "StatisticalData.csv").is_file())

        # read the output file to assert that the output is valid
        with Path(self.pcs.storage.local_dir, "StatisticalData.csv").open() as fin:
            out_csv = fin.readlines()
            out_csv = out_csv[0:5]
            out_csv = "\n".join(out_csv)

        # assert output file content is valid
        self.assertMultiLineEqual(
            """Hidrologic Year;Station;Year Mean;Year Maximum;Year minimum;Year Collected Data;Year Empty Data;Year Collected Data (Percentage);Year Empty Data (Percentage);Sum of the Year

1970/1971;JABUGO;2.7703296703296703;84.5;0.0;273;92;74.79452054794521;25.205479452054796;756.3

1971/1972;JABUGO;2.7008196721311477;78.3;0.0;366;0;100.0;0.0;988.5

1972/1973;JABUGO;2.7457534246575346;62.2;0.0;365;0;100.0;0.0;1002.2

1973/1974;JABUGO;2.378630136986301;65.2;0.0;365;0;100.0;0.0;868.1999999999999
""",
            out_csv,
        )

        # assert output data is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
