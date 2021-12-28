import shutil
import unittest
import datetime
import openpyxl
from pathlib import Path
from unittest.mock import MagicMock


from drama.storage import LocalStorage
from drama.models.task import TaskResult

from drama_enbic2lab.catalog.water.TemperatureMatrixTransformation import execute
from drama_enbic2lab.catalog.water.TemperatureMatrixTransformation import (
    SimpleTabularDatasetMax,
    SimpleTabularDatasetMin,
)
from drama_enbic2lab.catalog.water.tests import RESOURCES
from drama.core.model import SimpleTabularDataset


class TemperatureMatrixTransformationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_TemperatureMatrixTransformation"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset = shutil.copy(Path(RESOURCES, "AEMETTemperaturaTest.xlsx"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(return_value={"ExcelDataset": [{"resource": dataset}]})

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "AEMETTemperaturaTest.xlsx").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "MinTempTimeSeries.csv").is_file())

        self.assertTrue(Path(self.pcs.storage.local_dir, "MaxTempTimeSeries.csv").is_file())

        # read the output files to assert that the output is valid
        with Path(self.pcs.storage.local_dir, "MinTempTimeSeries.csv").open() as fin:
            min_out_csv = fin.readlines()
            min_out_csv = min_out_csv[367:372]
            min_out_csv = "\n".join(min_out_csv)

        with Path(self.pcs.storage.local_dir, "MaxTempTimeSeries.csv").open() as fin:
            max_out_csv = fin.readlines()
            max_out_csv = max_out_csv[367:372]
            max_out_csv = "\n".join(max_out_csv)

        # assert output file content is valid
        self.assertMultiLineEqual(
            """1917-10-02;11.0;13.0;14.0

1917-10-03;8.0;11.0;10.0

1917-10-04;7.0;10.0;10.0

1917-10-05;7.0;10.0;10.0

1917-10-06;6.0;9.0;9.0
""",
            min_out_csv,
        )

        # assert output file content is valid
        self.assertMultiLineEqual(
            """1917-10-02;21.0;21.0;21.0

1917-10-03;19.0;23.0;22.0

1917-10-04;17.0;24.0;23.0

1917-10-05;20.0;25.0;23.0

1917-10-06;20.0;24.0;24.0
""",
            max_out_csv,
        )

        # assert output data is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
