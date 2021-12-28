import shutil
import unittest

from pathlib import Path
from unittest.mock import MagicMock


from drama.storage import LocalStorage
from drama_enbic2lab.catalog.water.PrecipitationMatrixTransformation import execute
from drama_enbic2lab.catalog.water.tests import RESOURCES
from drama_enbic2lab.model import ExcelDataset
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult


class PrecipitationMatrixTransformationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_PrecipitationMatrixTransformation"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset = shutil.copy(Path(RESOURCES, "AEMETPrecipitationTest.xlsx"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(return_value={"ExcelDataset": [{"resource": dataset}]})

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "AEMETPrecipitationTest.xlsx").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "PrecipitationTimeSeries.csv").is_file())

        # read the output file to assert that the output is valid
        with Path(self.pcs.storage.local_dir, "PrecipitationTimeSeries.csv").open() as fin:
            out_csv = fin.readlines()
            out_csv = out_csv[94:101]
            out_csv = "\n".join(out_csv)

        # assert output file content is valid
        self.assertMultiLineEqual(
            """1971-01-02;0.0;0.0;10.1;4.0;14.0

1971-01-03;12.9;10.0;3.1;7.0;4.6

1971-01-04;0.0;3.0;0.0;8.0;12.6

1971-01-05;0.0;4.5;0.0;0.0;2.4

1971-01-06;0.0;0.0;0.0;0.0;0.0

1971-01-07;0.0;0.0;0.0;0.0;0.0

1971-01-08;0.0;0.0;0.0;0.0;0.0
""",
            out_csv,
        )

        # assert output data is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
