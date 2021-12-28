import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.core.model import SimpleTabularDataset
from drama.storage import LocalStorage

from drama_titan.catalog.processor.df.RemoveOutliers import execute
from drama_titan.catalog.processor.tests import RESOURCES


class RemoveOutliersTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "drama", "test_RemoveOutliers"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_csv = shutil.copy(Path(RESOURCES, "iris.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(return_value={"Dataset": [{"resource": dataset_csv, "delimiter": ","}]})

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs, label="variety", n_partitions=1, outlier_threshold=0.7)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "iris.csv").is_file())

        # assert output file exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "no_outliers.csv").is_file())

        # assert output file content is valid
        with Path(self.pcs.storage.local_dir, "no_outliers.csv").open() as fin:
            out_csv = fin.readlines()

        self.assertMultiLineEqual(
            "5.1,3.5,1.4,0.2,Setosa\n4.9,3.0,1.4,0.2,Setosa\n", "".join(out_csv[1:3]),
        )

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertIs(type(data["output"]), SimpleTabularDataset)

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
