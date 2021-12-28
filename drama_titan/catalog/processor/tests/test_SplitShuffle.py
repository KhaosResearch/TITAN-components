import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.core.model import SimpleTabularDataset
from drama.storage import LocalStorage

from drama_titan.catalog.processor.df.SplitShuffle import execute
from drama_titan.catalog.processor.tests import RESOURCES


class SplitShuffleTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "drama", "test_SplitShuffle"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_csv = shutil.copy(Path(RESOURCES, "iris.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(return_value={"Dataset": [{"resource": dataset_csv, "delimiter": ","}]})

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs, proportion=0.5)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "iris.csv").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "train.csv").is_file())
        self.assertTrue(Path(self.pcs.storage.local_dir, "test.csv").is_file())

        # assert output files content is valid
        with open(Path(self.pcs.storage.local_dir, "train.csv")) as f:
            columns = f.readline()
            count = sum(1 for _ in f)

            self.assertEqual(
                columns, "sepal.length,sepal.width,petal.length,petal.width,variety\n",
            )
            self.assertTrue(count == 75)

        with open(Path(self.pcs.storage.local_dir, "test.csv")) as f:
            columns = f.readline()
            count = sum(1 for _ in f)

            self.assertEqual(
                columns, "sepal.length,sepal.width,petal.length,petal.width,variety\n",
            )
            self.assertTrue(count == 75)

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertTrue(len(data["output"]) == 2)
        self.assertTrue(isinstance(data["output"][0], SimpleTabularDataset))
        self.assertTrue(isinstance(data["output"][1], SimpleTabularDataset))

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
