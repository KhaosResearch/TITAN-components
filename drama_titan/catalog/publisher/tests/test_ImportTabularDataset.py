import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.storage import LocalStorage

from drama_titan.catalog.publisher.load.ImportTabularDataset import execute
from drama_titan.catalog.publisher.tests import RESOURCES
from drama_titan.model import TabularDataset


class ImportTSVTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_ImportTabularDataset"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        _ = shutil.copy(Path(RESOURCES, "countries.csv"), storage.local_dir)

        self.pcs = MagicMock(storage=storage)

    def test_integration(self):
        # execute func
        url = str(Path(self.pcs.storage.local_dir, "countries.csv"))
        data = execute(pcs=self.pcs, url=url)

        # assert output file exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "out.csv").is_file())

        # assert output file content is valid
        with open(Path(self.pcs.storage.local_dir, "out.csv"), "r") as f:
            columns = f.readline()
            self.assertEqual(
                columns, "Country,Region\n",
            )

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertIs(type(data["output"]), TabularDataset)

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
