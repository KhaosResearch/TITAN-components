import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.core.model import SimpleTabularDataset
from drama.storage import LocalStorage

from drama_titan.catalog.processor.df.Merger import execute
from drama_titan.catalog.processor.tests import RESOURCES


class MergerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "drama", "test_Merger"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_one_csv = shutil.copy(Path(RESOURCES, "df.part1.csv"), storage.local_dir)
        dataset_two_csv = shutil.copy(Path(RESOURCES, "df.part2.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.inputs = {"DatasetOne": "d1", "DatasetTwo": "d2"}
        self.pcs.poll_from_upstream = MagicMock(
            return_value=iter(
                [
                    ("d1", {"resource": dataset_one_csv, "delimiter": ","}),
                    ("d2", {"resource": dataset_two_csv, "delimiter": ","}),
                ]
            )
        )

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs, join_label="row")

        with Path(self.pcs.storage.local_dir, "merged.csv").open() as fin:
            out_csv = fin.read()

        # assert output file exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "merged.csv").is_file())

        # assert output file content is valid
        self.assertMultiLineEqual(
            "sepal.length,sepal.width,variety,petal.length,petal.width\n6,3,Virginica,5,2\n5,3,Virginica,5,1\n",
            out_csv,
        )

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertIs(type(data["output"]), SimpleTabularDataset)

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
