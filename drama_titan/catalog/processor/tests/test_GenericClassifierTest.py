import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.core.model import SimpleTabularDataset
from drama.storage import LocalStorage

from drama_titan.catalog.processor.ml.GenericClassifierTest import execute
from drama_titan.catalog.processor.tests import RESOURCES


class ClassifierTestTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "drama", "test_GenericClassifierTest"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_csv = shutil.copy(Path(RESOURCES, "iris.csv"), storage.local_dir)
        model_bin = shutil.copy(Path(RESOURCES, "dtc_model.bin.pickle"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.inputs = {"Dataset": "d", "MlModel": "mm"}
        self.pcs.poll_from_upstream = MagicMock(
            return_value=iter(
                [
                    ("Dataset", {"resource": dataset_csv, "delimiter": ","}),
                    ("Model", {"resource": model_bin, "label": "variety"}),
                ]
            )
        )

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "iris.csv").is_file())
        self.assertTrue(Path(self.pcs.storage.local_dir, "dtc_model.bin.pickle").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "ClassifierTestConfusionMatrix.csv").is_file())
        self.assertTrue(Path(self.pcs.storage.local_dir, "ClassifierTestScore.csv").is_file())
        self.assertTrue(Path(self.pcs.storage.local_dir, "ClassifierTestReport.csv").is_file())

        # assert output files content are valid
        with open(Path(self.pcs.storage.local_dir, "ClassifierTestConfusionMatrix.csv")) as f:
            count = sum(1 for _ in f)
            self.assertTrue(count == 4)

        with open(Path(self.pcs.storage.local_dir, "ClassifierTestScore.csv")) as f:
            count = sum(1 for _ in f)
            self.assertTrue(count == 2)

        with open(Path(self.pcs.storage.local_dir, "ClassifierTestReport.csv")) as f:
            count = sum(1 for _ in f)
            self.assertTrue(count == 7)

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertTrue(len(data["output"]) == 3)

        self.assertTrue(isinstance(data["output"][0], SimpleTabularDataset))
        self.assertTrue(isinstance(data["output"][1], SimpleTabularDataset))
        self.assertTrue(isinstance(data["output"][2], SimpleTabularDataset))

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
