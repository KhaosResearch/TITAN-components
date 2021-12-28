import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.core.model import SimpleTabularDataset
from drama.storage import LocalStorage

from drama_titan.catalog.processor.ml.GenericSlidingWindowClassifierPredict import execute
from drama_titan.catalog.processor.tests import RESOURCES


class GenericClassifierPredictTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "drama", "test_GenericSlidingWindowClassifierPredict"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_csv = shutil.copy(Path(RESOURCES, "iris.csv"), storage.local_dir)
        model_bin = shutil.copy(Path(RESOURCES, "lstm_model.h5"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.inputs = {"Dataset": "d", "Model": "mdtc"}
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
        data = execute(pcs=self.pcs, time_window=10, stride=5)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "iris.csv").is_file())
        self.assertTrue(Path(self.pcs.storage.local_dir, "lstm_model.h5").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "predict.csv").is_file())

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertIs(type(data["output"]), SimpleTabularDataset)

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
