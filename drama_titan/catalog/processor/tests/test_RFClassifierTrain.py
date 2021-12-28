import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.storage import LocalStorage

from drama_titan.catalog.processor.ml.RFClassifierTrain import execute
from drama_titan.catalog.processor.tests import RESOURCES
from drama_titan.model import MlModelRF


class RFClassifierTrainTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "drama", "test_RFClassifierTrain"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_csv = shutil.copy(Path(RESOURCES, "iris.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(return_value={"Dataset": [{"resource": dataset_csv, "delimiter": ","}]})

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs, label="variety", n_estimators=100)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "iris.csv").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "rf_model.bin.pickle").is_file())

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertIs(type(data["output"]), MlModelRF)

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
