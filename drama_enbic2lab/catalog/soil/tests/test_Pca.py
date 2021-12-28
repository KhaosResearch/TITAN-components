import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.storage import LocalStorage

from drama_enbic2lab.catalog.soil.Pca import execute
from drama_enbic2lab.catalog.soil.tests import RESOURCES
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult


class PcaTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_Pca"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset = shutil.copy(Path(RESOURCES, "DataNormalized.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(
            return_value={"SimpleTabularDataset": [{"resource": dataset, "delimiter": ";"}]}
        )

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "DataNormalized.csv").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "PCA.csv").is_file())

        # read the output file to assert that the output is valid
        with Path(self.pcs.storage.local_dir, "PCA.csv").open() as fin:
            out_csv = fin.readlines()
            out_csv = out_csv[0:4]
            out_csv = "\n".join(out_csv)

        # assert output file content is valid
        self.assertMultiLineEqual(
            """PC1;PC2;PC3;PC4

0.8275874531031756;-0.3208473811817771;0.14120288661166344;0.21135191909473516

0.9743668662635736;0.6038976644488726;0.29000556463538607;0.452082426623872

1.5312627029492234;-0.0609639594899686;-0.07958075678917984;0.05107998772088834
""",
            out_csv,
        )

        # assert output data is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
