import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.storage import LocalStorage

from drama_enbic2lab.catalog.soil.DataNormalization import execute
from drama_enbic2lab.catalog.soil.tests import RESOURCES
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult


class DataNormalizationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_DataNormalization"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset = shutil.copy(Path(RESOURCES, "Data.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(
            return_value={"SimpleTabularDataset": [{"resource": dataset, "delimiter": ";"}]}
        )

    def test_integration(self):
        # execute func
        data = execute(pcs=self.pcs)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "Data.csv").is_file())

        # assert output files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "DataNormalized.csv").is_file())

        # read the output file to assert that the output is valid
        with Path(self.pcs.storage.local_dir, "DataNormalized.csv").open() as fin:
            out_csv = fin.readlines()
            out_csv = out_csv[0:4]
            out_csv = "\n".join(out_csv)

        # assert output file content is valid
        self.assertMultiLineEqual(
            """pmm;DensidadAp;Arenasmuyfinas;Textarenas;Textlimos;Textarci;ConductividadmSg;C.O.5cm;M.O;C.O.tha1;E.E;K.usle;CIC;Ksat;nespecies;cobertura;v1;v2;HumGen

2.0118235236690243;0.39138407455091107;-1.0470246316686154;-0.37551831269288166;0.8891334268264451;-0.05511236739741996;1.2338318717498977;0.30750429910142457;0.43688924382341154;0.6846322653384062;-0.2124474892378325;-0.36253510745995265;0.22044210701033307;0.4759645565648454;1.149862488523563;1.7570019557550207;0.003583152880424975;-0.17292585195058843;0.5144332706523439

2.0118235236690243;0.10326717643267773;-1.0470246316686154;-0.06809550027185787;0.07334736830271736;0.6548035698028696;0.660770390845005;0.3451578596743032;0.48138101102735925;0.632385633770334;-0.5629529560574339;-0.7667297738124326;0.22044210701033307;1.6231038604342343;1.149862488523563;1.7570019557550207;0.33545349997287316;0.3235398040112769;1.056706188151153

2.0118235236690243;0.33559265208292927;-1.1925299358047003;-0.749410922394127;1.217767621018438;0.29984560120272485;2.398440687782421;0.8824013195031857;1.1161924493848554;1.4558013560541159;1.3472762014744548;-0.8285308454265414;0.6671749004310659;0.42838223403481374;1.149862488523563;1.7570019557550207;1.3679390242604892;0.3235398040112769;2.234554863365477
""",
            out_csv,
        )

        # assert output data is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
