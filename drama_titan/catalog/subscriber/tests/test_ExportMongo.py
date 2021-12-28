import unittest
from unittest import mock
from unittest.mock import MagicMock

from drama_titan.catalog.subscriber.ExportMongo import execute


class ExportMongoDBTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # mock process
        self.pcs = MagicMock()
        self.pcs.get_from_upstream = MagicMock(
            return_value={"Dataclass": [{"resource": "model_bin", "label": "variety"}]}
        )

    @mock.patch("drama_titan.catalog.subscriber.ExportMongo._db_connection")
    def test_integration(self, _db_connection):
        # execute func
        execute(
            pcs=self.pcs, connection_uri="mongodb+srv://test:test@test.test/test", database="test", collection="test"
        )

        # assert connection established
        _db_connection.assert_called_once_with("mongodb+srv://test:test@test.test/test")

    def tearDown(self) -> None:
        self.pcs.storage.destroy()


if __name__ == "__main__":
    unittest.main()
