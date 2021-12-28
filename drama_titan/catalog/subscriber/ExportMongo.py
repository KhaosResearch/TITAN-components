from drama.process import Process
from pymongo import MongoClient


def _db_connection(uri):
    return MongoClient(uri)


def execute(pcs: Process, connection_uri: str, database: str, collection: str):
    """
    Exports data into a mongoDB instance given its credentials.

    Args:
        pcs (Process)

    Parameters:
        connection_uri (str): URI to connect to a MongoDB instance.
        database (str): Database to connect on the MongoDB instance.
        collection (str): Collection to store the data in.

    Inputs:
        Dataclass (BaseModel): Input dataclass

    Outputs:
        None

    Produces:
        None

    Author:
        Khaos Research
    """
    # read inputs
    inputs = pcs.get_from_upstream()
    json_doc = inputs["Dataclass"]

    pcs.info("Connecting with the MongoDB instance")

    with _db_connection(connection_uri) as client:
        mongo_collection = client[database][collection]
        mongo_collection.insert_one(json_doc)
