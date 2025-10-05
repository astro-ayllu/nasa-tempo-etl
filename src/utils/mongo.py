import src.config as config
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from src.utils.logger import logger

MONGO_URI = config.MONGO_URI


def save_processing(time: str, process_key: str = ""):
    client = MongoClient(MONGO_URI, server_api=ServerApi("1"))

    try:
        client.admin.command("ping")
        logger.info("Connected to MongoDB!")

        db = client["nasa-tempo-challenge"]
        collection = db["processing"]

        record = {
            "time": time,
            "process_key": process_key
        }

        result = collection.insert_one(record)

        logger.info("Record inserted!")

    except Exception as e:
        logger.info(e)
