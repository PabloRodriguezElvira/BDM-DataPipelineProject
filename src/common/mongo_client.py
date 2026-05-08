from pymongo import MongoClient
import src.common.global_variables as config
import logging

logger = logging.getLogger(__name__)

def get_mongo_client() -> MongoClient:
    mongo_uri = f"mongodb://{config.MONGO_USER}:{config.MONGO_PASSWORD}@{config.MONGO_HOST}:{config.MONGO_PORT}/"
    try:
        client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        print(f"[MONGO] Succesful connection to {config.MONGO_HOST}:{config.MONGO_PORT}")
        return client
        
    except Exception as e:
        logger.error(f"[ERROR] Not possible to connect {e}")
        raise e
