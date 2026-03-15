from datetime import datetime, timedelta, timezone
from bson import json_util
import tempfile
import json
import logging

from datasets.clases.dataset_creator import DatasetCreator
from clases.mongo_wrapper import MongoWrapper
from datasets.config.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)



def upload_data(data: list, data_tag: str):
    """
        save the data temporary as files, upload the files to clearml and remove them
    """
    dataset_creator = DatasetCreator(
        config.CLEARML_DATASET_PROJECT,
        config.CLEARML_DATASET_NAME,
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        for doc in data:
            file_path = f"{tmp_dir}/{doc['_id']}.json"
            with open(file_path, "w", encoding="utf8") as f:
                json.dump(doc, f, ensure_ascii=False,  default=json_util.default)

        dataset = dataset_creator.upload_version(data_tag, tmp_dir)
        logger.info(f"Dataset uploaded successfully | id: {dataset.id} | tag: {data_tag}")


def get_data(start_time, end_time):
    """
        get specific data from mongodb
    """
    tag = f"{start_time.strftime('%Y-%m-%d')} - {end_time.strftime('%Y-%m-%d')}"
    logger.info(f"Starting pipeline run | window: {tag}")

    mongo_wrapper = MongoWrapper(
        config.MONGO_URI,
        config.MONGO_DB,
        config.MONGO_COLLECTION,
    )

    data = mongo_wrapper.fetch_data(config.DATE_FIELD, start_time, end_time)
    logger.info(f"Fetched {len(data)} documents from MongoDB.")
    return data