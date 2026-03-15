from datetime import datetime
from bson import json_util
import tempfile
import json
import logging

from datasets.clases.mongo_wrapper import MongoWrapper
from datasets.clases.dataset_creator import DatasetCreator
from datasets.config.config import config
from data_pipeline import get_data, upload_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    start_time = datetime.strptime(config.START_DATE, "%Y-%m-%d")
    end_time = datetime.strptime(config.END_DATE, "%Y-%m-%d")

    data = get_data(start_time, end_time)
    if not data:
        logger.warning("No documents found for the given window — skipping upload.")
        return

    data_tag = f"data: {start_time.strftime('%Y-%m-%d')} - {end_time.strftime('%Y-%m-%d')}"
    upload_data(data, data_tag)



if __name__ == "__main__":
    main()

