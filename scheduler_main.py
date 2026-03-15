import json
import logging
import tempfile
from bson import json_util
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.blocking import BlockingScheduler

from config import config
from dataset_creator import DatasetCreator
from mongo_wrapper import MongoWrapper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline():
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=config.LOOKBACK_HOURS)
    tag = f"{start_time.strftime('%Y-%m-%dT%H:%M')} - {end_time.strftime('%Y-%m-%dT%H:%M')}"

    logger.info(f"Starting pipeline run | window: {tag}")

    mongo_wrapper = MongoWrapper(
        config.MONGO_URI,
        config.MONGO_DB,
        config.MONGO_COLLECTION,
    )
    dataset_creator = DatasetCreator(
        config.CLEARML_DATASET_PROJECT,
        config.CLEARML_DATASET_NAME,
    )

    data = mongo_wrapper.fetch_data(config.DATE_FIELD, start_time, end_time)
    if not data:
        logger.warning("No documents found for the given window — skipping upload.")
        return

    logger.info(f"Fetched {len(data)} documents from MongoDB.")

    with tempfile.TemporaryDirectory() as tmp_dir:
        for doc in data:
            file_path = f"{tmp_dir}/{doc['_id']}.json"
            with open(file_path, "w", encoding="utf8") as f:
                json.dump(doc, f, ensure_ascii=False, default=json_util.default)

        dataset = dataset_creator.upload_version(tag, tmp_dir)
        logger.info(f"Dataset uploaded successfully | id: {dataset.id} | tag: {tag}")


def main():
    logger.info(
        f"Scheduler starting | interval: every {config.SCHEDULE_INTERVAL_HOURS}h "
        f"| lookback: {config.LOOKBACK_HOURS}h"
    )

    # Run immediately on startup, then on schedule
    run_pipeline()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_pipeline,
        trigger="interval",
        hours=config.SCHEDULE_INTERVAL_HOURS,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()