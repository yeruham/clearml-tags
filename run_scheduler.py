import json
import logging
import tempfile
from bson import json_util
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.blocking import BlockingScheduler

from datasets.config.config import config
from datasets.clases.dataset_creator import DatasetCreator
from datasets.clases.mongo_wrapper import MongoWrapper
from data_pipeline import get_data, upload_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

def run_pipeline():
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=config.LOOKBACK_HOURS)
    tag = f"{start_time.strftime('%Y-%m-%d')} - {end_time.strftime('%Y-%m-%d')}"

    data = get_data(start_time, end_time)
    if not data:
        logger.warning("No documents found for the given window — skipping upload.")
        return

    upload_data(data, tag)

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