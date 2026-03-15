from datetime import datetime
from bson import json_util
import tempfile
import json

from mongo_wrapper import MongoWrapper
from dataset_creator import DatasetCreator
from config import config


def upload_data(dataset_creator: DatasetCreator, data: list, data_tag: str):
    """
        save the data temporary as files, upload the files to clearml and remove them
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        for doc in data:
            file_path = f"{tmp_dir}/{doc['_id']}.json"
            with open(file_path, "w", encoding="utf8") as f:
                json.dump(doc, f, ensure_ascii=False,  default=json_util.default)

        dataset_creator.upload_version(data_tag, tmp_dir)


def main():
    dataset_creator = DatasetCreator(
                                    config.CLEARML_DATASET_PROJECT,
                                    config.CLEARML_DATASET_NAME
                                    )
    mongo_wrapper = MongoWrapper(
                                 config.MONGO_URI,
                                 config.MONGO_DB,
                                 config.MONGO_COLLECTION
                                 )

    start_time = datetime.strptime(config.START_DATE, "%Y-%m-%d")
    end_time = datetime.strptime(config.END_DATE, "%Y-%m-%d")

    data = mongo_wrapper.fetch_data(config.DATE_FIELD, start_time, end_time)
    if not data:
        return

    data_tag = f"{config.START_DATE} - {config.END_DATE}"
    upload_data(dataset_creator, data, data_tag)




if __name__ == "__main__":
    main()

