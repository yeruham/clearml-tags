import os
from dotenv import load_dotenv
from dataclasses import dataclass


load_dotenv()

@dataclass
class Config:
    MONGO_URI: str = os.getenv("MONGO_URI")
    MONGO_DB: str = os.getenv("MONGO_DB")
    MONGO_COLLECTION: str = os.getenv("MONGO_COLLECTION")
    DATE_FIELD: str = os.getenv("DATE_FIELD")
    START_DATE: str = os.getenv("START_DATE")
    END_DATE: str = os.getenv("END_DATE")
    CLEARML_DATASET_PROJECT: str = os.getenv("CLEARML_DATASET_PROJECT")
    CLEARML_DATASET_NAME: str = os.getenv("CLEARML_DATASET_NAME")

config = Config()