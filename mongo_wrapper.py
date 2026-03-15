from pymongo import MongoClient
from datetime import datetime


class MongoWrapper:

    """
        connect to mongodb and fetch specific data
    """

    def __init__(
        self,
        mongo_uri: str,
        database: str,
        collection: str,
        extra_query: dict = None,
    ):
        self._mongo_uri = mongo_uri
        self._database = database
        self._collection = collection
        self._extra_query = extra_query or {}

    def fetch_data(self, time_field: str, start_time: datetime, end_time: datetime) -> list | None:
        """
            fetch specific data - between the indicated times and return them
        """
        query = {
            time_field: {
                "$gte": start_time,
                "$lte": end_time,
            },
            **self._extra_query,
        }

        client = MongoClient(self._mongo_uri)
        try:
            docs = list(client[self._database][self._collection].find(query))
        finally:
            client.close()

        if not docs:
            print("No documents found for the given date range.")
            return

        return docs