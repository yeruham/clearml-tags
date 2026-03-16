from clearml import Dataset
from datetime import datetime


class DatasetCreator:

    """
        manage clearml dataset - upload data and follows his versions
    """

    def __init__(self, dataset_project: str, dataset_name: str):
        self._dataset_project = dataset_project
        self._dataset_name = dataset_name


    def upload_version(self, files_path: str, tag: str | None = None) -> Dataset | None:
        """
            create new version of dataset, upload files, and finalize it
        """
        try:
            created_at_tag = f"created_at: {datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}"
            parent_id = self._get_latest_parent()
            dataset = Dataset.create(
                dataset_project=self._dataset_project,
                dataset_name=self._dataset_name,
                dataset_tags=[created_at_tag, tag],
                parent_datasets=[parent_id] if parent_id else None,
            )

            dataset.add_files(files_path)
            dataset.upload(show_progress=False)
            dataset.finalize()
            return dataset
        except Exception as e:
            print(f"Could not create dataset and upload data: {e}")
            return

    def _get_latest_parent(self) -> str | None:
        """
            get latest version id of to the dataset if exists
        """
        try:
            dataset_parent = Dataset.get(
                dataset_project=self._dataset_project,
                partial_name=self._dataset_name,
            )
            return dataset_parent.id
        except Exception as e:
            print(f"Could not fetch parent dataset: {e}")
            return