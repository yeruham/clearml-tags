from clearml import Dataset


class DatasetCreator:

    """
        manage clearml dataset - upload data and follows his versions
    """

    def __init__(self, dataset_project: str, dataset_name: str):
        self._dataset_project = dataset_project
        self._dataset_name = dataset_name


    def upload_version(self, tag: str, files_path: str) -> Dataset | None:
        """
            create new version of dataset, upload files, and finalize it
        """
        try:
            parent_id = self._get_latest_parent()
            dataset = Dataset.create(
                dataset_project=self._dataset_project,
                dataset_name=self._dataset_name,
                dataset_tags=[tag],
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
            datasets = Dataset.list_datasets(
                dataset_project=self._dataset_project,
                partial_name=self._dataset_name,
                only_completed=True,
            )
            if not datasets:
                return
            return sorted(datasets, key=lambda d: d.get("created", ""))[-1].get("id")
        except Exception as e:
            print(f"Could not fetch parent dataset: {e}")
            return