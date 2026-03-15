from clearml import Dataset


class DatasetCreator:

    def __init__(self, dataset_project: str, dataset_name: str):
        self._dataset_project = dataset_project
        self._dataset_name = dataset_name


    def upload_version(self, tag: str, files_path: str) -> Dataset:

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

    def _get_latest_parent(self) -> str | None:
        try:
            datasets = Dataset.list_datasets(
                dataset_project=self._dataset_project,
                partial_name=self._dataset_name,
                only_completed=True,
            )
            if not datasets:
                return None
            return sorted(datasets, key=lambda d: d.get("created", ""))[-1].get("id")
        except:
            return None