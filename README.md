# MongoDB → ClearML Dataset Pipeline

Pulls documents from MongoDB by date range and uploads them as versioned datasets to ClearML - all document as a file.

## How it works

Each run queries MongoDB for documents within a time window and uploads them as a new dataset version (with parent linkage) to ClearML.

## Configuration

All settings are managed via `.env`:

| Variable | Description |
|---|---|
| `MONGO_URI` | MongoDB connection string |
| `MONGO_DB` | Database name |
| `MONGO_COLLECTION` | Collection name |
| `DATE_FIELD` | The datetime field to filter by |
| `CLEARML_DATASET_PROJECT` | ClearML project name |
| `CLEARML_DATASET_NAME` | ClearML dataset name |
| `SCHEDULE_INTERVAL_HOURS` | How often the scheduler runs (scheduler mode only) |
| `LOOKBACK_HOURS` | How far back each run looks (scheduler mode only) |

## Running

### One-time run
Pulls data between `START_DATE` and `END_DATE` (set in `.env`):
```bash
python main.py
```

### Scheduled (continuous) run
Runs automatically every `SCHEDULE_INTERVAL_HOURS`, pulling the last `LOOKBACK_HOURS` of data each time. Executes once immediately on startup, then on the configured interval:
```bash
python scheduler_main.py
```

## Requirements

```bash
pip install -r requirements.txt
```
