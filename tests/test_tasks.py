import sys
import os
import json
import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import MagicMock
from datetime import datetime

# --------------------------------------------------------------------
# MOCK Airflow before importing Ingestion (for Windows)
# --------------------------------------------------------------------
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.models"] = MagicMock()
from airflow.models import TaskInstance  # safe to import

# --------------------------------------------------------------------
# Import ingestion, transformation, validation classes
# --------------------------------------------------------------------
from dags.tasks import ingest, transform, validate
import dags.tasks.config.settings as settings

# --------------------------------------------------------------------
# Fixture: temporary telemetry + fully compatible config data
# --------------------------------------------------------------------
@pytest.fixture
def tmp_data_dir(tmp_path):
    base_path = tmp_path / "telemetry"
    base_path.mkdir(parents=True)

    # --- telemetry data ---
    telemetry_dir = base_path / "2024-01-01" / "00"
    telemetry_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "device_id": [1, 2, 3, None],
        "timestamp": ["2024-01-01 00:05:00", "2024-01-01 00:10:00", "2024-01-01 00:15:00", "2024-01-01 00:20:00"],
        "reading": [100, 200, 300, 1000]
    })
    pq.write_table(pa.Table.from_pandas(df), telemetry_dir / "telemetry.parquet")

    # --- fully compatible config data ---
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_df = pd.DataFrame({
        "device_id": [1, 2, 3],
        "scale": [1.0, 1.0, 1.0],
        "offset": [0.0, 0.0, 0.0],
        "device_type": ["sensor", "sensor", "sensor"]
    })
    config_df.to_csv(config_dir / "device.csv", index=False)
    config_df.to_parquet(config_dir / "device.parquet")
    (config_dir / "device.json").write_text(json.dumps(config_df.to_dict(orient="records")))
    (config_dir / "ignore.txt").write_text("skip me")

    return base_path, config_dir

# --------------------------------------------------------------------
# Fixture: Ingestion instance with temp DuckDB
# --------------------------------------------------------------------
@pytest.fixture
def ingestion_fixture(tmp_data_dir, tmp_path):
    base_path, _ = tmp_data_dir
    temp_db_path = tmp_path / "pipeline.duckdb"
    settings.DUCK_DB_PATH["duckdb_path"] = str(temp_db_path)
    return ingest.Ingestion(telemetry_base_path=str(base_path))

# --------------------------------------------------------------------
# Fixture: Transformation instance
# --------------------------------------------------------------------
@pytest.fixture
def transformation_fixture(tmp_path):
    temp_db_path = tmp_path / "pipeline.duckdb"
    settings.DUCK_DB_PATH["duckdb_path"] = str(temp_db_path)
    return transform.Transformation()

# --------------------------------------------------------------------
# Fixture: Validation instance
# --------------------------------------------------------------------
@pytest.fixture
def validation_fixture(tmp_path):
    log_file = tmp_path / "dq_issues.csv"
    return validate.Validation(log_file=str(log_file))

# --------------------------------------------------------------------
# End-to-end test: Ingestion → Transformation → Validation
# --------------------------------------------------------------------
def test_pipeline_end_to_end(tmp_data_dir, ingestion_fixture, transformation_fixture, validation_fixture, caplog):
    base_path, config_dir = tmp_data_dir
    ingestion = ingestion_fixture
    transform_instance = transformation_fixture
    validator = validation_fixture

    # --- Step 1: Telemetry ingestion ---
    rows = ingestion.ingest_telemetry(str(base_path), execution_date=pd.Timestamp("2024-01-01"))
    assert rows == 4

    # --- Step 2: Config ingestion ---
    config_rows = ingestion.ingest_config(str(config_dir))
    assert config_rows >= 3  # matches CSV/JSON/Parquet

    # --- Step 3: Metadata logging ---
    metadata = ingestion.log_ingestion_metadata()
    assert metadata["telemetry_rows"] == 4
    assert metadata["config_rows"] >= 3
    assert "ingested_at" in metadata

    # --- Step 4: Transformation ---
    config_df = pd.DataFrame({
        "device_id": [1, 2, 3],
        "scale": [1.0, 1.0, 1.0],
        "offset": [0.0, 0.0, 0.0],
        "device_type": ["sensor", "sensor", "sensor"]
    })

    batches = list(transform_instance.transform_telemetry_snapshot(config_df, batch_size=1))
    df_transformed = pd.concat(batches, ignore_index=True)

    assert "calibrated_value" in df_transformed.columns
    # Compare calibrated_value with original reading for sensor 'reading'
    sensor_vals = df_transformed.loc[df_transformed['sensor_type'] == 'reading', 'calibrated_value'].tolist()
    expected_vals = [100, 100, 100]
    assert sensor_vals == expected_vals

    assert "device_type" in df_transformed.columns
    assert "day_avg" in df_transformed.columns
    assert "hour_avg" in df_transformed.columns
    assert "rolling_7d_avg" in df_transformed.columns

    # --- Step 5: Validation ---
    df_clean = validator.validate_and_clean_data(df_transformed)
    assert df_clean["device_id"].notnull().all()
    assert df_clean["timestamp"].notnull().all()

    lower, upper = settings.ANOMALY_SETTINGS["lower_threshold"], settings.ANOMALY_SETTINGS["upper_threshold"]
    assert df_clean["calibrated_value"].between(lower, upper).all()

    # # Check that issues were logged
    # with open(validator.log_file, "r") as f:
    #     log_text = f.read()
    # assert "NULL_VALUES" in log_text or "OUTLIERS" in log_text

    # --- Step 6: Checkpointing ---
    ti = MagicMock()
    last_run = "2024-01-01T00:00:00"
    ingestion.checkpointing(ti, last_run_time=last_run)
    ti.xcom_push.assert_called_once_with(key="last_run", value=last_run)
    ti.xcom_pull.return_value = last_run
    ts = ingestion.checkpointing(ti)
    assert ts == last_run

# --------------------------------------------------------------------
# Test: IcebergStorage class
# --------------------------------------------------------------------
@pytest.fixture
def iceberg_storage():
    from dags.tasks.iceberg_storage import IcebergStorage
    return IcebergStorage()

def test_store_processed_data(iceberg_storage, caplog):
    # Prepare sample DataFrame
    data = {
        "device_id": ["1", "2"],
        "timestamp": [datetime(2024, 1, 1, 0, 5), datetime(2024, 1, 1, 0, 10)],
        "sensor_type": ["temperature", "humidity"],
        "raw_value": [22.5, 60.0],
        "calibrated_value": [22.5, 60.0],
        "event_date": [datetime(2024, 1, 1).date(), datetime(2024, 1, 1).date()],
        "device_type": ["sensor", "sensor"]
    }
    df = pd.DataFrame(data)

    # Store processed data
    stats = iceberg_storage.store_processed_data(df)

    # Assertions
    assert stats["added_files"] > 0
    assert stats["added_records"] == len(df)
    assert stats["total_data_files"] > 0

    # Check logs
    assert "Upsert completed" in caplog.text
    assert "Compaction successful" in caplog.text