import os
import json
import logging
import pandas as pd
from datetime import datetime
from airflow.models import TaskInstance
import duckdb
import glob
from dags.tasks.config.settings import DAY_HOUR 
from dags.tasks.config.settings import DUCK_DB_PATH, TELEMETRY_DAG_CONFIG

class Ingestion:
    def __init__(self, telemetry_base_path: str = None):
        """
        DuckDB connection for intermediate storage.
        Creates telemetry_snapshot and config_snapshot tables.
        """
        # -------------------------
        # Logger setup
        # -------------------------
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
            ))
            self.logger.addHandler(ch)

        # -------------------------
        # DuckDB connection
        # -------------------------
        self.duckdb_path = DUCK_DB_PATH["duckdb_path"]
        self.con = duckdb.connect(self.duckdb_path) if self.duckdb_path else duckdb.connect()
        self.logger.info(f"DuckDB initialized at {self.duckdb_path or ':memory:'}")

        # -------------------------
        # Telemetry snapshot table
        # -------------------------
        parquet_path = telemetry_base_path or TELEMETRY_DAG_CONFIG["parquet_path"]
        first_files = glob.glob(os.path.join(parquet_path, "*", "*", "*.parquet"))

        if first_files:
            first_file = first_files[0]
            self.logger.info(f"Creating telemetry_snapshot table dynamically from: {first_file}")
            self.con.execute(f"""
                CREATE TABLE IF NOT EXISTS telemetry_snapshot AS
                SELECT * FROM read_parquet('{first_file}') LIMIT 0
            """)
        else:
            self.logger.warning(f"No parquet files found to initialize telemetry_snapshot at {parquet_path}")
            # Fallback: create table with minimal schema
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_snapshot (
                    device_id INTEGER,
                    timestamp TIMESTAMP,
                    reading DOUBLE
                )
            """)

        # -------------------------
        # Config snapshot table
        # -------------------------
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS config_snapshot (
                device_id INTEGER,
                scale DOUBLE,
                "offset" DOUBLE,
                device_type VARCHAR
            )
        """)

    # -------------------------
    # Telemetry ingestion with batching
    # -------------------------
    def ingest_telemetry(self, base_path: str, execution_date=None, checkpoint_ts: str = None) -> int:
        """
        Ingest telemetry parquet files using DuckDB in **batches**.
        Returns total number of rows ingested.
        """
        if execution_date is None:
            raise ValueError("execution_date must be provided for partitioned ingestion.")

        date_str = execution_date.strftime("%Y-%m-%d")
        self.logger.info(f"Ingesting telemetry for date partition: {date_str}")

        total_rows = 0

        for hour in range(DAY_HOUR):
            hour_str = f"{hour:02d}"
            partition_path = os.path.join(base_path, date_str, hour_str, "*.parquet")
            files = glob.glob(partition_path)
            if not files:
                self.logger.warning(f"No parquet files found at {partition_path}")
                continue

            for file in files:
                try:
                    query = f"SELECT * FROM read_parquet('{file}')"
                    if checkpoint_ts:
                        query += f" WHERE timestamp > '{checkpoint_ts}'"


                    # Get RecordBatchReader
                    reader = self.con.execute(query).arrow()

                    # Convert all batches to pandas
                    for batch in reader:  # RecordBatchReader is iterable
                        df_batch = batch.to_pandas()
                        if not df_batch.empty:
                            self.con.register("temp_batch", df_batch)
                            self.con.execute("INSERT INTO telemetry_snapshot SELECT * FROM temp_batch")
                            total_rows += len(df_batch)
                    self.logger.info(f"Ingested {len(df_batch)} rows from {file}")
                except Exception as e:
                    self.logger.error(f"Failed to read parquet file {file}: {e}")

        self.logger.info(f"Total telemetry rows ingested for {date_str}: {total_rows}")
        return total_rows

    # -------------------------
    # Config ingestion
    # -------------------------
    def ingest_config(self, config_dir: str) -> int:
        """
        Ingest device configs from JSON/CSV/Parquet files.
        Returns number of rows ingested.
        """
        if not os.path.exists(config_dir):
            raise FileNotFoundError(f"Config directory not found: {config_dir}")

        total_rows = 0

        for root, _, files in os.walk(config_dir):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    if file.endswith(".json"):
                        with open(file_path, "r") as f:
                            data = json.load(f)
                        df = pd.DataFrame([data]) if isinstance(data, dict) else pd.DataFrame(data)
                    elif file.endswith(".csv"):
                        df = pd.read_csv(file_path)
                    elif file.endswith(".parquet"):
                        df = pd.read_parquet(file_path)
                    else:
                        self.logger.warning(f"Skipping unsupported file type: {file_path}")
                        continue

                    if not df.empty:
                        self.con.register("temp_config", df)
                        self.con.execute("INSERT INTO config_snapshot SELECT * FROM temp_config")
                        total_rows += len(df)

                except Exception as e:
                    self.logger.error(f"Failed to ingest config file {file_path}: {e}")

        self.logger.info(f"Ingested {total_rows} config rows from {config_dir}")
        return total_rows

    # -------------------------
    # Log ingestion metadata
    # -------------------------
    def log_ingestion_metadata(self):
        """
        Log metadata from DuckDB snapshots.
        """
        telemetry_count = self.con.execute("SELECT COUNT(*) FROM telemetry_snapshot").fetchone()[0]
        config_count = self.con.execute("SELECT COUNT(*) FROM config_snapshot").fetchone()[0]

        metadata = {
            "telemetry_rows": telemetry_count,
            "config_rows": config_count,
            "ingested_at": datetime.now().isoformat()
        }

        self.logger.info(f"Ingestion metadata: {metadata}")
        return metadata

    # -------------------------
    # Checkpointing via XCom
    # -------------------------
    def checkpointing(self, ti: TaskInstance, last_run_time: str = None) -> str:
        if last_run_time:
            ti.xcom_push(key="last_run", value=last_run_time)
            self.logger.info(f"Checkpoint saved to XCom: {last_run_time}")
            return last_run_time
        else:
            ts = ti.xcom_pull(key="last_run")
            if ts:
                self.logger.info(f"Checkpoint loaded from XCom: {ts}")
                return ts
            return None