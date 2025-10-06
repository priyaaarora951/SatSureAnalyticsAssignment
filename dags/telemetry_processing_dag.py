from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import get_current_context
import datetime

from tasks.ingest import Ingestion
from tasks.transform import Transformation
from tasks.validate import Validation
from tasks.load import IcebergStorage
#from tasks.monitor import Reports

from dags.tasks.config.settings import (
    TELEMETRY_DAG_CONFIG,
    DEVICE_DAG_CONFIG,
    default_args,
    max_active_runs,
    catchup,
)

# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id="core_telemetry_pipeline",
    default_args=default_args,
    description="Telemetry ETL pipeline with DuckDB snapshot, validation, anomaly detection, Iceberg storage, and monitoring",
    schedule_interval=TELEMETRY_DAG_CONFIG["schedule_interval"],  # hourly
    catchup=catchup,
    max_active_runs=max_active_runs,
    tags=["Telemetry"],
) as dag:

    # -----------------------
    # Ingest Telemetry & Config
    # -----------------------
    @task(retries=3, retry_delay=timedelta(minutes=5))
    def ingest_telemetry():
        ingestion = Ingestion()
        
        ctx = get_current_context()
        ti = ctx["ti"]
        execution_date = ctx["execution_date"]

        # Load last checkpoint
        last_run_time = ingestion.checkpointing(ti=0)

        # Ingest telemetry parquet files into DuckDB snapshot
        ingestion.ingest_telemetry(
            TELEMETRY_DAG_CONFIG["parquet_path"],
            execution_date=execution_date,
            checkpoint_ts=last_run_time,
        )

        # Ingest device config (needed for calibration)
        ingestion.ingest_config(DEVICE_DAG_CONFIG["config_path"])

        # Log ingestion metadata
        ingestion.log_ingestion_metadata()

        # Save new checkpoint
        now_str = datetime.datetime.now().isoformat()
        ingestion.checkpointing(ti, last_run_time=now_str)

        return True

    # -----------------------
    # Validate Telemetry Snapshot
    # -----------------------
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def validate_telemetry():
        ingestion = Ingestion()
        df_snapshot = ingestion.con.execute("SELECT * FROM telemetry_snapshot").fetchdf()

        validator = Validation()
        validator.validate_and_log(df_snapshot)  # logs nulls, duplicates, outliers, etc.

        return True

    # -----------------------
    # Transform + Validate + Load Batches
    # -----------------------
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def transform_and_store_batches():
        transformer = Transformation()
        validator = Validation()
        storage = IcebergStorage()

        # Process DuckDB snapshot batch-wise
        for batch_df in transformer.transform_telemetry_snapshot(
            config_df=transformer.con.execute("SELECT * FROM config_snapshot").fetchdf()
        ):
            if batch_df.empty:
                continue

            # Run validation on transformed batch
            cleaned_batch = validator.validate_and_clean_data(batch_df)

            # Load into Iceberg
            storage.store_processed_data(cleaned_batch)

        return True

    # -----------------------
    # Generate Telemetry Reports
    # -----------------------
    # @task(retries=1)
    # def generate_reports():
    #     Reports().generate_telemetry_report()
    #     return True

    # -----------------------
    # Task Dependencies
    # -----------------------
    ingested = ingest_telemetry()
    validated = validate_telemetry()
    processed = transform_and_store_batches()
    #reported = generate_reports()

    ingested >> validated >> processed #>> reported
