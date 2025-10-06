from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from tasks.ingest import Ingestion
from tasks.transform import Transformation
from tasks.validate import Validation
from dags.tasks.load import IcebergStorage
#from dags.tasks.monitor import Reports
from tasks.config.settings import DEVICE_DAG_CONFIG, default_args, max_active_runs, catchup

# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id="device_config_pipeline",
    default_args=default_args,
    description="Device Config ETL pipeline with Iceberg storage",
    schedule_interval='@weekly',  # Weekly schedule for config
    catchup=catchup,
    max_active_runs=max_active_runs,
    tags=["DeviceConfig"],
) as dag:

    # -----------------------
    # Ingest Device Config
    # -----------------------
    @task(retries=3, retry_delay=timedelta(minutes=5))
    def ingest_config_data():
        ingestion = Ingestion()
        config_rows = ingestion.ingest_config(DEVICE_DAG_CONFIG["config_path"])
        ingestion.log_ingestion_metadata()  # Logs number of rows ingested
        return config_rows

    # -----------------------
    # Validate Device Config
    # -----------------------
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def validate_config_data():
        ingestion = Ingestion()
        # Load current config snapshot from DuckDB
        config_df = ingestion.con.execute("SELECT * FROM config_snapshot").fetchdf()
        validator = Validation()
        validator.validate_and_log(config_df)  # logs issues to CSV
        return config_df

    # -----------------------
    # Transform Device Config
    # -----------------------
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def transform_config_data(config_df):
        transformer = Transformation()
        transformed_df = transformer.transform_device_config_data(config_df)
        return transformed_df

    # -----------------------
    # Load Processed Config to Iceberg
    # -----------------------
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def load_config_data(transformed_df):
        storage = IcebergStorage()
        storage.store_processed_data(transformed_df)
        return True

    # -----------------------
    # Generate Device Config Reports
    # -----------------------
    # @task(retries=1)
    # def generate_config_reports():  
    #     Reports().generate_device_config_report()
    #     return True

    # -----------------------
    # DAG Task Dependencies
    # -----------------------
    ingested = ingest_config_data()
    validated = validate_config_data()
    transformed = transform_config_data(validated)
    loaded = load_config_data(transformed)
    #reported = generate_config_reports()

    ingested >> validated >> transformed >> loaded #>> reported