import pendulum
import configparser, os
from datetime import timedelta

# ----------------------------------------------------
# Config File Path Handling
# ----------------------------------------------------
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "dag_config.ini")
CONFIG_PATH = os.environ.get("DAG_CONFIG_PATH", DEFAULT_CONFIG_PATH)

config = configparser.ConfigParser()
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"DAG config file not found: {CONFIG_PATH}")

config.read(CONFIG_PATH)

# ----------------------------------------------------
# Common Configurations
# ----------------------------------------------------

DAY_HOUR = config.getint("common", "day_hour", fallback=24)

# ----------------------------------------------------
# DAG-Specific Configurations
# ----------------------------------------------------
TELEMETRY_DAG_CONFIG = {
    "schedule_interval": config.get("telemetry_dag", "schedule_interval", fallback="@hourly"),
    "parquet_path": config.get("telemetry_dag", "parquet_path", fallback="/tmp/telemetry.parquet"),
}

DEVICE_DAG_CONFIG = {
    "schedule_interval": config.get("device_dag", "schedule_interval", fallback="@weekly"),
    "config_path": config.get("device_dag", "config_path"),
}

DUCK_DB_PATH = {
    "duckdb_path": config.get("dag_settings", "duckdb_path", fallback="/tmp/pipeline.duckdb")
}

# ----------------------------------------------------
# Timezone & Start Date
# ----------------------------------------------------
local_tz_str = config.get("dag_settings", "local_timezone", fallback="ISO8601")
start_date_str = config.get("dag_settings", "start_date", fallback="2025-01-01")

local_tz = pendulum.timezone(local_tz_str)
start_date = pendulum.parse(start_date_str, tz=local_tz)

# ----------------------------------------------------
# DAG Runtime Settings
# ----------------------------------------------------
catchup = config.getboolean("dag_settings", "catchup", fallback=False)
max_active_runs = config.getint("dag_settings", "max_active_runs", fallback=1)

retries = config.getint("dag_settings", "retries", fallback=2)
retry_delay_minutes = config.getint("dag_settings", "retry_delay_minutes", fallback=5)
email_on_failure = config.getboolean("dag_settings", "email_on_failure", fallback=True)
email_on_retry = config.getboolean("dag_settings", "email_on_retry", fallback=False)

# ----------------------------------------------------
# DAG Default Args
# ----------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": start_date,
    "email_on_failure": email_on_failure,
    "email_on_retry": email_on_retry,
    "retries": retries,
    "retry_delay": timedelta(minutes=retry_delay_minutes),
}

#--------------------------------------------------------
#Transformation Anamoly Detection Settings
#--------------------------------------------------------
ANOMALY_SETTINGS = {
    "upper_threshold": config.getfloat("anomaly_settings", "upper_threshold", fallback=100),
    "lower_threshold": config.getfloat("anomaly_settings", "lower_threshold", fallback=0),
}

#--------------------------------------------------------
#Transformation Anamoly Detection Settings
#-----------------------------------------
METADATA_COLS = {
    "metadata_cols": config.get("tansform_settings", "metadata_cols", fallback="['device_id', 'timestamp', 'scale', 'offset', 'device_type']")
}

#--------------------------------------------------------
#Transformation Anamoly Detection Settings
#-----------------------------------------
ICEBERG_CONFIG = {
    "catalog_name": "local",
    "warehouse_path": "/tmp/warehouse", 
    "db": "db",
    "table_name": "telemetry_snapshot"
}

#--------------------------------------------------------
#Iceberg Storage parameters
#--------------------------------------------------------   
BYTES_SIZE = config.getint("load_settings", "bytes_size", fallback=1024)
TARGET_FILE_SIZE_MB = config.getint("load_settings", "target_file_size_mb", fallback=128)