import pandas as pd
import numpy as np
from datetime import datetime
import logging
import json
import duckdb
from dags.tasks.config.settings import ANOMALY_SETTINGS, METADATA_COLS, DUCK_DB_PATH

class Transformation:
    def __init__(self):
        # -------------------------
        # Logger
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
        self.duckdb_path = DUCK_DB_PATH.get("duckdb_path")
        self.con = duckdb.connect(self.duckdb_path) if self.duckdb_path else duckdb.connect()
        self.logger.info(f"DuckDB connected at {self.duckdb_path or ':memory:'}")

        # Metadata columns
        self.metadata_cols = json.loads(METADATA_COLS.get("metadata_cols", "[]"))

    # -------------------------
    # Sensor columns
    # -------------------------
    def get_sensor_columns(self, df_columns):
        return [c for c in df_columns if c not in self.metadata_cols]

    # -------------------------
    # Process batches from DuckDB
    # -------------------------
    def process_batches(self, query, batch_size=100_000):
        try:
            reader = self.con.execute(query).fetch_record_batch(batch_size)
            if reader is None:
                return
            while True:
                batch = reader.read_next_batch()
                if batch is None or batch.num_rows == 0:
                    break
                yield batch.to_pandas()
        except StopIteration:
            return

    # -------------------------
    # Calibration
    # -------------------------
    def calibrate_batch(self, df_batch, config_df):
        id_vars = [c for c in ['device_id', 'timestamp'] if c in df_batch.columns]
        sensor_cols = self.get_sensor_columns(df_batch.columns)
        if not sensor_cols:
            return pd.DataFrame()

        long_df = df_batch.melt(
            id_vars=id_vars,
            value_vars=sensor_cols,
            var_name='sensor_type',
            value_name='raw_value'
        )

        # Ensure config defaults
        for col, default in {'scale':1.0, 'offset':0.0, 'device_type':'unknown'}.items():
            if col not in config_df.columns:
                config_df[col] = default

        if 'device_id' not in config_df.columns:
            raise ValueError("Config must contain 'device_id'")

        df = long_df.merge(
            config_df[['device_id','scale','offset','device_type']],
            on='device_id',
            how='left'
        )

        df['scale'] = df['scale'].fillna(1.0)
        df['offset'] = df['offset'].fillna(0.0)
        df['calibrated_value'] = df['raw_value'] * df['scale'] + df['offset']

        return df

    # -------------------------
    # Derived fields
    # -------------------------
    def add_derived_fields_batch(self, df_batch):
        df_batch = df_batch.copy()
        df_batch['event_ts'] = pd.to_datetime(df_batch['timestamp'])
        df_batch['ingestion_ts'] = datetime.now()
        df_batch['event_date'] = df_batch['event_ts'].dt.date
        df_batch['event_hour'] = df_batch['event_ts'].dt.hour

        # Daily/hourly averages
        df_batch['day_avg'] = df_batch.groupby(['device_id','sensor_type','event_date'])['calibrated_value'].transform('mean')
        df_batch['hour_avg'] = df_batch.groupby(['device_id','sensor_type','event_date','event_hour'])['calibrated_value'].transform('mean')

        # Rolling 7-day average per device/sensor
        df_batch = df_batch.sort_values(by=['device_id','sensor_type','event_ts'])
        df_batch['rolling_7d_avg'] = np.nan

        # Use original indices to assign values
        for (dev, sensor), grp in df_batch.groupby(['device_id','sensor_type']):
            grp_sorted = grp.sort_values('event_ts')
            rolled = grp_sorted.set_index('event_ts')['calibrated_value'].rolling('7d').mean()
            df_batch.loc[grp_sorted.index, 'rolling_7d_avg'] = rolled.values

        return df_batch 

    # -------------------------
    # Anomaly detection
    # -------------------------
    def detect_anomalies_batch(self, df_batch):
        upper = ANOMALY_SETTINGS.get("upper_threshold", 999999)
        lower = ANOMALY_SETTINGS.get("lower_threshold", -999999)
        df_batch['anomaly_flag'] = ((df_batch['calibrated_value'] > upper) |
                                    (df_batch['calibrated_value'] < lower)).astype(int)
        return df_batch

    # -------------------------
    # Full batch transform
    # -------------------------
    def transform_telemetry_snapshot(self, config_df, batch_size=100_000):
        query = "SELECT * FROM telemetry_snapshot"
        for df_batch in self.process_batches(query, batch_size=batch_size):
            if df_batch.empty:
                continue

            df_batch = df_batch.drop_duplicates()
            df_batch = df_batch.dropna(subset=['device_id','timestamp'])
            df_batch = self.calibrate_batch(df_batch, config_df)

            if df_batch.empty:
                continue

            df_batch['raw_value'] = df_batch['raw_value'].fillna(df_batch['raw_value'].mean())
            df_batch['calibrated_value'] = df_batch['calibrated_value'].fillna(df_batch['calibrated_value'].mean())
            upper, lower = ANOMALY_SETTINGS.get("upper_threshold", 999999), ANOMALY_SETTINGS.get("lower_threshold", -999999)
            df_batch['calibrated_value'] = df_batch['calibrated_value'].clip(lower, upper)

            df_batch = self.add_derived_fields_batch(df_batch)
            df_batch = self.detect_anomalies_batch(df_batch)
            yield df_batch

    # -------------------------
    # Config normalization
    # -------------------------
    def transform_device_config_data(self, config_df):
        if 'device_type' in config_df.columns:
            config_df['device_type'] = config_df['device_type'].fillna('unknown')
        return config_df
