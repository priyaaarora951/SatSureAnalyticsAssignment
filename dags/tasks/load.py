import logging
from typing import Dict, Any, Optional

import pandas as pd
from pyspark.sql import SparkSession

from dags.tasks.config.settings import ICEBERG_CONFIG, BYTES_SIZE, TARGET_FILE_SIZE_MB


class IcebergStorage:
    def __init__(self):
        """
        Initialize Iceberg connection, Spark session, and table.
        """
        self.catalog_name = ICEBERG_CONFIG["catalog_name"]
        self.warehouse_path = ICEBERG_CONFIG["warehouse_path"]
        self.table_name = ICEBERG_CONFIG["table_name"]
        self.database = ICEBERG_CONFIG["db"]

        # -------------------------
        # Logger setup
        # -------------------------
        self.logger = logging.getLogger("IcebergStorage")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            self.logger.addHandler(handler)
        self.logger.info(f"IcebergStorage initialized for {self.catalog_name}.{self.database}.{self.table_name}")

        # -------------------------
        # Spark Session w/ Iceberg
        # -------------------------
        self.spark = (
            SparkSession.builder
            .appName("IcebergPipeline")
            # Iceberg catalog registration
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.warehouse_path)
            # Iceberg SQL extensions (enables CALL, MERGE, etc.)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate()
        )

        # Enable Arrow for Pandas->Spark conversion
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        # -------------------------
        # Create table if missing
        # -------------------------
        self.iceberg_table = f"{self.catalog_name}.{self.database}.{self.table_name}"

        self.logger.info(f"Ensuring Iceberg table exists: {self.iceberg_table}")
        # Create with format-version=2 for upserts (position deletes) if not already
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.iceberg_table} (
                device_id STRING,
                timestamp TIMESTAMP,
                sensor_type STRING,
                raw_value DOUBLE,
                calibrated_value DOUBLE,
                event_date DATE,
                device_type STRING
            )
            USING iceberg
            PARTITIONED BY (event_date, device_type)
            TBLPROPERTIES ('format-version'='2')
        """)
        self.logger.info(f"Iceberg table ready: {self.iceberg_table}")

    # ---------------------------
    # Public methods
    # ---------------------------
    def store_processed_data(self, df: pd.DataFrame, compact: bool = False, compact_threshold: int = 500):
        """
        Store processed data: upsert, optional compaction (if requested or if threshold exceeded),
        and then return commit stats.
        """
        self.upsert_data(df)

        if compact:
            # Only compact if number of data files exceeds threshold to save compute
            file_count = self._get_file_count()
            if file_count is None:
                self.logger.info("Could not determine file count; proceeding to compact anyway.")
                self.compact_table()
            elif file_count >= compact_threshold:
                self.logger.info(f"File count {file_count} >= threshold {compact_threshold}; running compaction.")
                self.compact_table()
            else:
                self.logger.info(f"Skipping compaction: file_count={file_count} < threshold={compact_threshold}")
        else:
            self.logger.info("Compaction skipped (compact=False).")

        stats = self.get_commit_stats()
        return stats

    def upsert_data(self, df: pd.DataFrame):
        """
        Merge new batch into Iceberg table:
        - Upsert on (device_id, timestamp)
        - Support late-arriving data
        """
        # 1. Evolve schema if needed
        self.evolve_schema(df)

        # 2. Convert Pandas DF → Spark DF (Arrow enabled)
        spark_df = self.spark.createDataFrame(df)
        spark_df.createOrReplaceTempView("updates")

        # 3. MERGE INTO (requires Iceberg format-version=2)
        self.spark.sql(f"""
            MERGE INTO {self.iceberg_table} t
            USING updates u
            ON t.device_id = u.device_id AND t.timestamp = u.timestamp
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.logger.info(f"Upsert completed for {len(df)} rows into {self.iceberg_table}")

    def evolve_schema(self, df: pd.DataFrame):
        """
        Add new columns dynamically to the Iceberg table using Spark SQL.
        Maps pandas dtypes → Spark SQL types for ALTER TABLE statements.
        """
        if df is None or df.empty:
            self.logger.info("No data provided for schema evolution.")
            return

        new_columns = list(df.columns)
        existing_cols = [col.name for col in self.spark.table(self.iceberg_table).schema.fields]
        missing_cols = [col for col in new_columns if col not in existing_cols]

        if not missing_cols:
            self.logger.info("No new columns to add. Schema is up-to-date.")
            return

        # mapping pandas dtype -> spark sql type
        dtype_map = {
            "int64": "BIGINT",
            "float64": "DOUBLE",
            "bool": "BOOLEAN",
            "object": "STRING",
            "datetime64[ns]": "TIMESTAMP",
            "timedelta64[ns]": "STRING"  # store durations as string by default
        }

        for col in missing_cols:
            pandas_dtype = str(df[col].dtype)
            spark_type = dtype_map.get(pandas_dtype, "STRING")
            # If column name conflicts / contains special chars, you might need to quote it.
            self.spark.sql(f"ALTER TABLE {self.iceberg_table} ADD COLUMN IF NOT EXISTS {col} {spark_type}")
            self.logger.info(f"Added new column to Iceberg table: {col} ({pandas_dtype} -> {spark_type})")

        self.logger.info("Schema evolution completed.")

    # ---------------------------
    # Compaction & helpers
    # ---------------------------
    def compact_table(self, target_file_size_mb: Optional[int] = None):
        """
        Optimize Iceberg table by compacting small data files and manifests.

        - target_file_size_mb: optional override (if None, value is read from TARGET_FILE_SIZE_MB).
        """
        try:
            self.logger.info(f"Starting compaction for {self.iceberg_table}")

            # pre-compaction stats
            pre_stats = self._get_file_stats()
            if pre_stats:
                self.logger.info(f"Before compaction: {pre_stats['file_count']} files, {pre_stats['total_mb']:.2f} MB total")
            else:
                self.logger.info("Before compaction: could not read file stats.")

            # determine target_file_size_mb robustly
            if target_file_size_mb is None:
                if isinstance(TARGET_FILE_SIZE_MB, dict):
                    target_file_size_mb = int(TARGET_FILE_SIZE_MB.get("target_file_size_mb", 128))
                else:
                    target_file_size_mb = int(TARGET_FILE_SIZE_MB)

            target_file_size_bytes = int(target_file_size_mb) * 1024 * 1024  # MB -> bytes

            # rewrite data files
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.rewrite_data_files(
                    table => '{self.database}.{self.table_name}',
                    strategy => 'binpack',
                    options => map('target-file-size-bytes', '{target_file_size_bytes}')
                )
            """)
            self.logger.info("Data file compaction completed (rewrite_data_files).")

            # rewrite manifests (metadata compact)
            self.spark.sql(f"""
                CALL {self.catalog_name}.system.rewrite_manifests(
                    table => '{self.database}.{self.table_name}'
                )
            """)
            self.logger.info("Manifest compaction completed (rewrite_manifests).")

            # post-compaction stats (use standard MB conversion)
            post_stats = self._get_file_stats()
            if post_stats:
                self.logger.info(f"After compaction: {post_stats['file_count']} files, {post_stats['total_mb']:.2f} MB total")
            else:
                self.logger.info("After compaction: could not read file stats.")

            self.logger.info(f"Compaction successful for {self.iceberg_table}")

        except Exception as e:
            self.logger.error(f"Compaction failed for {self.iceberg_table}: {e}")
            raise

    def _get_file_stats(self) -> Optional[Dict[str, Any]]:
        """
        Return file_count and total_mb by querying <table>.files system table.
        Returns None on failure.
        """
        try:
            row = self.spark.sql(f"""
                SELECT
                    COUNT(*) AS file_count,
                    SUM(file_size_in_bytes) / 1024.0 / 1024.0 AS total_mb
                FROM {self.iceberg_table}.files
            """).collect()
            if not row:
                return None
            r = row[0].asDict()
            return {"file_count": int(r.get("file_count", 0)), "total_mb": float(r.get("total_mb") or 0.0)}
        except Exception as e:
            self.logger.warning(f"Could not fetch file stats: {e}")
            return None

    def _get_file_count(self) -> Optional[int]:
        stats = self._get_file_stats()
        return stats["file_count"] if stats else None

    # ---------------------------
    # Commit/snapshot info
    # ---------------------------
    def get_commit_stats(self) -> Dict[str, Any]:
        """
        Return details about the most recent Iceberg snapshot (commit).
        """
        try:
            self.logger.info(f"Fetching commit stats for {self.iceberg_table}")

            # snapshots table (most recent)
            snapshots_df = self.spark.sql(f"SELECT * FROM {self.iceberg_table}.snapshots ORDER BY committed_at DESC LIMIT 1")
            if snapshots_df.count() == 0:
                self.logger.warning("No snapshots found for this table yet.")
                return {}

            snapshot = snapshots_df.collect()[0].asDict()

            # history table (most recent)
            history_df = self.spark.sql(f"SELECT * FROM {self.iceberg_table}.history ORDER BY made_current_at DESC LIMIT 1")
            history_row = history_df.collect()[0].asDict() if history_df.count() > 0 else {}

            stats = {
                "snapshot_id": snapshot.get("snapshot_id"),
                "parent_id": snapshot.get("parent_id"),
                "operation": snapshot.get("operation"),
                "committed_at": snapshot.get("committed_at"),
                "added_files": snapshot.get("added_data_files"),
                "removed_files": snapshot.get("removed_data_files"),
                "added_records": snapshot.get("added_records"),
                "removed_records": snapshot.get("removed_records"),
                "manifest_count": snapshot.get("manifest_count"),
                "total_data_files": snapshot.get("total_data_files"),
                "made_current_at": history_row.get("made_current_at")
            }

            self.logger.info(
                f"Latest commit: ID={stats['snapshot_id']} | Op={stats['operation']} | "
                f"Added={stats['added_files']} files | Removed={stats['removed_files']} files | At={stats['committed_at']}"
            )

            return stats

        except Exception as e:
            self.logger.error(f"Error fetching commit stats for {self.iceberg_table}: {e}")
            raise

    # ---------------------------
    # Cleanup
    # ---------------------------
    def close(self):
        """Stop Spark session when done (call from your DAG/task finally block)."""
        try:
            self.spark.stop()
            self.logger.info("Spark session stopped.")
        except Exception as e:
            self.logger.warning(f"Error stopping Spark session: {e}")
 