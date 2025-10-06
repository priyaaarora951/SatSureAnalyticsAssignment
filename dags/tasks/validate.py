import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime
from dags.tasks.config.settings import ANOMALY_SETTINGS

#TO DO: Need to pick the schema from the data. Should be moved to settings
EXPECTED_SCHEMA = {
    "device_id": "object",           # string
    "timestamp": "datetime64[ns]",   # datetime
    "sensor_type": "object",         # string
    "raw_value": "float64",          # numeric
    "calibrated_value": "float64"    # numeric
}

class Validation:
    def __init__(self, log_file: str = "data_quality_issues.csv"):
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
        # Log file setup
        # -------------------------
        self.log_file = log_file
        if not os.path.exists(self.log_file):
            pd.DataFrame(columns=[
                "timestamp", "issue_type", "description", "affected_rows"
            ]).to_csv(self.log_file, index=False)

    # -------------------------
    # Utility: log issue
    # -------------------------
    def log_issue(self, issue_type: str, description: str, affected_rows: int):
        entry = {
            "timestamp": datetime.now().isoformat(),
            "issue_type": issue_type,
            "description": description,
            "affected_rows": affected_rows
        }
        pd.DataFrame([entry]).to_csv(self.log_file, mode="a", header=False, index=False)
        self.logger.warning(f"Issue logged: {issue_type} | {description} | rows={affected_rows}")

    # -------------------------
    # Null check
    # -------------------------
    def check_nulls(self, df: pd.DataFrame):
        null_counts = df.isnull().sum()
        issues = null_counts[null_counts > 0]
        if not issues.empty:
            self.log_issue(
                "NULL_VALUES",
                f"Nulls detected in columns: {issues.to_dict()}",
                int(issues.sum())
            )

    # -------------------------
    # Duplicate check
    # -------------------------
    def check_duplicates(self, df: pd.DataFrame):
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            self.log_issue(
                "DUPLICATES",
                f"Duplicate rows detected",
                dup_count
            )

    # -------------------------
    # Type mismatch check
    # -------------------------
    def check_type_mismatches(self, df):
        mismatches = {}
        for col, expected_dtype in EXPECTED_SCHEMA.items():
            if col not in df.columns:
                mismatches[col] = f"Missing column (expected {expected_dtype})"
            else:
                actual_dtype = str(df[col].dtype)
                if actual_dtype != expected_dtype:
                    mismatches[col] = f"Expected {expected_dtype}, got {actual_dtype}"
        return mismatches

    # -------------------------
    # Outlier detection
    # -------------------------
    def check_outliers(self, df: pd.DataFrame, col: str = "calibrated_value"):
        if col not in df.columns:
            return 0
        lower, upper = ANOMALY_SETTINGS["lower_threshold"], ANOMALY_SETTINGS["upper_threshold"]
        mask = (df[col] < lower) | (df[col] > upper)
        outliers = df[mask]
        if not outliers.empty:
            self.log_issue(
                "OUTLIERS",
                f"Outliers detected in {col} outside range [{lower}, {upper}]",
                len(outliers)
            )

    # -------------------------
    # Calibration check
    # -------------------------
    def check_calibration(self, df: pd.DataFrame):
        if "calibrated_value" not in df.columns or "raw_value" not in df.columns:
            return 0
        invalid = df[df["calibrated_value"].isnull()]
        if not invalid.empty:
            self.log_issue(
                "CALIBRATION_ERROR",
                f"Calibration failed for some rows (null calibrated_value)",
                len(invalid)
            )

    # -------------------------
    # High-level API: validate + log only
    # -------------------------
    def validate_and_log(self, df: pd.DataFrame):
        self.check_nulls(df)
        self.check_duplicates(df)
        self.check_type_mismatches(df)
        self.check_outliers(df)
        self.check_calibration(df)

    # -------------------------
    # High-level API: validate + clean
    # -------------------------
    def validate_and_clean_data(self, df: pd.DataFrame):
        # Log issues
        self.validate_and_log(df)

        # Clean step (non-destructive where possible)
        df = df.drop_duplicates()
        df = df.dropna(subset=["device_id", "timestamp"])  # critical fields must not be null

        if "calibrated_value" in df.columns:
            lower, upper = ANOMALY_SETTINGS["lower_threshold"], ANOMALY_SETTINGS["upper_threshold"]
            df = df[(df["calibrated_value"] >= lower) & (df["calibrated_value"] <= upper)]

        return df
