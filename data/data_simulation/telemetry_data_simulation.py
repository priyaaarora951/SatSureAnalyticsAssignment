import os
from pathlib import Path
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from datetime import datetime

# ========== CONFIGURATION ==========

# Local base root for storing telemetry
BASE_PATH = "./raw"  # adjust as you like
TELEM_PATH = os.path.join(BASE_PATH, "telemetry")
STAGING_PATH = os.path.join(BASE_PATH, "staging", "telemetry")

# List of station IDs you want
STITION_IDS = ["USW00094728"]
TELEM_URL_PATTERN = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/"
    "by-year/2024/parquet/GHCNh_{station}_2024.parquet"
)

def ensure_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)

def download_parquet(url: str, out_path: str):
    """Download via HTTP streaming; raise on HTTP errors."""
    print(f"Downloading {url} -> {out_path}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    ensure_dir(os.path.dirname(out_path))
    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if chunk:
                f.write(chunk)

def repartition_parquet(src_parquet: str, dest_base: str):
    """
    Read the source Parquet file, and write out partitioned by date & hour:
    dest_base/YYYY-MM-DD/HH/*.parquet
    """
    print("Reading", src_parquet)
    # Use dataset scanning to handle large files
    dataset = ds.dataset(src_parquet, format="parquet")
    scanner = dataset.scanner(batch_size=500_000)  # tune batch size if needed

    for batch in scanner.to_batches():
        table = pa.Table.from_batches([batch])
        df = table.to_pandas()

        # Determine the timestamp column name:
        # Many GHCNh Parquet files may use "DATE" column (ISO) per documentation. :contentReference[oaicite:0]{index=0}
        if "DATE" in df.columns:
            df["timestamp"] = pd.to_datetime(df["DATE"])
        elif "date_time" in df.columns:
            df["timestamp"] = pd.to_datetime(df["date_time"])
        elif "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        else:
            raise ValueError(f"No recognizable timestamp column in {src_parquet}, cols: {df.columns}")

        # Derive partition keys
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp"].dt.strftime("%H")

        # For each (date, hour) group, write out
        for (dt, hh), sub in df.groupby(["date", "hour"]):
            out_dir = os.path.join(dest_base, dt, hh)
            ensure_dir(out_dir)

            # filename: include station or batch identity
            base = Path(src_parquet).stem
            # e.g. "GHCNh_USW00094728_2024" -> base
            out_fname = f"{base}_{dt}T{hh}.parquet"
            out_path = os.path.join(out_dir, out_fname)

            sub2 = sub.drop(columns=["date", "hour"])
            tbl2 = pa.Table.from_pandas(sub2)
            pq.write_table(tbl2, out_path)

    print("Finished repartitioning for", src_parquet)

def main():
    ensure_dir(STAGING_PATH)
    ensure_dir(TELEM_PATH)

    for station in STITION_IDS:
        url = TELEM_URL_PATTERN.format(station=station)
        local = os.path.join(STAGING_PATH, f"GHCNh_{station}_2024.parquet")
        try:
            download_parquet(url, local)
        except Exception as e:
            print("Failed downloading for station", station, ":", e)
            continue

        # Repartition that file
        try:
            repartition_parquet(local, TELEM_PATH)
        except Exception as e:
            print("Error repartitioning file", local, ":", e)

    print("All done.")

if __name__ == "__main__":
    main()
