import json
import os
from datetime import datetime

# Define the base directory for storing config data
base_dir = './raw/config/'

# Ensure the base directory exists
os.makedirs(base_dir, exist_ok=True)

# Generate the current date in YYYY-MM-DD format
today = datetime.now().strftime('%Y-%m-%d')

# Create the directory for today's date
date_dir = os.path.join(base_dir, today)
os.makedirs(date_dir, exist_ok=True)

# Define a list of device configurations
device_configs = [
    {
        "device_id": "device_001",
        "location": "Zone A",
        "sensor_types": ["temperature", "humidity", "pressure"],
        "calibration": {
            "temperature_offset": 0.5,
            "humidity_offset": 2.0,
            "pressure_offset": -1.0
        },
        "last_updated": datetime.now().isoformat()
    },
    {
        "device_id": "device_002",
        "location": "Zone B",
        "sensor_types": ["temperature", "humidity"],
        "calibration": {
            "temperature_offset": 0.3,
            "humidity_offset": 1.5
        },
        "last_updated": datetime.now().isoformat()
    }
]

# Define the path for the devices.json file
devices_file = os.path.join(date_dir, 'devices.json')

# Write the device configurations to the devices.json file
with open(devices_file, 'w') as f:
    json.dump(device_configs, f, indent=4)

print(f"Device configuration saved to {devices_file}")
