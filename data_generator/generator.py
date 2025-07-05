# Add the project root to sys.path
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import csv
import random
import datetime
import json
from pathlib import Path
from faker import Faker
from core.models import MeteorologicalEvent
from data_generator.anomalies import ANOMALY_FUNCTIONS # Import our new functions

# --- Configuration ---
NUM_REGIONS = 5
STATIONS_PER_REGION = 5
TOTAL_STATIONS = NUM_REGIONS * STATIONS_PER_REGION

def setup_regions(fake: Faker) -> dict:
    """Creates a set of fake regions with varied base meteorological data."""
    regions = {}
    for _ in range(NUM_REGIONS):
        region_name = fake.city()
        regions[region_name] = {
            "temp": random.uniform(15, 30),
            "humidity": random.uniform(60, 90),
            "pressure": random.uniform(1005, 1020)
        }
    return regions

def generate_data(num_events: int, output_csv_path: str, output_json_path: str, anomaly_percentage: float):
    """
    Generates synthetic data with anomalies and saves both the dataset and the
    ground truth for anomalies.
    """
    print(f"Generating {num_events} events with {anomaly_percentage:.0%} anomalies...")

    fake = Faker('pt_BR')
    regions_config = setup_regions(fake)
    regions_list = list(regions_config.keys())
    
    Path(output_csv_path).parent.mkdir(parents=True, exist_ok=True)
    
    generated_anomalies = []
    
    with open(output_csv_path, 'w', newline='') as csvfile:
        fieldnames = ["timestamp", "station_id", "region", "temperature", "humidity", "pressure"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        today = datetime.datetime.now(datetime.timezone.utc)
        start_date = today - datetime.timedelta(days=1)

        for _ in range(num_events):
            station_id = random.randint(1, TOTAL_STATIONS)
            region_name = regions_list[(station_id - 1) // STATIONS_PER_REGION]
            base_values = regions_config[region_name]
            
            event_time = fake.date_time_between(start_date=start_date, end_date=today, tzinfo=datetime.timezone.utc)

            event_data = {
                "timestamp": event_time.isoformat(),
                "station_id": station_id,
                "region": region_name,
                "temperature": round(base_values["temp"] + random.uniform(-2.5, 2.5), 2),
                "humidity": round(base_values["humidity"] + random.uniform(-5, 5), 2),
                "pressure": round(base_values["pressure"] + random.uniform(-3, 3), 2)
            }
            
            # Decide whether to introduce an anomaly for this event [cite: 42]
            if random.random() < (anomaly_percentage / 100.0):
                sensor_to_alter = random.choice(list(ANOMALY_FUNCTIONS.keys()))
                
                # Get the anomaly function and apply it
                anomaly_func = ANOMALY_FUNCTIONS[sensor_to_alter]
                normal_value = event_data[sensor_to_alter]
                anomalous_value = anomaly_func(normal_value)
                event_data[sensor_to_alter] = anomalous_value
                
                # Record the anomaly for later validation
                generated_anomalies.append({
                    "timestamp": event_data["timestamp"],
                    "station_id": station_id,
                    "sensor": sensor_to_alter,
                    "value": anomalous_value,
                })

            writer.writerow(event_data)

    # Persist the list of generated anomalies to a JSON file 
    with open(output_json_path, 'w') as jsonfile:
        json.dump(generated_anomalies, jsonfile, indent=4)
            
    print(f"Data generation complete. CSV saved to {output_csv_path}")
    print(f"Anomalies ground truth saved to {output_json_path}")


# This part allows you to run this file directly to test it
if __name__ == "__main__":
    NUMBER_OF_EVENTS = 10000
    ANOMALY_PERCENTAGE = 5.0  # Introduce anomalies in 5% of events
    
    OUTPUT_CSV = "data/synthetic_data.csv"
    OUTPUT_JSON = "data/generated_anomalies.json"
    
    generate_data(
        num_events=NUMBER_OF_EVENTS, 
        output_csv_path=OUTPUT_CSV, 
        output_json_path=OUTPUT_JSON,
        anomaly_percentage=ANOMALY_PERCENTAGE
    )