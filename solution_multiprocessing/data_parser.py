import csv
from collections import defaultdict

def load_and_group_by_station(data_path: str) -> dict[int, list[dict]]:
    """
    Loads data from a CSV file and groups it by station_id without pandas.

    Returns:
        A dictionary where keys are station_ids and values are lists of 
        event dictionaries for that station.
    """
    station_groups = defaultdict(list)
    
    try:
        with open(data_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Manually convert data types from string to numeric
                # This is a critical step that pandas did automatically
                try:
                    event = {
                        'timestamp': row['timestamp'],
                        'station_id': int(row['station_id']),
                        'region': row['region'],
                        'temperature': float(row['temperature']),
                        'humidity': float(row['humidity']),
                        'pressure': float(row['pressure']),
                    }
                    station_groups[event['station_id']].append(event)
                except (ValueError, KeyError) as e:
                    print(f"Warning: Skipping malformed row: {row}. Error: {e}")

    except FileNotFoundError:
        # Return an empty dict and let the processor handle the error message
        return {}

    return station_groups

def load_and_group_by_region(data_path: str) -> dict[str, list[dict]]:
    """
    Carrega dados de um arquivo CSV, agrupa por região e ordena por timestamp.
    """
    region_groups = defaultdict(list)
    
    # Reutilizamos a lógica de carregamento da função anterior
    station_groups = load_and_group_by_station(data_path)
    if not station_groups:
        return {}

    # Desagrupa os dados das estações e reagrupa por região
    all_events = []
    for station_id in station_groups:
        all_events.extend(station_groups[station_id])

    for event in all_events:
        region_groups[event['region']].append(event)
        
    # Ordena os eventos de cada região pelo timestamp
    for region in region_groups:
        region_groups[region].sort(key=lambda x: x['timestamp'])
        
    return region_groups