from collections import deque
from datetime import datetime, timedelta

def is_anomalous(event: dict) -> tuple[bool, str | None]:
    """
    Checks if a given event contains an anomalous sensor reading.
    Returns a tuple: (is_anomaly: bool, anomalous_sensor: str | None)
    """
    if event['temperature'] < -10.0 or event['temperature'] > 45.0:
        return (True, 'temperature')
    if event['humidity'] < 0.0 or event['humidity'] > 100.0:
        return (True, 'humidity')
    if event['pressure'] < 950.0 or event['pressure'] > 1070.0:
        return (True, 'pressure')
    return (False, None)



def calculate_moving_averages(events: list[dict], window_size: int) -> dict:
    """
    Calcula as médias móveis para temperatura, umidade e pressão,
    ignorando valores anômalos.
    """
    averages = {
        'temperature': [],
        'humidity': [],
        'pressure': []
    }
    
    temp_window = deque(maxlen=window_size)
    hum_window = deque(maxlen=window_size)
    press_window = deque(maxlen=window_size)

    for event in events:
        anomaly_found, _ = is_anomalous(event)
        
        if not anomaly_found:
            temp_window.append(event['temperature'])
            hum_window.append(event['humidity'])
            press_window.append(event['pressure'])
        
        if len(temp_window) == window_size:
            averages['temperature'].append(sum(temp_window) / window_size)
            averages['humidity'].append(sum(hum_window) / window_size)
            averages['pressure'].append(sum(press_window) / window_size)
    
    last_averages = {
        sensor: round(vals[-1], 2) if vals else 0
        for sensor, vals in averages.items()
    }
    
    return last_averages

def count_multi_sensor_anomaly_periods(events: list[dict], window_minutes: int = 10) -> int:
    """
    Conta o número de períodos de 'window_minutes' em que uma estação teve
    anomalias em sensores distintos.
    """
    if not events:
        return 0

    period_count = 0
    window = deque()
    
    for event in events:
        event_time = datetime.fromisoformat(event['timestamp'])
        
        while window and (event_time - window[0]['timestamp_obj']) > timedelta(minutes=window_minutes):
            window.popleft()

        anomaly_found, sensor = is_anomalous(event)
        if anomaly_found:
            window.append({
                'timestamp_obj': event_time,
                'sensor': sensor
            })

        sensors_in_window = {e['sensor'] for e in window}
        if len(sensors_in_window) > 1:
            period_count += 1
            window.clear()
            
    return period_count