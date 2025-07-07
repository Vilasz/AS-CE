from collections import deque
from datetime import datetime, timedelta

def is_anomalous(event: dict) -> tuple[bool, str | None]:
    """
    Checks if a given event contains an anomalous sensor reading.
    Returns a tuple: (is_anomaly: bool, anomalous_sensor: str | None)
    """
    # Temperature anomaly: < -10.0 or > 45.0 (matches generator)
    if event['temperature'] < -10.0 or event['temperature'] > 45.0:
        return (True, 'temperature')
    # Humidity anomaly: < 0.0 or > 100.0 (matches generator)
    if event['humidity'] < 0.0 or event['humidity'] > 100.0:
        return (True, 'humidity')
    # Pressure anomaly: deviation of at least 30 from the value (generator adds/subtracts 30-50)
    # We'll consider normal pressure to be 980-1040, but generator can create values outside by at least 30
    # So, if pressure < 980-30=950 or > 1040+30=1070, it's definitely an anomaly
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
    
    # Janelas deslizantes para cada sensor
    temp_window = deque(maxlen=window_size)
    hum_window = deque(maxlen=window_size)
    press_window = deque(maxlen=window_size)

    for event in events:
        # Primeiro, verifica se o evento é anômalo
        anomaly_found, _ = is_anomalous(event)
        
        if not anomaly_found:
            # Adiciona valores válidos às janelas
            temp_window.append(event['temperature'])
            hum_window.append(event['humidity'])
            press_window.append(event['pressure'])
        
        # Calcula a média da janela atual e armazena
        # Apenas se a janela estiver cheia para uma média mais estável
        if len(temp_window) == window_size:
            averages['temperature'].append(sum(temp_window) / window_size)
            averages['humidity'].append(sum(hum_window) / window_size)
            averages['pressure'].append(sum(press_window) / window_size)
    
    # Retorna a última média calculada para cada sensor como representante da região
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
    # Usamos um deque como uma janela deslizante de eventos
    window = deque()
    
    for event in events:
        # Converte o timestamp de string ISO para objeto datetime
        event_time = datetime.fromisoformat(event['timestamp'])
        
        # Remove da janela os eventos que estão fora do período de 10 minutos
        # em relação ao evento atual.
        while window and (event_time - window[0]['timestamp_obj']) > timedelta(minutes=window_minutes):
            window.popleft()

        # Adiciona o evento atual (com o objeto datetime) à janela
        anomaly_found, sensor = is_anomalous(event)
        if anomaly_found:
            window.append({
                'timestamp_obj': event_time,
                'sensor': sensor
            })

        # Verifica se a janela atual contém anomalias em mais de um tipo de sensor
        sensors_in_window = {e['sensor'] for e in window}
        if len(sensors_in_window) > 1:
            period_count += 1
            # Limpa a janela para não contar o mesmo período múltiplas vezes
            window.clear()
            
    return period_count