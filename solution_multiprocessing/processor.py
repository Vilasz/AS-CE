import multiprocessing
import time
import csv
import io
import os
from collections import defaultdict

# Importa as funções de métricas e o parser do próprio módulo
from .metrics import is_anomalous, calculate_moving_averages, count_multi_sensor_anomaly_periods
from .data_parser import get_file_chunks

def process_file_chunk(args: tuple[str, int, int]) -> dict:
    """
    Função do worker que lê um pedaço do arquivo, processa as métricas
    e coleta as anomalias encontradas.
    """
    data_path, start_byte, end_byte = args
    
    station_events = defaultdict(list)
    region_events = defaultdict(list)
    
    found_anomalies_in_chunk = []

    with open(data_path, 'r', newline='') as f:
        f.seek(start_byte)
        chunk_size = end_byte - start_byte
        chunk_buffer = io.StringIO(f.read(chunk_size))

    reader = csv.reader(chunk_buffer)
    header = ["timestamp", "station_id", "region", "temperature", "humidity", "pressure"]
    
    for row in reader:
        if len(row) != len(header):
            continue
            
        try:
            event = {h: v for h, v in zip(header, row)}
            event['station_id'] = int(event['station_id'])
            event['temperature'] = float(event['temperature'])
            event['humidity'] = float(event['humidity'])
            event['pressure'] = float(event['pressure'])

            station_events[event['station_id']].append(event)
            
            # Verifica a anomalia uma vez e coleta se for o caso
            anomaly_found, sensor = is_anomalous(event)
            if anomaly_found:
                found_anomalies_in_chunk.append({
                    "timestamp": event['timestamp'],
                    "station_id": event['station_id'],
                    "sensor": sensor
                })
            else:
                # Apenas eventos não anômalos para o cálculo da média
                region_events[event['region']].append(event)
        except (ValueError, KeyError):
            continue

    station_results = {}
    for station_id, events in station_events.items():
        events.sort(key=lambda x: x['timestamp'])
        
        # A contagem de anomalias pode ser feita a partir da lista já coletada
        anomaly_count = sum(1 for anom in found_anomalies_in_chunk if anom['station_id'] == station_id)
        
        station_results[station_id] = {
            'total_events': len(events), 
            'anomaly_events': anomaly_count,
            'multi_sensor_periods': count_multi_sensor_anomaly_periods(events)
        }

    region_results = {}
    for region, events in region_events.items():
        events.sort(key=lambda x: x['timestamp'])
        region_results[region] = calculate_moving_averages(events, window_size=50)

    # Retorna os resultados e também a lista de anomalias
    return {
        "station_results": station_results, 
        "region_results": region_results, 
        "found_anomalies": found_anomalies_in_chunk
    }


# A função agora retorna uma tupla (float, list)
def run_analysis(data_path: str, num_workers: int) -> tuple[float, list]:
    start_time = time.perf_counter()

    # 1. Obter os pedaços do arquivo para cada worker
    chunks = get_file_chunks(data_path, num_workers)
    task_args = [(data_path, start, end) for start, end in chunks]

    # 2. Executar o processamento em paralelo
    with multiprocessing.Pool(processes=num_workers) as pool:
        partial_results = pool.map(process_file_chunk, task_args)

    final_station_report = defaultdict(lambda: {"total_events": 0, "anomaly_events": 0, "multi_sensor_periods": 0})
    # Lista para agregar todas as anomalias encontradas pelos workers
    all_found_anomalies = []

    for res in partial_results:
        for station_id, metrics in res['station_results'].items():
            final_station_report[station_id]["total_events"] += metrics["total_events"]
            final_station_report[station_id]["anomaly_events"] += metrics["anomaly_events"]
        
        # Adiciona as anomalias encontradas pelo worker à lista principal
        if res.get("found_anomalies"):
            all_found_anomalies.extend(res['found_anomalies'])
    
    end_time = time.perf_counter()
    
    # Retorna o tempo e a lista agregada de anomalias
    return (end_time - start_time), all_found_anomalies


if __name__ == '__main__':
    DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'synthetic_data.csv')
    print("Iniciando teste direto do processador Multiprocessing com 4 workers...")
    
    execution_time, anomalies_found = run_analysis(DATA_FILE, num_workers=4)
    
    print(f"Tempo de execução do teste direto: {execution_time:.4f} segundos.")
    print(f"Total de anomalias encontradas: {len(anomalies_found)}")
    if anomalies_found:
        print("Exemplo de anomalia encontrada:", anomalies_found[0])