# solution_multiprocessing/processor.py

import multiprocessing
import time
from collections import defaultdict
from .metrics import is_anomalous, calculate_moving_averages, count_multi_sensor_anomaly_periods
from .data_parser import load_and_group_by_station, load_and_group_by_region

# --- As funções Worker (process_station_chunk, process_region_chunk) continuam as mesmas ---

def process_station_chunk(station_data: tuple[int, list[dict]]) -> dict:
    station_id, event_list = station_data
    event_list.sort(key=lambda x: x['timestamp'])
    total_events = len(event_list)
    anomaly_counts = defaultdict(int)
    for event in event_list:
        anomaly_found, sensor = is_anomalous(event)
        if anomaly_found:
            anomaly_counts[sensor] += 1
    multi_sensor_anomaly_periods = count_multi_sensor_anomaly_periods(event_list)
    result = {
        'total_events': total_events,
        'anomaly_counts': dict(anomaly_counts),
        'multi_sensor_anomaly_periods': multi_sensor_anomaly_periods
    }
    return {station_id: result}

def process_region_chunk(region_data: tuple[str, list[dict]]) -> dict:
    region_name, event_list = region_data
    moving_averages = calculate_moving_averages(event_list, window_size=50)
    return {region_name: moving_averages}

# --- Orquestrador de Benchmark ---

def run_analysis_benchmark(data_path: str, num_workers: int, workload_multiplier: int) -> float:
    """
    Executa a análise completa e retorna o tempo de execução.
    """
    # Carrega os dados uma única vez
    station_groups = load_and_group_by_station(data_path)
    region_groups = load_and_group_by_region(data_path)

    if not station_groups:
        print("Arquivo de dados não encontrado.")
        return -1.0
        
    # Simula uma carga de trabalho maior replicando a lista de tarefas
    station_work_items = list(station_groups.items()) * workload_multiplier
    region_work_items = list(region_groups.items()) * workload_multiplier

    start_time = time.perf_counter()

    # Define o número de workers para o Pool
    with multiprocessing.Pool(processes=num_workers) as pool:
        # Executa ambas as análises
        pool.map(process_station_chunk, station_work_items)
        pool.map(process_region_chunk, region_work_items)

    end_time = time.perf_counter()
    duration = end_time - start_time
    
    print(f"Execução com {num_workers} worker(s) finalizada em {duration:.4f} segundos.")
    return duration


if __name__ == "__main__":
    DATA_FILE = "data/synthetic_data.csv"
    # Aumenta a carga de trabalho para que a tarefa demore alguns segundos
    WORKLOAD_MULTIPLIER = 100 
    # Lista de contagens de workers para testar
    WORKER_COUNTS = [1, 2, 4, 8] 

    print("Iniciando benchmark de desempenho do multiprocessing...")
    print(f"Carga de trabalho aumentada em {WORKLOAD_MULTIPLIER}x.\n")

    results = {}
    for workers in WORKER_COUNTS:
        # Ignora o teste se o número de workers for maior que o de CPUs disponíveis
        if workers > multiprocessing.cpu_count():
            print(f"Pulando teste com {workers} workers (Máximo de CPUs: {multiprocessing.cpu_count()}).")
            continue
        
        duration = run_analysis_benchmark(DATA_FILE, num_workers=workers, workload_multiplier=WORKLOAD_MULTIPLIER)
        results[workers] = duration
    
    print("\n--- Resultados do Benchmark ---")
    base_time = results.get(1, 1.0) # Tempo com 1 worker como base
    for workers, duration in results.items():
        speedup = base_time / duration if duration > 0 else 0
        print(f"Workers: {workers} | Tempo: {duration:.4f} seg | Speedup: {speedup:.2f}x")