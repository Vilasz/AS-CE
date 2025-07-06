# # solution_multiprocessing/processor.py
# import time
# import multiprocessing
# from collections import defaultdict
# from .metrics import is_anomalous, calculate_moving_averages, count_multi_sensor_anomaly_periods
# from .data_parser import load_and_group_by_station, load_and_group_by_region

# # --- Funções Worker ---

# def process_station_chunk(station_data: tuple[int, list[dict]]) -> dict:
#     """
#     Worker que agora calcula AMBAS as métricas por estação:
#     1. Percentual de anomalias.
#     2. Contagem de períodos com anomalias em múltiplos sensores.
#     """
#     station_id, event_list = station_data
    
#     # Ordena os eventos por tempo, crucial para a nova métrica
#     event_list.sort(key=lambda x: x['timestamp'])

#     # Cálculo do percentual de anomalias (como antes)
#     total_events = len(event_list)
#     anomaly_counts = defaultdict(int)
#     for event in event_list:
#         anomaly_found, sensor = is_anomalous(event)
#         if anomaly_found:
#             anomaly_counts[sensor] += 1
    
#     # NOVO: Cálculo dos períodos de anomalia múltipla
#     multi_sensor_anomaly_periods = count_multi_sensor_anomaly_periods(event_list)

#     result = {
#         'total_events': total_events,
#         'anomaly_counts': dict(anomaly_counts),
#         'multi_sensor_anomaly_periods': multi_sensor_anomaly_periods
#     }
#     return {station_id: result}


# def process_region_chunk(region_data: tuple[str, list[dict]]) -> dict:
#     region_name, event_list = region_data
#     moving_averages = calculate_moving_averages(event_list, window_size=50)
#     return {region_name: moving_averages}


# --- Orquestrador Principal ---

# def run_analysis(data_path: str):
#     """
#     Executa a análise completa para todas as 3 métricas.
#     """
#     print("Iniciando análise completa com multiprocessing...")

#     # --- Análises por Estação ---
#     station_groups = load_and_group_by_station(data_path)
#     if not station_groups:
#         print(f"Erro: Arquivo não encontrado em {data_path} ou está vazio.")
#         print("Por favor, gere os dados primeiro: python data_generator/generator.py")
#         return
    
#     with multiprocessing.Pool() as pool:
#         station_results = pool.map(process_station_chunk, station_groups.items())

#     final_station_report = {k: v for d in station_results for k, v in d.items()}

#     print("\n--- Relatório de Anomalias por Estação ---")
#     for station_id, report in sorted(final_station_report.items()):
#         print(f"\n[Estação ID: {station_id}] - Total de Eventos: {report['total_events']}")
#         # Imprime o resultado da nova métrica
#         print(f"  Períodos com anomalias em múltiplos sensores: {report['multi_sensor_anomaly_periods']}")
        
#         if not report['anomaly_counts']:
#             print("  Nenhuma anomalia percentual detectada.")
#             continue
#         for sensor, count in report['anomaly_counts'].items():
#             percentage = (count / report['total_events']) * 100
#             print(f"  - {sensor.capitalize()}: {count} anomalias ({percentage:.2f}%)")

#     # --- Análise por Região ---
#     region_groups = load_and_group_by_region(data_path)
#     with multiprocessing.Pool() as pool:
#         region_results = pool.map(process_region_chunk, region_groups.items())
#     final_region_report = {k: v for d in region_results for k, v in d.items()}

#     print("\n\n--- Relatório de Média Móvel por Região ---")
#     for region, averages in sorted(final_region_report.items()):
#         print(f"\n[Região: {region}]")
#         print(f"  - Temperatura Média: {averages['temperature']:.2f} °C")
#         print(f"  - Umidade Média: {averages['humidity']:.2f} %")
#         print(f"  - Pressão Média: {averages['pressure']:.2f} hPa")

#     print("\nAnálise completa.")

# solution_multiprocessing/processor.py
import multiprocessing
import time
import csv
from collections import defaultdict
import io
from .metrics import is_anomalous, calculate_moving_averages, count_multi_sensor_anomaly_periods
from .data_parser import get_file_chunks

def process_file_chunk(args: tuple[str, int, int]) -> dict:
    """
    A worker function that reads an assigned file chunk into memory and then processes it.
    """
    data_path, start_byte, end_byte = args
    
    station_events = defaultdict(list)
    region_events = defaultdict(list)

    # Each worker opens the file and reads its specific byte chunk into a buffer
    with open(data_path, 'r', newline='') as f:
        f.seek(start_byte)
        chunk_size = end_byte - start_byte
        # Read the entire assigned chunk into an in-memory text buffer
        chunk_buffer = io.StringIO(f.read(chunk_size))

    # Now, process the in-memory buffer, which is safe and fast
    reader = csv.reader(chunk_buffer)
    header = ["timestamp", "station_id", "region", "temperature", "humidity", "pressure"]
    
    for row in reader:
        # It's possible to read a partial line at the end of a chunk, so we check
        if len(row) != len(header):
            continue
            
        try:
            event = {h: v for h, v in zip(header, row)}
            # Manual type conversion
            event['station_id'] = int(event['station_id'])
            event['temperature'] = float(event['temperature'])
            event['humidity'] = float(event['humidity'])
            event['pressure'] = float(event['pressure'])

            station_events[event['station_id']].append(event)
            if not is_anomalous(event)[0]:
                region_events[event['region']].append(event)
        except (ValueError, KeyError):
            # Skip malformed rows
            continue

    # --- Perform calculations on the worker's local data ---
    station_results = {}
    for station_id, events in station_events.items():
        events.sort(key=lambda x: x['timestamp'])
        anomaly_events = [e for e in events if is_anomalous(e)[0]]
        station_results[station_id] = {
            'total_events': len(events), 'anomaly_events': len(anomaly_events),
            'multi_sensor_periods': count_multi_sensor_anomaly_periods(events)
        }

    region_results = {}
    for region, events in region_events.items():
        events.sort(key=lambda x: x['timestamp'])
        region_results[region] = calculate_moving_averages(events, window_size=50)

    return {"station_results": station_results, "region_results": region_results}


def run_analysis(data_path: str, num_workers: int) -> float:
    start_time = time.perf_counter()

    # 1. Get file chunks
    chunks = get_file_chunks(data_path, num_workers)
    task_args = [(data_path, start, end) for start, end in chunks]

    # 2. Run the pool
    with multiprocessing.Pool(processes=num_workers) as pool:
        partial_results = pool.map(process_file_chunk, task_args)
    
    # 3. Final, fast aggregation
    final_station_report = defaultdict(lambda: {"total_events": 0, "anomaly_events": 0, "multi_sensor_periods": 0})
    for res in partial_results:
        for station_id, metrics in res['station_results'].items():
            final_station_report[station_id]["total_events"] += metrics["total_events"]
            final_station_report[station_id]["anomaly_events"] += metrics["anomaly_events"]
    
    end_time = time.perf_counter()
    return end_time - start_time

