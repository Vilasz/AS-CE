# solution_multiprocessing/processor.py

import multiprocessing
from collections import defaultdict
from .metrics import is_anomalous, calculate_moving_averages, count_multi_sensor_anomaly_periods
from .data_parser import load_and_group_by_station, load_and_group_by_region

# --- Funções Worker ---

def process_station_chunk(station_data: tuple[int, list[dict]]) -> dict:
    """
    Worker que agora calcula AMBAS as métricas por estação:
    1. Percentual de anomalias.
    2. Contagem de períodos com anomalias em múltiplos sensores.
    """
    station_id, event_list = station_data
    
    # Ordena os eventos por tempo, crucial para a nova métrica
    event_list.sort(key=lambda x: x['timestamp'])

    # Cálculo do percentual de anomalias (como antes)
    total_events = len(event_list)
    anomaly_counts = defaultdict(int)
    for event in event_list:
        anomaly_found, sensor = is_anomalous(event)
        if anomaly_found:
            anomaly_counts[sensor] += 1
    
    # NOVO: Cálculo dos períodos de anomalia múltipla
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


# --- Orquestrador Principal ---

def run_analysis(data_path: str):
    """
    Executa a análise completa para todas as 3 métricas.
    """
    print("Iniciando análise completa com multiprocessing...")

    # --- Análises por Estação ---
    station_groups = load_and_group_by_station(data_path)
    if not station_groups:
        print(f"Erro: Arquivo não encontrado em {data_path} ou está vazio.")
        print("Por favor, gere os dados primeiro: python data_generator/generator.py")
        return
    
    with multiprocessing.Pool() as pool:
        station_results = pool.map(process_station_chunk, station_groups.items())

    final_station_report = {k: v for d in station_results for k, v in d.items()}

    print("\n--- Relatório de Anomalias por Estação ---")
    for station_id, report in sorted(final_station_report.items()):
        print(f"\n[Estação ID: {station_id}] - Total de Eventos: {report['total_events']}")
        # Imprime o resultado da nova métrica
        print(f"  Períodos com anomalias em múltiplos sensores: {report['multi_sensor_anomaly_periods']}")
        
        if not report['anomaly_counts']:
            print("  Nenhuma anomalia percentual detectada.")
            continue
        for sensor, count in report['anomaly_counts'].items():
            percentage = (count / report['total_events']) * 100
            print(f"  - {sensor.capitalize()}: {count} anomalias ({percentage:.2f}%)")

    # --- Análise por Região ---
    region_groups = load_and_group_by_region(data_path)
    with multiprocessing.Pool() as pool:
        region_results = pool.map(process_region_chunk, region_groups.items())
    final_region_report = {k: v for d in region_results for k, v in d.items()}

    print("\n\n--- Relatório de Média Móvel por Região ---")
    for region, averages in sorted(final_region_report.items()):
        print(f"\n[Região: {region}]")
        print(f"  - Temperatura Média: {averages['temperature']:.2f} °C")
        print(f"  - Umidade Média: {averages['humidity']:.2f} %")
        print(f"  - Pressão Média: {averages['pressure']:.2f} hPa")

    print("\nAnálise completa.")


if __name__ == "__main__":
    DATA_FILE = "data/synthetic_data.csv"
    run_analysis(DATA_FILE)