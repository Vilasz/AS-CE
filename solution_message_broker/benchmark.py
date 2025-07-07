import pika
import time
import subprocess
import os
import json
from collections import defaultdict
from .producer import run_producer

def run_single_test(data_path: str, num_workers: int) -> float:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue', durable=True)
    channel.queue_purge(queue='task_queue')
    channel.queue_purge(queue='result_queue')
    connection.close()

    total_tasks = run_producer(data_path)
    if total_tasks <= 0: return -1.0
    print(f"Teste com {num_workers} worker(s): {total_tasks} tarefas na fila.")

    start_time = time.perf_counter()

    workers = []
    for _ in range(num_workers):
        proc = subprocess.Popen(['python', '-m', 'solution_message_broker.worker'])
        workers.append(proc)
    
    for proc in workers:
        proc.wait()

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    worker_results = []
    for method_frame, properties, body in channel.consume('result_queue', inactivity_timeout=3):
        if method_frame is None: break
        worker_results.append(json.loads(body))
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    
    final_station_report = defaultdict(lambda: {"total_events": 0, "anomaly_events": 0, "multi_sensor_periods": 0})
    for res in worker_results:
        for station_id, metrics in res["station_metrics"].items():
            final_station_report[station_id]["total_events"] += metrics["total_events"]
            final_station_report[station_id]["anomaly_events"] += metrics["anomaly_events"]
            final_station_report[station_id]["multi_sensor_periods"] += metrics["multi_sensor_periods"]
    
    end_time = time.perf_counter()
    return end_time - start_time

if __name__ == '__main__':
    DATA_FILE = "data/synthetic_data.csv"
    WORKER_COUNTS = [1, 2, 4, 8]
    print("Iniciando benchmark da solução OTIMIZADA com Message Broker...")
    results = {}
    for workers in WORKER_COUNTS:
        if workers > os.cpu_count():
            print(f"Pulando teste com {workers} workers (Máximo de CPUs: {os.cpu_count()}).")
            continue
        duration = run_single_test(DATA_FILE, num_workers=workers)
        if duration > 0: results[workers] = duration
        print(f"  -> Concluído em {duration:.4f} segundos.")
    print("\n--- Resultados do Benchmark (Otimizado) ---")
    base_time = results.get(1, 1.0)
    for workers, duration in results.items():
        speedup = base_time / duration
        print(f"Workers: {workers} | Tempo: {duration:.4f} seg | Speedup: {speedup:.2f}x")