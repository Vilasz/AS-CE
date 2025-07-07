import pika
import time
import subprocess
import os
import json
from collections import defaultdict
from .producer import run_producer

def run_analysis(data_path: str, num_workers: int) -> tuple[float, list]:
    """
    Orquestra a análise distribuída com o padrão de workers agregadores (otimizado).
    """
    print("\n--- Iniciando Análise com Message Broker (Otimizado) ---")
    
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)
        channel.queue_declare(queue='result_queue', durable=True)
        channel.queue_purge(queue='task_queue')
        channel.queue_purge(queue='result_queue')
        connection.close()
    except Exception as e:
        print(f"Erro de conexão no Setup: {e}")
        return -1.0, []

    total_tasks = run_producer(data_path)
    if total_tasks <= 0: return -1.0, []
    
    start_time = time.perf_counter()

    workers = []
    for _ in range(num_workers):
        proc = subprocess.Popen(['python', '-m', 'solution_message_broker.worker'])
        workers.append(proc)
    
    for proc in workers:
        proc.wait(timeout=300)

    all_found_anomalies = []
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    for method_frame, properties, body in channel.consume('result_queue', inactivity_timeout=5):
        if method_frame is None: break
        result = json.loads(body)
        if result.get("found_anomalies"):
            all_found_anomalies.extend(result["found_anomalies"])
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    
    connection.close()
    end_time = time.perf_counter()
    
    return (end_time - start_time), all_found_anomalies

if __name__ == '__main__':
    DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'synthetic_data.csv')
    run_analysis(DATA_FILE, num_workers=4)