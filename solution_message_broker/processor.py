# # solution_message_broker/processor.py
# import pika
# import time
# import subprocess
# import os
# from .producer import run_producer
# from .reducer import run_reducer

# def run_analysis(data_path: str, num_workers: int):
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     queues = ['task_queue', 'result_queue', 'aggregation_queue']
#     for q in queues:
#         channel.queue_declare(queue=q, durable=True)
#         channel.queue_purge(queue=q)
#     connection.close()

#     workers = []
#     for _ in range(num_workers):
#         FNULL = open(os.devnull, 'w')
#         proc = subprocess.Popen(['python', '-m', 'solution_message_broker.worker'], stdout=FNULL, stderr=FNULL)
#         workers.append(proc)
    
#     time.sleep(2)

#     total_tasks = run_producer(data_path)
#     if total_tasks <= 0: return

#     print(f"Análise iniciada com {num_workers} worker(s)...")
#     start_time = time.perf_counter()

#     # Phase 1: Wait for all workers (mappers) to finish
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     tasks_done = 0
#     for _ in channel.consume('result_queue'):
#         tasks_done += 1
#         if tasks_done == total_tasks: break
    
#     print("Fase de Mapeamento (Workers) concluída.")
#     for proc in workers: proc.terminate()

#     # Phase 2: Run the reducer to get the final report
#     final_report = run_reducer(total_tasks)
    
#     end_time = time.perf_counter()
#     duration = end_time - start_time
    
#     # --- PRINT FINAL REPORT ---
#     print(f"\nAnálise distribuída completa em {duration:.4f} segundos.")
#     print("\n--- Relatório de Métricas por Estação ---")
#     for station_id, metrics in sorted(final_report["station_metrics"].items()):
#         print(f"\n[Estação ID: {station_id}] - Total: {metrics['total_events']} eventos")
#         print(f"  - Anomalias: {metrics['anomaly_percentage']:.2f}%")
#         print(f"  - Períodos de Anomalia Múltipla: {metrics['multi_sensor_periods']}")

#     print("\n--- Relatório de Média Móvel por Região ---")
#     for region, metrics in sorted(final_report["region_metrics"].items()):
#         print(f"\n[Região: {region}]")
#         print(f"  - Temperatura Média: {metrics.get('temperature', 0):.2f}°C")
#         print(f"  - Umidade Média: {metrics.get('humidity', 0):.2f}%")
#         print(f"  - Pressão Média: {metrics.get('pressure', 0):.2f} hPa")

# if __name__ == '__main__':
#     run_analysis("data/synthetic_data.csv", num_workers=4)



# solution_message_broker/processor.py
import pika
import time
import subprocess
import os
import json
from collections import defaultdict
from .producer import run_producer

def run_analysis(data_path: str, num_workers: int) -> float:
    """
    Orquestra a análise distribuída (modelo otimizado) e retorna o tempo total.
    """
    # 1. Setup inicial e limpeza das filas
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)
        channel.queue_declare(queue='result_queue', durable=True)
        channel.queue_purge(queue='task_queue')
        channel.queue_purge(queue='result_queue')
        connection.close()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Erro de conexão com RabbitMQ: {e}")
        print("Certifique-se de que os containers Docker estão rodando com 'docker-compose up -d'")
        return -1.0

    # 2. Inicia o producer PRIMEIRO para encher a fila de tarefas
    total_tasks = run_producer(data_path)
    if total_tasks <= 0: 
        print("Falha ao popular a fila. Verifique o gerador de dados e o producer.")
        return -1.0

    # 3. Mede o tempo a partir do momento em que os workers começam a trabalhar
    start_time = time.perf_counter()

    # Inicia os processos worker, que irão consumir da fila e se encerrar
    workers = []
    for _ in range(num_workers):
        # Esconde o output dos workers para não poluir o console do dashboard
        FNULL = open(os.devnull, 'w')
        proc = subprocess.Popen(['python', '-m', 'solution_message_broker.worker'], stdout=FNULL, stderr=FNULL)
        workers.append(proc)
    
    # Aguarda que os processos worker terminem seu trabalho
    for proc in workers:
        proc.wait(timeout=300) # Timeout de 5 minutos para evitar bloqueio infinito

    # 4. Coleta os resultados agregados (um por worker)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    worker_results = []
    try:
        # Tenta consumir da fila de resultados
        for method_frame, properties, body in channel.consume('result_queue', inactivity_timeout=5):
            if method_frame is None:
                break # Sai do loop se não houver mais mensagens
            worker_results.append(json.loads(body))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    finally:
        connection.close()
    
    # 5. Realiza a agregação final (muito rápida) dos resultados dos workers
    # (Embora não precisemos dos resultados para o benchmark, este passo simula o processo real)
    final_station_report = defaultdict(lambda: {"total_events": 0, "anomaly_events": 0, "multi_sensor_periods": 0})
    for res in worker_results:
        if res.get("station_metrics"):
            for station_id, metrics in res["station_metrics"].items():
                final_station_report[station_id]["total_events"] += metrics.get("total_events", 0)
                final_station_report[station_id]["anomaly_events"] += metrics.get("anomaly_events", 0)
                final_station_report[station_id]["multi_sensor_periods"] += metrics.get("multi_sensor_periods", 0)
    
    # 6. Finaliza a medição do tempo
    end_time = time.perf_counter()
    return end_time - start_time

if __name__ == '__main__':
    # Bloco para teste direto do script
    DATA_FILE = "../data/synthetic_data.csv"
    print("Iniciando teste direto do processador Message Broker com 4 workers...")
    execution_time = run_analysis(DATA_FILE, num_workers=4)
    print(f"Tempo de execução do teste direto: {execution_time:.4f} segundos.")