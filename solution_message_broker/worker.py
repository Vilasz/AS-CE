# solution_message_broker/worker.py
import pika
import json
import os
from collections import defaultdict
from core.metrics import calculate_moving_averages, count_multi_sensor_anomaly_periods, is_anomalous

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue', durable=True)
    
    worker_id = os.getpid()
    print(f' [*] Worker (Aggregator) {worker_id} aguardando por mensagens.')

    # Estruturas de dados em memória para agregação local
    station_events = defaultdict(list)
    region_events = defaultdict(list)
    
    # Consome todas as mensagens disponíveis na fila
    for method_frame, properties, body in channel.consume('task_queue', inactivity_timeout=1):
        if method_frame is None: # Fila vazia, encerra o consumo
            break

        event = json.loads(body)
        try:
            event['temperature'] = float(event['temperature'])
            event['humidity'] = float(event['humidity'])
            event['pressure'] = float(event['pressure'])
            event['station_id'] = int(event['station_id'])
        except (ValueError, KeyError):
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            continue
        
        # Agrega eventos em memória
        station_events[event['station_id']].append(event)
        region_events[event['region']].append(event)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    print(f" [+] Worker {worker_id} processou suas mensagens. Calculando agregados...")

    # Agora, o worker calcula as métricas para os dados que ele processou
    worker_final_result = {
        "station_metrics": defaultdict(dict),
        "region_metrics": defaultdict(dict)
    }

    for station_id, events in station_events.items():
        events.sort(key=lambda x: x['timestamp'])
        anomaly_found = [is_anomalous(e)[0] for e in events]
        
        worker_final_result["station_metrics"][station_id] = {
            "total_events": len(events),
            "anomaly_events": sum(anomaly_found),
            "multi_sensor_periods": count_multi_sensor_anomaly_periods(events)
        }

    for region, events in region_events.items():
        events.sort(key=lambda x: x['timestamp'])
        worker_final_result["region_metrics"][region] = calculate_moving_averages(events, window_size=50)

    # Envia UM ÚNICO resultado agregado para a fila de resultados
    channel.basic_publish(
        exchange='',
        routing_key='result_queue',
        body=json.dumps(worker_final_result)
    )

    connection.close()
    print(f" [+] Worker {worker_id} enviou seu resultado agregado.")


if __name__ == '__main__':
    main()