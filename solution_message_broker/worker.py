import pika
import json
import os
import sys
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.metrics import is_anomalous, calculate_moving_averages, count_multi_sensor_anomaly_periods

def main():
    worker_id = os.getpid()
    print(f"[*] Aggregating Worker {worker_id}: Iniciando.")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)
        channel.queue_declare(queue='result_queue', durable=True)
        
        station_events = defaultdict(list)
        
        # Consome o máximo de mensagens que conseguir da fila
        for method_frame, properties, body in channel.consume('task_queue', inactivity_timeout=3):
            if method_frame is None: break
            
            event = json.loads(body)
            try:
                # Converte tipos de dados
                event['station_id'] = int(event['station_id'])
                event['temperature'] = float(event['temperature'])
                event['humidity'] = float(event['humidity'])
                event['pressure'] = float(event['pressure'])
                station_events[event['station_id']].append(event)
            except (ValueError, KeyError):
                pass 
            finally:
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        
        print(f"[*] Worker {worker_id}: Mensagens consumidas. Agregando localmente...")
        
        worker_station_report = {}
        worker_region_report = defaultdict(list)
        worker_found_anomalies = []

        for station_id, events in station_events.items():
            events.sort(key=lambda x: x['timestamp'])
            region_name = events[0]['region']
            anomalies_in_station = []

            for event in events:
                anomaly_found, sensor = is_anomalous(event)
                if anomaly_found:
                    anomalies_in_station.append(event)
                    worker_found_anomalies.append({
                        "timestamp": event['timestamp'],
                        "station_id": event['station_id'],
                        "sensor": sensor
                    })
                else:
                    worker_region_report[region_name].append(event)
            
            worker_station_report[station_id] = {
                "total_events": len(events),
                "anomaly_events": len(anomalies_in_station),
                "multi_sensor_periods": count_multi_sensor_anomaly_periods(events)
            }
        
        final_result = {
            "station_metrics": worker_station_report,
            "found_anomalies": worker_found_anomalies
        }
        channel.basic_publish(
            exchange='',
            routing_key='result_queue',
            body=json.dumps(final_result)
        )
        
        connection.close()
        print(f"[*] Worker {worker_id}: Resultado agregado enviado. Encerrando.")
    except Exception as e:
        print(f"Worker {worker_id} Error: {e}")

if __name__ == '__main__':
    main()