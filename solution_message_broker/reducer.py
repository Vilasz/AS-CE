import pika
import json
import os
import sys
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.metrics import calculate_moving_averages, count_multi_sensor_anomaly_periods

ANOMALY_OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'broker_found_anomalies.json')

def main():
    reducer_id = os.getpid()
    print(f"[*] Reducer {reducer_id}: Iniciando.")

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='results_queue', durable=True)
        
        station_events = defaultdict(list)
        region_events = defaultdict(list)
        
        print(f"[*] Reducer {reducer_id}: Consumindo dados da 'results_queue'...")
        
        for method_frame, properties, body in channel.consume('results_queue', inactivity_timeout=10):
            if method_frame is None: break
            
            event = json.loads(body)
            station_events[event['station_id']].append(event)
            
            if not event['is_anomaly']:
                 region_events[event['region']].append(event)
            
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        print(f"[*] Reducer {reducer_id}: Dados recebidos. Iniciando agregação...")
        
        final_report = { "station_metrics": {}, "region_metrics": {} }
        all_found_anomalies = []

        for station_id, events in station_events.items():
            events.sort(key=lambda x: x['timestamp'])
            
            # Coleta as anomalias desta estação
            station_anomalies = [e for e in events if e.get('is_anomaly')]
            all_found_anomalies.extend(station_anomalies)

            final_report["station_metrics"][station_id] = {
                'anomaly_percentage': (len(station_anomalies) / len(events)) * 100 if events else 0,
                'multi_sensor_periods': count_multi_sensor_anomaly_periods(events)
            }
        
        for region, events in region_events.items():
            events.sort(key=lambda x: x['timestamp'])
            final_report["region_metrics"][region] = calculate_moving_averages(events, window_size=50)

        # Salva o relatório completo para análise posterior
        with open('data/final_report_broker.json', 'w') as f:
            json.dump(final_report, f, indent=4)

        # Apenas os campos necessários para a verificação de corretude
        anomaly_list_for_processor = [
            {"timestamp": anom['timestamp'], "station_id": anom['station_id'], "sensor": anom['anomaly_sensor']}
            for anom in all_found_anomalies
        ]
        with open(ANOMALY_OUTPUT_FILE, 'w') as f:
            json.dump(anomaly_list_for_processor, f)

        print(f"[*] Reducer {reducer_id}: Relatório e anomalias salvas. Encerrando.")
        connection.close()

    except Exception as e:
        print(f"Reducer {reducer_id}: Erro fatal: {e}")
        # Garante que um arquivo vazio seja criado em caso de erro para não quebrar o processor
        if not os.path.exists(ANOMALY_OUTPUT_FILE):
            with open(ANOMALY_OUTPUT_FILE, 'w') as f:
                json.dump([], f)

if __name__ == '__main__':
    main()