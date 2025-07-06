# solution_message_broker/reducer.py
import pika
import json
from collections import defaultdict
from core.metrics import calculate_moving_averages, count_multi_sensor_anomaly_periods

def run_reducer(total_tasks: int):
    """
    Consumes enriched data from the aggregation queue, groups it,
    and performs the final calculations.
    """
    print("Reducer iniciado. Aguardando dados dos workers...")
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='aggregation_queue', durable=True)

    # In-memory storage to group data
    all_events = []
    
    tasks_done = 0
    for method_frame, properties, body in channel.consume('aggregation_queue'):
        all_events.append(json.loads(body))
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        tasks_done += 1
        if tasks_done == total_tasks:
            break
    
    connection.close()
    print("Reducer recebeu todos os dados. Iniciando cálculos finais...")

    # --- AGGREGATION LOGIC ---
    station_events = defaultdict(list)
    region_events = defaultdict(list)
    
    for event in all_events:
        station_events[event['station_id']].append(event)
        region_events[event['region']].append(event)

    final_report = {
        "station_metrics": defaultdict(dict),
        "region_metrics": defaultdict(dict)
    }

    # Calculate metrics per station
    for station_id, events in station_events.items():
        events.sort(key=lambda x: x['timestamp'])
        anomaly_events = [e for e in events if e['is_anomaly']]
        
        final_report["station_metrics"][station_id] = {
            "total_events": len(events),
            "anomaly_percentage": (len(anomaly_events) / len(events)) * 100 if events else 0,
            "multi_sensor_periods": count_multi_sensor_anomaly_periods(events)
        }

    # Calculate metrics per region
    for region, events in region_events.items():
        events.sort(key=lambda x: x['timestamp'])
        final_report["region_metrics"][region] = calculate_moving_averages(events, window_size=50)

    return final_report