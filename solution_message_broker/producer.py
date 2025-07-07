import pika
import csv
import json

def run_producer(data_path: str) -> int:
    """
    Lê o arquivo CSV e publica cada linha como uma mensagem na fila de tarefas.
    """
    message_count = 0
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Garante que a fila de tarefas exista
        channel.queue_declare(queue='task_queue', durable=True)
        print(f"Producer: Conectado e publicando para a 'task_queue'...")

        with open(data_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                message_body = json.dumps(row)
                channel.basic_publish(
                    exchange='',
                    routing_key='task_queue',
                    body=message_body,
                    properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
                )
                message_count += 1
        
        print(f"Producer: {message_count} mensagens publicadas.")
        connection.close()
        return message_count
    except (pika.exceptions.AMQPConnectionError, FileNotFoundError) as e:
        print(f"Producer Error: {e}")
        return -1