
# import pika
# import csv
# import json

# def run_producer(data_path: str):
#     """
#     Lê dados de um CSV e publica cada linha como uma mensagem em uma fila do RabbitMQ.
#     """
#     try:
#         # Conecta-se ao RabbitMQ (rodando via Docker)
#         connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#         channel = connection.channel()

#         # Cria a fila. 'durable=True' garante que a fila sobreviva a uma reinicialização do RabbitMQ.
#         channel.queue_declare(queue='task_queue', durable=True)
#         print("Producer conectado ao RabbitMQ e pronto para enviar mensagens.")

#         with open(data_path, 'r', newline='') as csvfile:
#             reader = csv.DictReader(csvfile)
#             for row in reader:
#                 # Converte a linha do CSV para uma string JSON
#                 message_body = json.dumps(row)
                
#                 channel.basic_publish(
#                     exchange='',
#                     routing_key='task_queue', # O nome da fila
#                     body=message_body,
#                     properties=pika.BasicProperties(
#                         delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE # Torna a mensagem persistente
#                     ))
#                 # print(f" [x] Enviado: {row['timestamp']}") # Descomente para depuração

#         print(f"Todas as mensagens do arquivo {data_path} foram enviadas.")
#         connection.close()

#     except pika.exceptions.AMQPConnectionError:
#         print("Erro: Não foi possível conectar ao RabbitMQ. Verifique se o container está rodando.")
#     except FileNotFoundError:
#         print(f"Erro: Arquivo de dados não encontrado em {data_path}.")

# if __name__ == '__main__':
#     DATA_FILE = "data/synthetic_data.csv"
#     run_producer(DATA_FILE)



# solution_message_broker/producer.py
import pika
import csv
import json

def run_producer(data_path: str) -> int:
    message_count = 0
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)

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
        connection.close()
        return message_count
    except (pika.exceptions.AMQPConnectionError, FileNotFoundError):
        return -1