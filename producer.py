# Cliente
import pika
import time
import random

QUEUE_LOW = 'fila_sdi_baixa'  # 0
QUEUE_HIGH = 'fila_sdi_alta'  # 1

MAX_TIME = 20
MAX_RESOURCES = 10

# msg = time, resources, priority

# Conecta-se ao servidor RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_LOW)
channel.queue_declare(queue=QUEUE_HIGH)

oi = 1 if 1 == 1 else 2

while True:
    try:
        execution_time = random.randint(1, MAX_TIME)
        resources = random.randint(1, MAX_RESOURCES)
        priority = random.randint(0, 1)

        msg = f"{execution_time};{resources};{priority}"

        # Envia uma mensagem para a fila
        channel.basic_publish(exchange='', routing_key=QUEUE_LOW if priority == 0 else QUEUE_HIGH, body=msg)
        print(f"Mensagem enviada: {msg}")

        time.sleep(0.1)
        
    except Exception:
        print("Falha ao enviar mensagem.")
        try:
            connection.close()
        except  Exception as e:
            print("Falha ao fechar a conexão.")
        break
