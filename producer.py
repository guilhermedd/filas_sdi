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

while True:
    auto = int(input("DO you want to create your own jobs (1) or let them be created automatically (2)?\n"))
    while auto > 2 or auto < 1:
        auto = int(input("DO you want to create your own jobs (1) or let them be created automatically (2)?\n"))
    try:
        if auto == 2:
            execution_time = random.randint(1, MAX_TIME)
            resources = random.randint(1, MAX_RESOURCES)
            priority = random.randint(0, 1)
        else:
            execution_time = int(input(f"Execution time (max: {MAX_TIME}):\n"))
            resources = int(input(f"Resources (max: {MAX_RESOURCES}):\n"))
            priority = int(input("Priority (1 for high and 0 for low):\n"))

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
