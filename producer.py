# Cliente
import pika

QUEUE_NAME_LOW = 'fila_sdi_baixa'
QUEUE_NAME_HIGH = 'fila_sdi_alta'

# Conecta-se ao servidor RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declara a fila 'fila_teste'
channel.queue_declare(queue=QUEUE_NAME_LOW)
channel.queue_declare(queue=QUEUE_NAME_HIGH)

# Envia uma mensagem para a fila
channel.basic_publish(exchange='', routing_key=QUEUE_NAME_LOW, body='Olá, mundo_1!')
channel.basic_publish(exchange='', routing_key=QUEUE_NAME_HIGH, body='Olá, mundo_2!')


print("Mensagem enviada.")

# Fecha a conexão
connection.close()
