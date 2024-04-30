#Server
import pika

QUEUE_NAME_LOW = 'fila_sdi_baixa'
QUEUE_NAME_HIGH = 'fila_sdi_alta'

# Conecta-se ao servidor RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Cria uma fila chamada 'fila_teste'
channel.queue_declare(queue=QUEUE_NAME_LOW)
channel.queue_declare(queue=QUEUE_NAME_HIGH)

# Função de callback para processar mensagens recebidas
def callback(ch, method, properties, body):
    print("Mensagem recebida:", body.decode())

# Registra a função de callback para a fila
channel.basic_consume(queue=QUEUE_NAME_LOW, on_message_callback=callback, auto_ack=True)
channel.basic_consume(queue=QUEUE_NAME_HIGH, on_message_callback=callback, auto_ack=True)


print('Aguardando mensagens. Pressione CTRL+C para sair.')

# Começa a consumir mensagens
channel.start_consuming()
