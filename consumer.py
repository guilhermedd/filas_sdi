import pika
import time
import threading
import csv
import plotly.express as px
import pandas as pd


class Job:
    def __init__(self, exec_time, resources, priority, id, start_at):
        self.exec_time = int(exec_time)
        self.resources = int(resources)
        self.priority = int(priority)
        self.id = id
        self.start_at = start_at
        self.end_at = start_at + self.exec_time

    def __str__(self):
        return f"ID:{self.id};Exec time:{self.exec_time};Resources:{self.resources};Priority{self.priority};Start at{self.start_at};End at:{self.end_at}"


class Server:
    def __init__(self):
        self.MAX_RESOURCES = 200
        self.cur_used_res = 0
        self.id_job = 0
        self.scheduled = []

    def callback(self, ch, method, properties, body):
        exec_time, resources, priority = body.decode().split(';')
        resources = int(resources)

        # Can be scheduled
        if self.cur_used_res + resources > self.MAX_RESOURCES:
            print(f"There is not space to schedule the job with {resources} resources.")
            return

        job = Job(exec_time, resources, priority, self.id_job, time.time())
        self.scheduled.append(job)

        self.id_job += 1
        self.cur_used_res += resources

        thread = threading.Thread(target=self.handle_job, args=(job,))
        thread.start()

        print(f"Resources used: {self.cur_used_res} | Resources available: {self.MAX_RESOURCES - self.cur_used_res}")

        if len(self.scheduled) % 20 == 0:
            field_names = ["Task", "Start", "Finish", "Resources"]
            with open('jobs.csv', mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=field_names)

                writer.writeheader()
                for job in self.scheduled:
                    writer.writerow({
                        "Task": job.id,
                        "Start": job.start_at,
                        "Finish": job.end_at,
                        "Resources": job.resources
                    })

    def handle_job(self, job):
        time.sleep(job.exec_time)
        self.cur_used_res -= job.resources

    def start(self):
        QUEUE_LOW = 'fila_sdi_baixa'  # 0
        QUEUE_HIGH = 'fila_sdi_alta'  # 1

        # Conecta-se ao servidor RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # low_info = channel.queue_declare(queue=QUEUE_LOW)

        while True:
            # Priorizar Q_HIGH
            channel.basic_consume(queue=QUEUE_HIGH, on_message_callback=self.callback, auto_ack=True)
            high_info = channel.queue_declare(queue=QUEUE_HIGH, passive=True)

            if high_info.method.message_count == 0:
                channel.basic_consume(queue=QUEUE_LOW, on_message_callback=self.callback, auto_ack=True)

            print('Aguardando mensagens. Pressione CTRL+C para sair.')

            # Começa a consumir mensagens
            channel.start_consuming()


if __name__ == "__main__":
    server = Server()
    server.start()
