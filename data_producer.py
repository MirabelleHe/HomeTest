import os
import pika

# Create a connection to the messaging queue.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')

# Send every csv or json file to the queue.
path = './resources'
table = 'invoices'
for file in os.listdir(path):
    file_type = file.split(".")[1]
    if file_type in ("csv", "json"):
        packet = path + "/" + file + ";" + file_type + ";" + table
        channel.basic_publish(exchange='',
                              routing_key='messaging_queue',
                              body=packet)
        print(file + " Sent.")

connection.close()
