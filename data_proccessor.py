import os
import pika
import sys
import dbHandler


def main():
    # Create a connection to the messaging queue and the graph queue.
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='messaging_queue')
    channel.queue_declare(queue='graph_queue')

    # This method will be triggered after receiving a message with a new fie to load to the DB.
    def db_callback(ch, method, properties, body):
        str_body = body.decode("utf-8")
        splitted_body = str_body.split(";")
        print("Received %r" % splitted_body[0].split("/")[-1])

        # Load data to DB.
        db = dbHandler.dbHandler('./resources/homeTest.db')
        db.load(splitted_body)
        print("Loaded to table: %r" % splitted_body[2])

        # Send message to the graph queue that new data was loaded.
        channel.basic_publish(exchange='',
                              routing_key='graph_queue',
                              body=splitted_body[2])

    # Receive a message on the messaging queue.
    channel.basic_consume(queue='messaging_queue',
                          auto_ack=True,
                          on_message_callback=db_callback)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
