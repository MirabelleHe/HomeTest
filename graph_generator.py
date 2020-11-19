import os
import sys
import pika
import dbHandler
import pandas as pd
import plotly.express as px


def main():
    # Create a connection to the graph queue.
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='graph_queue')

    # This method will be triggered after receiving a message that new data was loaded to the DB.
    def graph_callback(ch, method, properties, body):
        str_body = body.decode("utf-8")
        print("Starting to generate the graph of table: %r" % str_body)
        # Query the data from the DB.
        db = dbHandler.dbHandler('./resources/homeTest.db')
        conn = db.connect()
        df = pd.read_sql_query("SELECT strftime('%Y', InvoiceDate) AS year, strftime('%m', InvoiceDate) AS month, "
                               "Total, count(CustomerId) as customers FROM invoices GROUP BY year, month;", conn)
        # Render Graph
        fig = px.bar(df, x="month", y="Total", color="customers", hover_data=['customers'], barmode="group",
                     facet_col="year")
        fig.show()

    # Receive a message on the graph queue.
    channel.basic_consume(queue='graph_queue',
                          auto_ack=True,
                          on_message_callback=graph_callback)

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
