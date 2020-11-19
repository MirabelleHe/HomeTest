import json
import sqlite3
import pandas as pd
import json_parser


class dbHandler:
    def __init__(self, dbPath):
        self.path = dbPath
        self.conn = None

    def getPath(self):
        return self.path

    def connect(self):
        try:
            conn = sqlite3.connect(self.path)
            self.conn = conn  # .cursor()
            return self.conn
        except:
            print('connection error')
            return -1

    def disconnect(self):
        if self.conn:
            self.conn.close()
            print("disconnected from database")

    def select(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        rs = cur.fetchall()
        self.disconnect()
        return rs

    # Wrapper method for loading data to DB.
    def load(self, splitted_body):
        conn = dbHandler.connect(self)
        cur = self.conn.cursor()
        if splitted_body[1] == 'csv':
            data = pd.read_csv(splitted_body[0])
            df = pd.DataFrame(data, columns=['InvoiceId', 'CustomerId', 'InvoiceDate', 'BillingAddress', 'BillingCity',
                                             'BillingState', 'BillingCountry', 'BillingPostalCode', 'Total'])
            self.load_csv(df, conn, cur)
        elif splitted_body[1] == 'json':
            self.load_json(splitted_body[0], conn, cur)

    def load_csv(self, df, conn, cur):
        for row in df.itertuples():
            statement = '''INSERT INTO invoices(InvoiceId, CustomerId, InvoiceDate, BillingAddress,
            BillingCity, BillingState, BillingCountry, BillingPostalCode, Total)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            values = (row.InvoiceId, row.CustomerId, row.InvoiceDate, row.BillingAddress, row.BillingCity,
                      row.BillingState, row.BillingCountry, row.BillingPostalCode, row.Total)
            cur.execute(statement, values)
        conn.commit()

    def load_json(self, data, conn, cur):
        with open(data, 'r') as f:
            json_data = json.loads(f.read())
            for row in json_data:
                key_list, value_list = json_parser.parse_row(row)
                statement = "INSERT INTO invoices " + key_list + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);"
                cur.execute(statement, value_list)
            conn.commit()
