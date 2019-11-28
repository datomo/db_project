import mysql.connector
from sshtunnel import SSHTunnelForwarder

# ssh variables
host = '134.209.234.163'
localhost = '127.0.0.1'
ssh_username = 'root'
ssh_password = 'db_project2019'

# database variables
user = 'admin'
password = 'd7730e905d7c2a007371051d2060cc8d9de4ba9230e7bfc8'
database = 'db_project'


class Database:
    def __init__(self):
        self.open_connection()

    def open_connection(self):
        self.tunnel = SSHTunnelForwarder(
            (host, 22),
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=("localhost", 3306),
            local_bind_address=("localhost", 3306)
        )
        self.tunnel.start()
        self.db = mysql.connector.connect(
            host="localhost",
            user=user,
            passwd=password,
            database=database
        )

    def query(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        cursor.close()

    def query_all(self, queries):
        cursor = self.db.cursor()
        for query in queries:
            cursor.execute(query)
        cursor.close()

    def close_connection(self):
        self.db.close()
        self.tunnel.stop()

'''def main():
    db = Database()
    db.query("CREATE TABLE test (nummer INTEGER, PRIMARY KEY (nummer))")
    db.close_connection()


main()'''
