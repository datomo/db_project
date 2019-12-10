import mysql.connector
from sshtunnel import SSHTunnelForwarder

# ssh variables
host = '134.209.234.163'
localhost = '127.0.0.1'
ssh_username = 'root'
ssh_password = 'db_project2019'

# database variables
user = 'root'
password = 'd7730e905d7c2a007371051d2060cc8d9de4ba9230e7bfc8'
database = 'db_multiprocess'


class Database:
    def __init__(self):
        self.open_connection()
        self.cursor = self.db.cursor()
        self.query("SET GLOBAL max_allowed_packet=1073741824")
        self.query("SET autocommit = 0")
        self.query("SET FOREIGN_KEY_CHECKS=0;")
        self.buffered_cursor = self.db.cursor(buffered=True)

    def open_connection(self):
        ''' self.tunnel = SSHTunnelForwarder(
            (host, 22),
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=("localhost", 3306),
            local_bind_address=("localhost", 3306)
        )
        self.tunnel.start()'''
        self.db = mysql.connector.connect(
            host="localhost",
            port="3306",
            user=user,
            passwd=ssh_password,
            database=database,
            connect_timeout=28800,
            allow_local_infile=True
        )

    def commit(self):
        self.db.commit()

    def query(self, query):
        try:
            self.cursor.execute(query)
            self.db.commit()
        except mysql.connector.Error as e:
            print("Something went wrong: {}".format(e))
            print(query)

    def querymany(self, query, data_list):
        try:
            self.cursor.executemany(query, data_list)
            self.db.commit()
        except mysql.connector.Error as e:
            print("Something went wrong: {}".format(e))
            print(query)

    def exists(self, table, statments):
        query = "SELECT (1) FROM {} WHERE {}".format(table, " AND ".join(statments))
        res = self.select_one(query)
        if res:
            return True
        else:
            return False

    def select(self, query):
        self.cursor.execute(query)
        fetch = self.cursor.fetchall()
        return fetch

    def select_one(self, query):
        self.buffered_cursor.execute(query)
        fetch = self.buffered_cursor.fetchone()

        return fetch

    def query_all(self, queries):
        for query in queries:
            self.query(query)

    def drop_table(self, table):
        self.query("DROP TABLE IF EXISTS {}".format(table))

    def drop_all_tables(self):
        tables = ["Address", "Drug", "Crime", "Review", "Report", "Business", "Add_Business_Info", "is_located",
                  "rates_a", "occured_at", "specifies", "reports", "has"]
        for table in tables:
            self.drop_table(table)

    def close_connection(self):
        self.cursor.close()
        self.buffered_cursor.close()
        self.db.close()
        # self.tunnel.stop()


'''def main():
    db = Database()
    db.query("CREATE TABLE test (nummer INTEGER, PRIMARY KEY (nummer))")
    db.close_connection()


main()'''
