from sshtunnel import SSHTunnelForwarder
import mysql.connector

# ssh variables
host = '134.209.234.163'
localhost = '127.0.0.1'
ssh_username = 'root'
ssh_password = 'db_project2019'

# database variables
user='admin'
password='d7730e905d7c2a007371051d2060cc8d9de4ba9230e7bfc8'
database='db_project'


def query(q):
     with SSHTunnelForwarder(
          (host, 22),
          ssh_username=ssh_username,
          ssh_password=ssh_password,
          remote_bind_address=("localhost", 3306),
          local_bind_address=("localhost", 3306)
    ) as tunnel:
        mydb = mysql.connector.connect(
            host="localhost",
            user=user,
            passwd=password,
            database=database
        )

        cursor = mydb.cursor()
        cursor.execute("CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))") 
        cursor.close()
        mydb.close()
        tunnel.stop()
    

query("")