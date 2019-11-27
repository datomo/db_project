import mysql.connector
import sshtunnel

with sshtunnel.open_tunnel(
    ('134.209.234.163', 22),
    ssh_username="root",
    ssh_password="db_project2019",
    remote_bind_address=('127.0.0.1', 3306)
) as tunnel:
    print("finished")
    mydb = mysql.connector.connect(
        host="localhost",
        user="admin",
        passwd="d7730e905d7c2a007371051d2060cc8d9de4ba9230e7bfc8",
        db="mysql",
        port='3306'
    )

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE mydatabase")

tunnel.close()
