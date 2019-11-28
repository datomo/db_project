from database import Database
from parser import Parser

initalize_tables = False

def main():
    db = Database()

    if initalize_tables:
        queries = Parser.transform_sql("./sql/create_tables.sql")
        db.query_all(queries)



    db.close_connection()



main()