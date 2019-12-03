from crime import Crime
from database import Database
import db_parser
from pill import Pill

initalize_tables = True
add_crime = True
add_pill = True

def main():
    db = Database()

    db.drop_all_tables()

    if initalize_tables:
        queries = db_parser.transform_sql("./sql/create_tables.sql")
        db.query_all(queries)

    if add_crime:
        Crime.add_data(db)

    if add_pill:
        Pill.add_data(db)

    db.close_connection()



main()