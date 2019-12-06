from crime import Crime
from database import Database
import db_parser


drop_tables = True
initalize_tables = True
add_crime = False
add_pill = True

def main():
    db = Database()

    if drop_tables:
        db.drop_all_tables()

    if initalize_tables:
        queries = db_parser.transform_sql("./sql/create_tables.sql")
        db.query_all(queries)

    if add_crime:
        Crime.add_data(db)

    db.close_connection()



main()