from crime import Crime
from database import Database
import db_parser


drop_tables = False
initalize_tables = False
add_crime = True
add_crime_rel = True

def main():
    db = Database()

    if drop_tables:
        db.drop_all_tables()

    if initalize_tables:
        queries = db_parser.transform_sql("./sql/create_tables.sql")
        db.query_all(queries)

    if add_crime:
        Crime.add_data(db)

    if add_crime_rel:
        Crime.add_rel_data(db)

    db.close_connection()



main()