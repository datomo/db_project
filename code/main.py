import db_parser
from crime import Crime
from database import Database
from pill import Pill
from pill_relations import Pill_relations
from yelp import Yelp

drop_tables = True
initalize_tables = True
add_pill = True
add_pill_rel = True
add_crime = True
add_crime_rel = True
add_yelp = True


def main():
    db = Database()

    if drop_tables:
        db.drop_all_tables()

    if initalize_tables:
        queries = db_parser.transform_sql("./sql/create_tables.sql")
        db.query_all(queries)

    if add_pill:
        Pill.add_data(db)

    if add_pill_rel:
        Pill_relations.add_data(db)

    if add_crime:
        Crime.add_data(db)

    if add_crime_rel:
        Crime.add_rel_data(db)

    if add_yelp:
        Yelp.add_data_business(db)

    db.close_connection()


main()
