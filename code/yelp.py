import json

import db_parser
from database import Database
import math
import time
import ijson


class Yelp:
    @staticmethod
    def add_data(db: Database):
        queries = db_parser.transform_sql("./sql/create_tables_review.sql")
        db.query_all(queries)
        db.query("ALTER TABLE Review CHANGE text text TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        file_path = "../data/yelp_academic_dataset_review.json"

        lines = sum(1 for i in open(file_path, 'rb'))
        print(lines)


        with open(file_path, encoding="utf8", mode="r") as file:
            chunk = 200000
            chunk_amount = math.ceil(float(lines) / chunk)
            print("{}".format(chunk_amount))


            start_time = time.time()

            for a_chunk in range(chunk_amount):
                output = []
                j = 0
                for line in file:

                    output.append(line)
                    j += 1

                    if j >= chunk:
                        break

                # parse_cols(output, db)
                # print("executed {} rows from {}: {}%".format(i, lines, round(i / lines * 100, 2)))
                # break
                ## hand to processe
                print("WORKING ON NEW CHUNK {}".format(a_chunk))
                start = time.time()
                Yelp.parse_cols(output, db)
                print("{}s part".format(time.time() - start))
            db.query("ALTER TABLE Review ADD INDEX comp(business_id) ")

    @staticmethod
    def parse_cols(cols: [], db: Database):
        r_query = "INSERT INTO Review " \
                  "VALUES (%(review_id)s,%(user_id)s, %(business_id)s, %(date)s, %(text)s, %(cool)s, %(funny)s, %(useful)s, %(stars)s)"
        r_data = []
        for col in cols:
            col = json.loads(col)
            #if len(col) > 9 or len(col) < 9: print(len(col))
            r_data.append(col)
        db.querymany(r_query, r_data)

    @staticmethod
    def add_data_business(db: Database):
        file_path = "../data/yelp_academic_dataset_business.json"

        lines = sum(1 for i in open(file_path, 'rb'))
        print(lines)
        with open(file_path, encoding="utf8", mode="r") as file:
            chunk = 200000
            chunk_amount = math.ceil(float(lines) / chunk)
            print("{}".format(chunk_amount))

            start_time = time.time()

            for a_chunk in range(chunk_amount):
                output = []
                j = 0
                for line in file:

                    output.append(line)
                    j += 1

                    if j >= chunk:
                        break

                print("WORKING ON NEW CHUNK {}".format(a_chunk))
                start = time.time()
                Yelp.parse_cols_business(output, db)
                print("{}s part".format(time.time() - start))

    @staticmethod
    def get_postal_codes(cols: [], db: Database):
        postal_codes = set()
        for col in cols:
            zip = col.split('postal_code":"')[1].split('"', 1)[0]
            if all(i.isdigit() for i in zip):
                postal_codes.add(zip)
        return postal_codes

'''
if __name__ == '__main__':
    db = Database()
    Yelp.add_data(db)'''