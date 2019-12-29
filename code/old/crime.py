from itertools import islice

import db_parser
from database import Database
import time


class Crime:
    # 21833 error for inc_num deleted "-"
    @staticmethod
    def add_data(db: Database):
        queries = db_parser.transform_sql("./sql/create_table_occurred_at.sql")
        db.query_all(queries)
        file_path = "../data/crime-data_crime-data_crimestat.csv"
        i = 1

        lines = sum(1 for i in open(file_path, 'rb'))
        print("number of columns: {}".format(lines))

        with open(file_path, 'r') as file:
            chunk = 100000

            a_query = "INSERT IGNORE INTO Address (zip, city, street, street_number, county, state) VALUES( " \
                      "%(zip)s, " \
                      "%(city)s, " \
                      "%(street)s, " \
                      "%(number)s, " \
                      "%(county)s, " \
                      "%(state)s)"
            # some crimes are doubled in the source file
            c_query = "INSERT IGNORE INTO Crime (inc_number, premise_type, occurred_on, occurred_to, ucr_crime_category) VALUES(" \
                      "%(inc)s," \
                      "%(premise)s," \
                      "%(on)s," \
                      "%(to)s," \
                      "%(cat)s)"
            c_data = []
            a_data = []

            db.query("ALTER TABLE Address ADD UNIQUE KEY address_id (zip, street, street_number);")

            start = time.time()

            for line in islice(file, 1, None):
                data = line[:-1].replace('"', '').split(",")
                data = [None if x == "" else x for x in data]
                words = data[4].split(" ")
                words = [' '.join(words[:2]), ' '.join(words[-2:])]

                executed = False

                # exists = db.exists("Address", {"'zip' = {}".format(data[5]), "'street' = '{}'".format(words[1]), "'street_number' = '{}'".format(words[0])})

                if i % chunk == 0:
                    end = time.time()
                    print("time passed: {}".format(round(end - start, 3)))
                    print("executed {} rows from {}: {}%".format(i, lines, round(i / lines * 100, 2)))

                    print("{}".format(len(a_data)))
                    a_data = [dict(t) for t in {tuple(d.items()) for d in a_data}]
                    print("{}".format(len(a_data)))

                    db.querymany(c_query, c_data)
                    db.querymany(a_query, a_data)

                    c_data = []
                    a_data = []

                    executed = True
                    start = time.time()

                else:
                    zip = data[5] if data[5] else "000000"
                    # address additional values
                    # if not id:
                    a_data.append({
                        'zip': zip,
                        'street': words[1],
                        'number': words[0],
                        'city': 'PHEONIX',
                        'county': 'MARICOPA',
                        'state': 'AZ',
                    })

                c_data.append({
                    'inc': data[0],
                    'premise': data[6],
                    'on': data[1],
                    'to': data[2],
                    'cat': data[3]
                })

                i += 1

            if not executed:
                db.querymany(c_query, c_data)
                db.querymany(a_query, a_data)
                print("finished...")
        db.query("ALTER TABLE Crime DROP PRIMARY KEY, ADD COLUMN crime_id BIGINT(15) PRIMARY KEY AUTO_INCREMENT, ADD INDEX comp(inc_number, ucr_crime_category, premise_type) ")
        # Crime.add_rel_data(db)

    @staticmethod
    def parse_old_data(db: Database):
        res = db.select("SELECT zip, street, street_number, id FROM Address")
        ids = [i[3] for i in res]
        keys = [str(i[0]) + str(i[1]) + str(i[2]) for i in res]

        parsed_address = dict(zip(keys, ids))

        res = db.select("SELECT inc_number , ucr_crime_category, premise_type, crime_id FROM Crime")
        ids = [i[3] for i in res]
        keys = [str(i[0]) + str(i[1]) + str(i[2]) for i in res]
        parsed_crime = dict(zip(keys, ids))

        print("Finished parsing")
        return parsed_address, parsed_crime

    @staticmethod
    def add_rel_data(db: Database):
        file_path = "../data/crime-data_crime-data_crimestat.csv"

        i = 1
        parsed_address, parsed_crime = Crime.parse_old_data(db)

        lines = sum(1 for i in open(file_path, 'rb'))
        print("number of columns: {}".format(lines))

        with open(file_path, 'r') as file:
            chunk = 100000

            o_query = "INSERT IGNORE INTO occured_at VALUES (" \
                      "%(a_id)s, " \
                      "%(c_id)s)"

            o_data = []

            start = time.time()

            for line in islice(file, 1, None):
                data = line[:-1].replace('"', '').split(",")
                data = [None if x == "" else x for x in data]
                c_inc_num = data[0] if data[0] else ""
                c_category = data[3] if data[3] else "null"
                c_type = data[6] if data[6] else ""
                words = data[4].split(" ")
                words = [' '.join(words[:2]), ' '.join(words[-2:])]

                zip = data[5] if data[5] else 0
                street = words[1]
                number = words[0]

                o_data.append({
                    'a_id': parsed_address[str(zip) + str(street) + str(number)],
                    'c_id': parsed_crime[str(c_inc_num) + c_category + c_type]
                })

                i += 1
            print("started sending")
            print("amount: {}".format(len(o_data)))
            db.querymany(o_query, o_data)
            end = time.time()
            print("time passed: {}".format(round(end - start, 3)))

            print("finished occurred_at...")
