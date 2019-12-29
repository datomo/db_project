import json
import math
import time

import db_parser
from database import Database


class Yelp:
    @staticmethod
    def add_data(db: Database):
        queries = db_parser.transform_sql("./sql/create_review.sql")
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
            # if len(col) > 9 or len(col) < 9: print(len(col))
            r_data.append(col)
        db.querymany(r_query, r_data)

    @staticmethod
    def add_data_business(db: Database):
        file_path = "../data/yelp_academic_dataset_business.json"
        db.query(
            "ALTER TABLE Business CHANGE business_name business_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        queries = db_parser.transform_sql("./sql/create_table_add_info.sql")
        db.query_all(queries)

        lines = sum(1 for i in open(file_path, 'rb'))
        print(lines)

        with open(file_path, encoding="utf8", mode="r") as file:
            names = set()
            postal_codes = set()
            for line in file:
                name = line.split('"name":"')[1].split('"', 1)[0]
                if not name == "":
                    names.add(name.replace("/", "").replace("\\", ""))
                code = line.split('postal_code":"')[1].split('"', 1)[0]
                if all(i.isdigit() for i in code) and not code == "":
                    postal_codes.add(code)

            names = list(names)
            postal_codes = list(postal_codes)

        Yelp.get_businesses(db, names)
        Yelp.get_addresses(db, postal_codes)

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
                Yelp.parse_cols_rel(db, output)
                print("{}s part".format(time.time() - start))

        print("finished creating tables")
        Yelp.get_addresses(db, postal_codes)
        Yelp.get_businesses(db, names)
        Yelp.get_businesses_locations(db, names, postal_codes)

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
                Yelp.parse_rel(db, output)
                print("{}s part".format(time.time() - start))

    @staticmethod
    def get_addresses(db: Database, codes):
        res = db.select("SELECT zip, street, id FROM Address WHERE zip IN ({})".format(str(codes)[1:-1]))
        global parsed_addresses
        parsed_addresses = dict([((str(x[0]) if x[0] else "") + (x[1] if x[1] else ""), x[2]) for x in res])

    @staticmethod
    def get_businesses(db: Database, names):
        res = db.select("SELECT business_name, id FROM Business WHERE business_name IN ({})".format(str(names)[1:-1]))
        global parsed_business
        parsed_business = dict(res)

    @staticmethod
    def get_businesses_locations(db, names, codes):
        # print("SELECT business_name, id FROM Business  WHERE business_name IN ({})".format(str(names)[1:-1]))
        res = db.select("SELECT db_project.business.id, tmp2.address_id "
                        "FROM db_project.business INNER JOIN ("
                        "SELECT * FROM db_project.is_located INNER JOIN (SELECT zip, street, id FROM db_project.address) as tmp ON address_id = tmp.id WHERE zip IN ({})"
                        ") as tmp2 ON tmp2.business_id = db_project.business.id WHERE business_name IN ({});".format(
            str(codes)[1:-1], str(names)[1:-1]))
        global parsed_business_locations
        parsed_business_locations = dict(res)

    @staticmethod
    def parse_cols_rel(db, cols):
        a_query = "INSERT IGNORE INTO Address(zip,city,street,street_number,county,state,address_name,longitude,latitude,addl_co_info) VALUES(" \
                  "%(zip)s, " \
                  "%(city)s, " \
                  "%(street)s, " \
                  "%(street_number)s, " \
                  "%(county)s, " \
                  "%(state)s, " \
                  "%(address_name)s, " \
                  "%(longitude)s, " \
                  "%(latitude)s, " \
                  "%(addl_co_info)s)"
        a_data = []
        b_query = "INSERT IGNORE INTO Business(business_name,reviewed_business_id,dea_no) VALUES(" \
                  "%(business_name)s, " \
                  "%(reviewed_business_id)s, " \
                  "%(dea_no)s)"
        b_data = []

        global parsed_business, parsed_addresses

        for col in cols:

            parsed = json.loads(col)
            street_p = " ".join([x for x in parsed["address"].split(" ") if not any(i.isdigit() for i in x)])

            if not str(parsed["postal_code"]) + street_p in parsed_addresses:
                a_data.append({
                    'zip': parsed["postal_code"],
                    'city': parsed["city"],
                    'street': street_p,
                    'street_number': 0,
                    'county': None,
                    'state': parsed["state"],
                    'address_name': None,
                    'longitude': parsed['longitude'],
                    'latitude': parsed['latitude'],
                    'addl_co_info': None
                })

            if not parsed["name"] in parsed_business:
                b_data.append({
                    'business_name': parsed["name"],
                    'reviewed_business_id': parsed["business_id"],
                    'dea_no': None
                })

        a_data = [dict(t) for t in {tuple(d.items()) for d in a_data}]
        b_data = [dict(t) for t in {tuple(d.items()) for d in b_data}]

        db.querymany(a_query, a_data)
        db.querymany(b_query, b_data)

    @staticmethod
    def parse_rel(db, cols):
        is_located_query = "INSERT IGNORE INTO is_located VALUES(%(a_id)s, %(b_id)s)"
        is_located_data = []
        abi_query = "INSERT IGNORE INTO Add_Business_Info VALUES (" \
                    "%(id)s, " \
                    "%(is_open)s, " \
                    "%(review_count)s, " \
                    "%(stars)s, " \
                    "%(categories)s, " \
                    "%(hours)s, " \
                    "%(attributes)s)"
        abi_data = []
        global parsed_addresses, parsed_business

        for col in cols:
            parsed = json.loads(col)
            street_p = " ".join([x for x in parsed["address"].split(" ") if not any(i.isdigit() for i in x)])

            if all(i.isdigit() for i in parsed["postal_code"]) and not parsed["postal_code"] == "":
                code = parsed["postal_code"]
            else:
                code = "0"

            try:
                name = parsed_business[parsed["name"].replace("/", "").replace("\\", "")]
            except:
                name = "0"

            try:
                address = parsed_addresses[str(code) + street_p]
            except:
                address = 0

            is_located_data.append({
                "a_id": address,
                "b_id": name
            })

            abi_data.append({
                "id": name,
                "is_open": parsed["is_open"],
                "review_count": parsed["review_count"],
                "stars": parsed["stars"],
                "categories": str(parsed["categories"]),
                "hours": str(parsed["hours"]).replace("{", "").replace("}", "") if parsed["hours"] != "null" else None,
                "attributes": str(parsed["attributes"])
            })
        db.querymany(is_located_query, is_located_data)
        db.querymany(abi_query, abi_data)


'''
if __name__ == '__main__':
    db = Database()
    Yelp.add_data(db)'''

def main():
    db = Database()
    Yelp.add_data_business(db)

main()