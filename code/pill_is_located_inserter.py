import os
import pickle
import shutil
import time
from multiprocessing import Pool, Manager
from os import listdir
from os.path import isfile, join

import db_parser
from database import Database

file_prefix = "business_addresses"

is_located_query = "INSERT INTO is_located (address_id, business_id) VALUES(%(a_id)s, %(b_id)s)"

master = []
i = 0


def filter_file(path):
    global master, i, businesses
    names = path.replace(".pkl", "").replace("##", "/").split("-")
    res = db.select(
        "SELECT street, street_number, id FROM Address WHERE state = '{}' AND city = '{}' AND zip = '{}' ;".format(
            *names))
    addresses = {f[0] + f[1]: f[2] for f in res}

    with open("./{}/{}".format(file_prefix, path), "rb") as file:
        while True:
            try:
                file = pickle.load(file)
                master.append({
                    'business_id': businesses[file['DEA_No']],
                    'address_id': addresses[file['street'] + file['street_number']]
                })
            except EOFError:
                break

    master = [dict(t) for t in {tuple(d.items()) for d in master}]

    # print(len(master))
    i += 1
    if i % 100 == 0 or len(master) > 2000000:
        insert(master, db)
        master = []
        print("Chuncked finished: {}".format(i))


    # insert(businesses, db)
    # addresses = db.select("SELECT * FROM Adress WHERE state = %s AND city = %s AND zip = %s ;".format(names))
    # print("finished file after {}s".format(round(time.time() - start, 2)))


def insert(inserts, db):
    db.querymany(b_query, inserts)


if __name__ == '__main__':
    start = time.time()
    db = Database()
    query = db_parser.transform_sql("sql/create_is_located.sql")
    db.query_all(query)
    global businesses
    businesses = {str(f[0]): f[1] for f in db.select("SELECT DEA_No, id FROM Business")}

    files = [f for f in listdir(file_prefix) if isfile(join(file_prefix, f))]

    [filter_file(file) for file in files]
    db.querymany(b_query, master)

    print("ended all after {}s".format(round(time.time() - start), 2))
