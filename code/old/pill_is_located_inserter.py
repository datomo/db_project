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
    print(names)
    res = db.select(
        "SELECT street, street_number, id, zip FROM Address WHERE state = '{}' AND city = '{}' AND zip = {} ;".format(
            *names))
    print("SELECT street, street_number, id, zip FROM Address WHERE state = '{}' AND city = '{}' AND zip = {} ;".format(
            *names))
    print(res)
    addresses = {(f[0] if f[0] or f[0] == " " else "") + (f[1] if f[1] or f[1] == " " else ""): f[2] for f in res}
    print(addresses)

    with open("./{}/{}".format(file_prefix, path), "rb") as file:
        while True:
            try:
                f = pickle.load(file)
                master.append({
                    'business_id': businesses[f['dea_no']],
                    'address_id': addresses[(f['street'] + f['street_number'])]
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


def insert(inserts, db):
    db.querymany(is_located_query, inserts)


if __name__ == '__main__':
    start = time.time()
    db = Database()
    query = db_parser.transform_sql("sql/create_is_located.sql")
    db.query_all(query)
    global businesses
    businesses = {str(f[0]): f[1] for f in db.select("SELECT DEA_No, id FROM Business")}

    files = [f for f in listdir(file_prefix) if isfile(join(file_prefix, f))]

    [filter_file(file) for file in files]
    db.querymany(is_located_query, master)

    print("ended all after {}s".format(round(time.time() - start), 2))
