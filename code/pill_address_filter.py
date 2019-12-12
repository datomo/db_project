import os
import pickle
import shutil
import time
from multiprocessing import Pool, Manager
from os import listdir
from os.path import isfile, join

import db_parser
from database import Database

file_prefix = "states"

master = []
i = 0


def filter_file(path):
    global master, i
    addresses = []

    with open("./{}/{}".format(file_prefix, path), "rb") as file:
        while True:
            try:
                master.append(pickle.load(file))

            except EOFError:
                break

    # print(len(master))
    i += 1
    if i % 100 == 0:
        print("Chuncked finished: {}".format(i))

    if len(master) > 500000:
        master = [dict(t) for t in {tuple(d.items()) for d in master}]
        insert(master, db)
        master = []
        print("too many addresses")
    # insert(businesses, db)
    # addresses = db.select("SELECT * FROM Adress WHERE state = %s AND city = %s AND zip = %s ;".format(names))
    # print("finished file after {}s".format(round(time.time() - start, 2)))


def insert(inserts, db):
    db.querymany(b_query, inserts)


if __name__ == '__main__':
    start = time.time()
    db = Database()
    query = db_parser.transform_sql("sql/create_business.sql")
    db.query_all(query)

    files = [f for f in listdir(file_prefix) if isfile(join(file_prefix, f))]

    [filter_file(file) for file in files]
    db.querymany(b_query, master)

    print("ended all after {}s".format(round(time.time() - start), 2))