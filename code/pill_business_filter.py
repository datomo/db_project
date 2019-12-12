import os
import pickle
import shutil
import time
from multiprocessing import Pool, Manager
from os import listdir
from os.path import isfile, join

import db_parser
from database import Database

file_prefix = "business"


b_query = "INSERT INTO Business(business_name, reviewed_business_id, DEA_No) VALUES(" \
          "%(business_name)s, " \
          "%(reviewed_business_id)s, " \
          "%(dea_no)s)"

master = []
i = 0

def filter_file(path):
    global master, i
    businesses = []
    # names = path.replace(".pkl", "").replace("##", "/").split("-")
    with open("./{}/{}".format(file_prefix, path), "rb") as file:
        while True:
            try:
                businesses.append(pickle.load(file))
            except EOFError:
                break

    businesses = [dict(t) for t in {tuple(d.items()) for d in master}]
    master.append(businesses)

    if len(master) > 10000:
        insert(master, db)
        master = []
        print("inserting 1000 businesses")
    #insert(businesses, db)
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
