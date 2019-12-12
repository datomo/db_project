import pickle
import time
from os import listdir
from os.path import isfile, join

import db_parser
from database import Database

file_prefix = "states/filtered"

a_query = "INSERT INTO Address(" \
          "zip, city, street, street_number, county, state, address_name, longitude, latitude, addl_co_info) VALUES(" \
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


def open_addresses(path, db):
    start = time.time()
    addresses = []
    with open("./{}/{}".format(file_prefix, path), "rb") as file:
        while True:
            try:
                addresses.append(pickle.load(file))
            except EOFError:
                break

    db.querymany(a_query, addresses)

    # print("finished file after {}s".format(round(time.time() - start, 2)))


def save_as_pickle(addresses, name, file_prefix):
    with open(file_prefix + "/" + name, "ab+") as state_file:
        for address in addresses:
            pickle.dump(address, state_file, 0)


if __name__ == '__main__':
    start = time.time()

    files = [f for f in listdir(file_prefix) if isfile(join(file_prefix, f))]

    print(files)

    db = Database()
    query = db_parser.transform_sql("sql/create_address.sql")
    db.query_all(query)

    [open_addresses(f, db) for f in files]

    print("ended all after {}s".format(round(time.time() - start), 2))
