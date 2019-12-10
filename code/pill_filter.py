import os
import pickle
import shutil
import time
from multiprocessing import Pool
from os import listdir
from os.path import isfile, join

file_prefix = "states"
filtered_files = "./{}/filtered".format(file_prefix)


def filter_file(path):
    start = time.time()
    print("starting process from pool")
    addresses = []
    with open("./{}/{}".format(file_prefix, path), "rb") as file:
        while True:
            try:
                addresses.append(pickle.load(file))
            except EOFError:
                break

    save_as_pickle(list(addresses), path, filtered_files)
    print("finished file after {}s".format(round(time.time() - start, 2)))


def save_as_pickle(addresses, name, file_prefix):
    with open(file_prefix + "/" + name, "ab+") as state_file:
        for address in addresses:
            pickle.dump(address, state_file, 0)


if __name__ == '__main__':
    start = time.time()
    if os.path.exists(filtered_files):
        shutil.rmtree(filtered_files)

    os.makedirs(filtered_files)

    pool = Pool(processes=12)
    files = [f for f in listdir(file_prefix) if isfile(join(file_prefix, f))]

    pool.map(filter_file, files)
    print("ended all after {}s".format(round(time.time() - start), 2))
