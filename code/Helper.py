import logging
import os
import pickle
import shutil
import time
from os.path import isfile, join
from typing import Callable

import db_parser
from database import Database


class Helper:

    @staticmethod
    def generate_key(keys: tuple(), delimiter="-", replace_char="") -> str:
        parsed = []
        for key in keys:
            parsed.append(str(key) if key else replace_char)
        return delimiter.join(parsed)

    @staticmethod
    def chunk_file_pkl(chunk_size, input_file, f: Callable):
        """
        :param chunk_size: size of each chunk
        :param input_file: the source file path
        :param f: function which is applied after each chunk is created
        """
        start = time.time()
        chunk = []
        i = 0
        with open(input_file, "rb") as file:
            while True:
                try:
                    chunk.append(pickle.load(file))

                except EOFError:
                    break
                i += 1
                if i >= chunk_size:
                    f(chunk)
                    i = 0
                    chunk = []
                    logging.debug("finished file {} after {}s".format(i, round(time.time() - start, 2)))
                    start = time.time()

    @staticmethod
    def clear_folder(folder):
        if os.path.exists(folder):
            shutil.rmtree(folder)
        os.makedirs(folder)

    @staticmethod
    def write_list(dicts, target_folder, target_file, remove_duplicates=False):
        if remove_duplicates:
            dicts = Helper.uniquify_list(dicts)

        file = open(target_folder + "/" + target_file + ".pkl", "ab+")
        for el in dicts:
            pickle.dump(el, file)
        file.flush()
        file.close()

    @staticmethod
    def write_dicts(dict_of_list, target_folder, remove_duplicates=False):
        for key in dict_of_list:
            file = open(target_folder + "/" + key + ".pkl", "ab+")
            if remove_duplicates:
                dict_of_list[key] = Helper.uniquify_dictlist(dict_of_list[key])
            pickle.dump(dict_of_list[key], file)
            file.flush()
            file.close()

    @staticmethod
    def write_dict_sets(dict_of_set, target_folder):
        for key in dict_of_set:
            file = open(target_folder + "/" + key + ".pkl", "ab+")
            for el in list(dict_of_set[key]):
                pickle.dump(el, file)
            file.flush()
            file.close()

    @staticmethod
    def uniquify_dictlist(dictlist):
        return [dict(t) for t in {tuple(d.items()) for d in dictlist}]

    @staticmethod
    def uniquify_list(a_list):
        return list(dict.fromkeys(a_list))

    @staticmethod
    def filter_file(file_name, path, filter_folder="filtered"):
        logging.debug("starting filtering file")
        content_set = set()
        with open("./{}/{}".format(path, file_name), "rb") as file:
            while True:
                try:
                    content_set.add(pickle.load(file))
                except EOFError:
                    break

        target_folder = path + "/" + filter_folder
        Helper.write_list(list(content_set), target_folder, file_name)
        logging.debug("finished writing filter list")

    @staticmethod
    def get_files_in_folder(folder) -> []:
        return [f for f in os.listdir(folder) if isfile(join(folder, f))]

    @staticmethod
    def add_file_to_list(file_name: str, path: str) -> []:
        content = []
        with open("{}/{}".format(path, file_name), "rb") as file:
            while True:
                try:
                    content.append(pickle.load(file))
                except EOFError:
                    break

        return content

    @staticmethod
    def merge_files_in_unique_list(files: [], path: str) -> []:
        content = []
        for file in files:
            content += Helper.add_file_to_list(file, path)

        return content

    @staticmethod
    def create_tables(sql_path: str, db: Database):
        queries = db_parser.transform_sql(sql_path)
        db.query_all(queries)

    @staticmethod
    def tuplelist_to_listlist(tuplelist) -> [[]]:
        return [list(el) for el in tuplelist]


