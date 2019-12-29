import logging
import os
import pickle
import shutil
import time
from collections import OrderedDict
from os.path import isfile, join
from typing import Callable

import db_parser
from database import Database


class Helper:

    @staticmethod
    def generate_key(keys: tuple(), delimiter="-", replace_char="", remove_whitespace=False) -> str:
        parsed = []
        for key in keys:
            parsed.append(str(key) if key else replace_char)
        if remove_whitespace:
            parsed = [f.replace(" ", "") for f in parsed]
        return delimiter.join(parsed)


    @staticmethod
    def chunk_file_pkl(chunk_size, input_file, f: Callable, encoding=None):
        """
        :param chunk_size: size of each chunk
        :param input_file: the source file path
        :param f: function which is applied after each chunk is created
        """
        start = time.time()
        chunk = []
        i = 0
        j = 0
        executed = False
        with open(input_file, "rb") as file:
            while True:
                executed = False
                try:
                    chunk.append(pickle.load(file, encoding=encoding if encoding else ""))

                except EOFError:
                    break
                i += 1
                if i >= chunk_size:
                    f(chunk)
                    executed = True
                    j += 1
                    i = 0
                    chunk = []
        if not executed:
            f(chunk)
            logging.debug("finished file {} after {}s".format(j, round(time.time() - start, 2)))
            start = time.time()

    @staticmethod
    def chunk_file(input_file, f: Callable):
        """
        :param chunk_size: size of each chunk
        :param input_file: the source file path
        :param f: function which is applied after each chunk is created
        """
        start = time.time()
        chunk = []
        i = 0
        chunk = set()
        with open(input_file, "rb") as file:
            for line in file:
                i += 1
                chunk.add(line)
            f(list(chunk))

            i = 0

            logging.debug("finished file after {}s".format(round(time.time() - start, 2)))

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
    def filter_file(file_name, path, filter_folder="filtered", filter_pos=None):
        logging.debug("starting filtering file")
        content_set = set()
        with open("./{}/{}".format(path, file_name), "rb") as file:
            while True:
                try:
                    content_set.add(pickle.load(file))
                except EOFError:
                    break

        if filter_pos is not None:
            keys = set()
            content_set = [t for t in content_set
                           if not (t[filter_pos] in keys or keys.add(t[filter_pos]))]

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

    @staticmethod
    def parse_tuplelist_to_dict(tuplelist):
        return {"".join([str(b) for b in a[:-1]]).replace(" ", ""): a[-1] for a in tuplelist}

    @staticmethod
    def append_to_csv(data, csv_path):
        with open(csv_path, "a+") as file:
            file.truncate()
            for item in data:
                file.write("|".join([str(i).replace("|", "") for i in item]) + "\n")

    @staticmethod
    def append_to_csv_special(data, csv_path):
        with open(csv_path, "a+") as file:
            file.truncate()
            for item in data:
                file.write("|".join(["\"" + str(i) + "\"".replace("|", "") for i in item]) + "\n")
