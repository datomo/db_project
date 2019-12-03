from itertools import islice

from database import Database
import time


class Pill:
    # 21833 error for inc_num deleted "-"
    @staticmethod
    def add_data(db: Database):
        file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"
        address_path = "../data/temp/c_address.txt"
        i = 1
        addresses = {}

        id_start = int(str(db.select_one("SELECT MAX(address_id) FROM Address")[0])[:-1])
        print(id_start)

        with open(address_path) as file:
            for line in file:
                (key, value) = line.replace("\n", "").split("#")
                addresses[str(key)] = value

        lines = sum(1 for i in open(file_path, 'rb'))
        print("number of columns: {}".format(lines))

        with open(file_path, 'r') as file:
            chunk = 100000
            found_address = 0

            for line in islice(file, 1, 20):
                elements = line.replace("\n", "").split('\t')
                # print(str(elements))

                rep_add1 = elements[4]
                rep_add2 = elements[5]

                buy_add1 = elements[14]
                buy_add2 = elements[15]

                (street_name, street_num, additional) = Pill.process_address(rep_add1, rep_add2)
                zip = elements[8]
                print("{}#{}#{}#{}".format(zip, street_name, street_num, additional))

                (street_name, street_num, additional) = Pill.process_address(buy_add1, buy_add2)
                zip = elements[18]
                print("{}#{}#{}#{}".format(zip, street_name, street_num, additional))

                ''''{
                id
                zip
                city
                street
                street_number
                county
                state
                address_name
                longitude
                latitude
                addl_co_info
                }'''

    @staticmethod
    def process_address(add1, add2) -> (str, str, str):
        if add1 == "null":
            return Pill.split_address(add2) + (None,)
        elif add2 == "null":
            return Pill.split_address(add1) + (None,)
        else:
            if add1 == "null" and add2 == "null":
                return None, None, None
            else:
                if "DBA" == add1[:2]:
                    # add2 it is
                    return Pill.split_address(add2) + (add1,)
                elif "DBA" == add2[:2]:
                    # add1 it is
                    return Pill.split_address(add1) + (add2,)
                else:
                    if any(i.isdigit() for i in add1):
                        # add1
                        return Pill.split_address(add1) + (add2,)
                    else:
                        # add2
                        return Pill.split_address(add2) + (add1,)

    @staticmethod
    def split_address(add) -> (str, str):
        parts = add.split(" ")
        num = parts[0]
        rest = parts[1:]
        if len(parts[1]) == 1 or (len(parts[1]) == 2 and parts[1][1] == "."):
            num += parts[1]
            rest = parts[2:]
        rest = " ".join(rest)

        return num, rest
