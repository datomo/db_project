from itertools import islice

from database import Database
import time


class Pill:
    # 21833 error for inc_num deleted "-"
    @staticmethod
    def add_data(db: Database):
        file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"
        address_path = "../data/temp/c_address.txt"

        addresses = {}
        i = 1
        id_start = int(str(db.select_one("SELECT MAX(address_id) FROM Address")[0])[:-1])
        print(id_start)
        a_i = 1 + id_start

        with open(address_path) as file:
            for line in file:
                (key, value) = line.replace("\n", "").split("#")
                addresses[str(key)] = value

        lines = sum(1 for i in open(file_path, 'rb'))
        print("number of columns: {}".format(lines))

        a_data = []
        a_id = str(a_i) + "00"

        b_data = []
        b_id = str(i) + "00"

        r_data = []
        d_data = []
        il_data = []
        re_data = []
        sp_data = []

        with open(file_path, 'r') as file:
            chunk = 100000
            found_address = 0

            for line in islice(file, 1, None):
                elements = line.replace("\n", "").split('\t')
                # print(str(elements))

                rep_add1 = elements[4]
                rep_add2 = elements[5]

                buy_add1 = elements[14]
                buy_add2 = elements[15]

                (street_name, street_num, additional) = Pill.process_address(rep_add1, rep_add2)
                zip = elements[8]
                # print("{}#{}#{}#{}".format(zip, street_name, street_num, additional))

                if str(elements[8]) + street_name + street_num in addresses:
                    print("in list: {}".format(a_i))
                    if not additional and not elements[3] or (additional == "null" and additional == "null"):
                        additional = None
                    elif additional and elements:
                        additional = "\n".join([additional, elements[3]])
                    else:
                        additional = additional if additional != "null" and additional else elements[3]

                    # rep_add
                    a_data.append({
                        'id': id_start,
                        'zip': elements[8],
                        'city': elements[6],
                        'street': street_name,
                        'street_number': street_num,
                        'county': elements[9],
                        'state': elements[7],
                        'address_name': additional,
                        'longitude': None,
                        'latitude': None,
                        'addl_co_info': additional
                    })
                    addresses[str(elements[8]) + street_name + street_num] = a_id
                    a_i += 1
                    a_id = str(a_id) + "00"

                # if data was added start id + 1

                (street_name, street_num, additional) = Pill.process_address(buy_add1, buy_add2)
                zip = elements[18]
                # print("{}#{}#{}#{}".format(zip, street_name, street_num, additional))

                if str(elements[18]) + street_name + street_num in addresses:

                    print("in list: {}".format(a_id))

                    if not additional and not elements[3] or (additional == "null" and additional == "null"):
                        additional = None
                    elif additional and elements:
                        additional = "\n".join([additional, elements[3]])
                    else:
                        additional = additional if additional != "null" and additional else elements[3]

                    # buy_add
                    a_data.append({
                        'id': id_start,
                        'zip': elements[18],
                        'city': elements[16],
                        'street': street_name,
                        'street_number': street_num,
                        'county': elements[19],
                        'state': elements[17],
                        'address_name': additional,
                        'longitude': None,
                        'latitude': None,
                        'addl_co_info': additional
                    })
                    addresses[str(elements[18]) + street_name + street_num] = a_id

                    # if data was added start id + 1
                    a_id += 1
        # reporter_bus_act = busines name and reporter name meist null darum nehmer wir hier dies
        # rep_add
        b_data.append({
            'id': b_id,
            'business_name': elements[1],
            'revied_business_id': None,
            'dea_no': elements[0]
        })
        # buy_add
        b_data.append({
            'id': b_id,
            'business_name': elements[11],
            'revied_business_id': None,
            'dea_no': elements[10]
        })

        r_data.append({
            'transaction_id': elements[34],
            'correction_no': elements[29],
            'action_indicator': elements[27],
            'transaction_code': elements[20],
            'order_from_no': elements[28],
            'reporter_family': elements[41],
            'transaction_date': elements[31],
            'revised_company_name': elements[30],
            'measure': elements[37],
            'unit': elements[26],
            'quantity': elements[25],
            'dosage_unit': elements[33]
        })

        i += 2

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
