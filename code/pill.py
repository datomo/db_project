from itertools import islice
import sys

from database import Database
import time


class Pill:
    # 21833 error for inc_num deleted "-"
    @staticmethod
    def add_data(db: Database):
        file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"
        address_path = "../data/temp/c_address.txt"

        addresses = {}
        i = 0
        id_start = int(str(db.select_one("SELECT MAX(address_id) FROM Address")[0])[:-1])
        print(id_start)
        print(sys.maxsize)
        a_i = 1 + id_start

        with open(address_path) as file:
            for line in file:
                (key, value) = line.replace("\n", "").split("#")
                addresses[str(key)] = value

        lines = sum(1 for i in open(file_path, 'rb'))
        print("number of columns: {}".format(lines))

        with open(file_path, 'r') as file:
            chunk = 80000
            addresses_generated = 0

            chunk_amount = int(float(lines) / chunk)
            print("{}".format(chunk_amount))

            a_data = []
            a_id = str(a_i) + "00"

            b_data = []
            b_id = str(i) + "00"

            r_data = []
            d_data = []
            il_data = []
            re_data = []
            sp_data = []


            for a_chunk in range(chunk_amount):

                start = (a_chunk * chunk) + 1
                end = start + chunk - 1
                if a_chunk == chunk_amount - 1:
                    end = lines

                print("{} - {}".format(start, end))
                print("i atm: {}".format(i))

                for line in file:

                    if i == 0:
                        i += 1
                        continue

                    # print(line)
                    elements = line.split('\t')

                    # print(str(elements))

                    rep_add1 = elements[4]
                    rep_add2 = elements[5]

                    buy_add1 = elements[14]
                    buy_add2 = elements[15]

                    (street_name, street_num, additional) = Pill.process_address(rep_add1, rep_add2)
                    zip = elements[8]
                    # print("{}#{}#{}#{}".format(zip, street_name, street_num, additional))

                    if str(elements[8]) + street_name + street_num not in addresses:
                        addresses_generated += 1
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

                    if str(elements[18]) + street_name + street_num not in addresses:

                        addresses_generated += 1

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
                        a_i += 1
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
                        'transaction_id': elements[33],
                        'correction_no': elements[28],
                        'action_indicator': elements[26],
                        'transaction_code': elements[20],
                        'order_from_no': elements[27],
                        'reporter_family': elements[40],
                        'transaction_date': elements[30],
                        'revised_company_name': elements[39],
                        'measure': elements[36],
                        'unit': elements[25],
                        'quantity': elements[24],
                        'dosage_unit': elements[32]
                    })

                    i += 1
                    if i == end: break  # bad practice...maybe change later

        print("addresses generated: {}".format(addresses_generated))
        print("{}".format(i))

        with open("../data/temp/c_address_after_pill.txt", "w+") as file:
            for k, v in addresses.items():
                file.write('{}#{}\n'.format(k, v))



        d_data.append({
            "ndc_no": elements[22],
            "combined_labeler_name": elements[38],
            "dos_str": elements[41],
            "calc_base_wt_in_gm": elements[31],
            "product_name": elements[34],
            "strength": elements[29],
            "drug_code": elements[21],
            "drug_name": elements[23],
            "ingredient_name": elements[35],
            "mme_conversion_factor": elements[37]
        })



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

        return num, rest.replace(" ST ", " STREET ")
