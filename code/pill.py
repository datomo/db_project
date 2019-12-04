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
        businesses = {}
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
            b_data = []
            r_data = []
            d_data = []

            is_loc_data = []
            reports_data = []
            specifies_data = []

            a_query = "INSERT INTO Address VALUES(" \
                      "%(a_id)s, " \
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
            b_query = "INSERT INTO Business VALUES(" \
                      "%(b_id)s," \
                      "%(business_name)s, " \
                      "%(reviewed_business_id)s, " \
                      "%(dea_no)s)"
            r_query = "INSERT INTO Reports VALUES(" \
                      "%(trans_id)s, " \
                      "%(correction_no)s," \
                      "%(action_indicator)s," \
                      "%(trans_code)s," \
                      "%(order_from_no)s," \
                      "%(reporter_family)s," \
                      "%(trans_code)s," \
                      "%(revised_company_name)s" \
                      "%(measure)s" \
                      "%(unit)s," \
                      "%(quantity)s," \
                      "%(dosage_unit)s)"
            d_query = "INSERT INTO Drug VALUES(" \
                      "%(ndc_id)s," \
                      "%(combined_labeler_name)s," \
                      "%(dos_str)s," \
                      "%(calc_base)s," \
                      "%(product_name)s," \
                      "%(strength)s," \
                      "%(drug_code)s," \
                      "%(drug_name)s," \
                      "%(ingredient_name)s," \
                      "%(mme)s)"
            is_loc_query = "INSERT INTO is_located VALUES(%(a_id)s, %(b_id)s)"
            reports_query = "INSERT INTO reports VALUES(%(b_id)s, %(trans_id)s, %(act)s, %(role)s)"
            specifies_query = "INSERT INTO specifies VALUES (%(trans_id)s, %(ndc_id)s)"
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

                    ## - report process
                    trans_id = elements[33]

                    r_data.append({
                        'transaction_id': trans_id,
                        'correction_no': elements[28],
                        'action_indicator': elements[26],
                        'trans_code': elements[20],
                        'order_from_no': elements[27],
                        'reporter_family': elements[40],
                        'trans_date': elements[30],
                        'revised_company_name': elements[39],
                        'measure': elements[36],
                        'unit': elements[25],
                        'quantity': elements[24],
                        'dosage_unit': elements[32]
                    })

                    ## - drug process
                    ndc_id = elements[22]
                    d_data.append({
                        "ndc_no": ndc_id,
                        "combined_labeler_name": elements[38],
                        "dos_str": elements[41],
                        "calc_base": elements[31],
                        "product_name": elements[34],
                        "strength": elements[29],
                        "drug_code": elements[21],
                        "drug_name": elements[23],
                        "ingredient_name": elements[35],
                        "mme": elements[37]
                    })

                    ## - specifies process

                    specifies_data.append({
                        "trans_id": trans_id,
                        "ndc_id": ndc_id
                    })

                    ## - reporter process
                    dea = elements[0]
                    b_name = elements[2]
                    inc = 2 * i

                    a_id, addresses_generated = Pill.process_reporter_address(a_data, addresses,
                                                                              addresses_generated, elements, inc)

                    b_id = Pill.process_business(b_data, b_name, businesses, dea, inc)

                    Pill.process_is_located(a_id, b_id, is_loc_data)

                    bus_act = elements[1]
                    role = "REPORTER"

                    Pill.process_reports(b_id, bus_act, reports_data, role, trans_id)

                    ## - buyer process

                    dea = elements[10]
                    b_name = elements[12]
                    inc = 2 * i + 1

                    a_id, addresses_generated = Pill.process_buyer_address(a_data, addresses,
                                                                           addresses_generated, elements, inc)

                    b_id = Pill.process_business(b_data, b_name, businesses, dea, inc)

                    Pill.process_is_located(a_id, b_id, is_loc_data)

                    bus_act = elements[11]
                    role = "BUYER"

                    Pill.process_reports(b_id, bus_act, reports_data, role, trans_id)

                    i += 1
                    if i == end:
                        break  # bad practice...maybe change later

                db.querymany(d_query, d_data)
                db.querymany(r_query, r_data)
                db.querymany(specifies_query, specifies_data)
                db.querymany(b_query, b_data)
                db.querymany(reports_query, reports_data)
                db.querymany(a_query, a_data)
                db.querymany(is_loc_query, is_loc_data)

                print("exuted: d: {}, r: {}, spec: {}, b: {}, rep: {}, a: {}, is: {}".format(len(d_data),
                                                                                             len(r_data),
                                                                                             len(specifies_data),
                                                                                             len(b_data),
                                                                                             len(reports_data),
                                                                                             len(a_data),
                                                                                             len(is_loc_data)))

        print("addresses generated: {}".format(addresses_generated))
        print("{}".format(i))

        with open("../data/temp/c_address_after_pill.txt", "w+") as file:
            for k, v in addresses.items():
                file.write('{}#{}\n'.format(k, v))

    @staticmethod
    def process_reports(b_id, bus_act, reports_data, role, trans_id):
        reports_data.append({
            "b_id": b_id,
            "trans_id": trans_id,
            "act": bus_act,
            "role": role
        })

    @staticmethod
    def process_is_located(a_id, b_id, is_loc_data):
        is_loc_data.append({
            'a_id': a_id,
            'b_id': b_id
        })

    @staticmethod
    def process_business(b_data, b_name, businesses, dea, inc):
        if dea not in businesses:
            # does not exist in db
            b_id = str(inc) + "00"

            b_data.append({
                'id': b_id,
                'business_name': b_name,
                'reviewed_business_id': None,
                'dea_no': dea
            })
            businesses[dea] = b_id

        else:
            # exists
            b_id = businesses[dea]
        return b_id

    @staticmethod
    def process_buyer_address(a_data, addresses, addresses_generated, elements, inc):
        buy_add1 = elements[14]
        buy_add2 = elements[15]
        (street_name, street_num, additional) = Pill.process_address(buy_add1, buy_add2)

        if str(elements[18]) + street_name + street_num not in addresses:
            a_id = str(inc) + "00"
            addresses_generated += 1

            if not additional and not elements[3] or (additional == "null" and additional == "null"):
                additional = None
            elif additional and elements:
                additional = "\n".join([additional, elements[3]])
            else:
                additional = additional if additional != "null" and additional else elements[3]

            # buy_add
            a_data.append({
                'id': a_id,
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
        else:
            a_id = addresses[str(elements[18]) + street_name + street_num]

        return a_id, addresses_generated

    @staticmethod
    def process_reporter_address(a_data, addresses, addresses_generated, elements, inc):
        rep_add1 = elements[4]
        rep_add2 = elements[5]
        (street_name, street_num, additional) = Pill.process_address(rep_add1, rep_add2)

        if str(elements[8]) + street_name + street_num not in addresses:
            a_id = str(inc) + "00"

            addresses_generated += 1
            if not additional and not elements[3] or (additional == "null" and additional == "null"):
                additional = None
            elif additional and elements:
                additional = "\n".join([additional, elements[3]])
            else:
                additional = additional if additional != "null" and additional else elements[3]

            # rep_add
            a_data.append({
                'id': a_id,
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


        else:
            a_id = addresses[str(elements[8]) + street_name + street_num]
        return a_id, addresses_generated

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
