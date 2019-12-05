import time

from database import Database


class Pill:
    # 21833 error for inc_num deleted "-"
    @staticmethod
    def add_data(db: Database):
        # file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"
        file_path = "../data/arcos_all_washpost.tsv"
        address_path = "../data/temp/c_address.txt"

        addresses = {}

        for i in range(10):
            addresses[str(i)] = {}

        raw_addresses = {}

        for i in range(10):
            raw_addresses[str(i)] = {}

        businesses = {}
        drugs = []
        is_located = []
        i = 0
        id_start = db.select_one("SELECT MAX(address_id) FROM Address")[0]
        print("start id from addresses: {}".format(id_start))

        with open(address_path) as file:
            for line in file:
                (key, value) = line.replace("\n", "").split("#")
                addresses[key[0]][key] = value

        # lines = sum(1 for i in open(file_path, 'rb'))
        lines = 178598027
        print("number of columns: {}".format(lines))

        with open(file_path, 'r') as file:
            chunk = 200000
            addresses_generated = 0

            chunk_amount = int(float(lines) / chunk)
            print("{}".format(chunk_amount))

            a_query = "INSERT INTO Address VALUES(" \
                      "%(id)s, " \
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
                      "%(id)s," \
                      "%(business_name)s, " \
                      "%(reviewed_business_id)s, " \
                      "%(dea_no)s)"
            r_query = "INSERT INTO Report VALUES(" \
                      "%(id)s, " \
                      "%(trans_id)s, " \
                      "%(correction_no)s," \
                      "%(action_indicator)s," \
                      "%(trans_code)s," \
                      "%(order_from_no)s," \
                      "%(reporter_family)s," \
                      "%(trans_code)s," \
                      "%(revised_company_name)s," \
                      "%(measure)s," \
                      "%(unit)s," \
                      "%(quantity)s," \
                      "%(dosage_unit)s)"
            d_query = "INSERT INTO Drug VALUES(" \
                      "%(ndc_no)s," \
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
            specifies_query = "INSERT INTO specifies VALUES (%(trans_id)s, %(ndc_no)s)"
            for a_chunk in range(chunk_amount):

                start_time = time.time()

                start = (a_chunk * chunk) + 1
                end = start + chunk - 1
                if a_chunk == chunk_amount - 1:
                    end = lines

                a_data = []
                b_data = []
                r_data = []
                d_data = []

                is_loc_data = []
                reports_data = []
                specifies_data = []

                print("{} - {}".format(start, end))
                print("i atm: {}".format(i))

                for line in file:

                    if i == 0:
                        i += 1
                        continue

                    # print(line)
                    elements = line.replace("\n", "").split('\t')

                    ## - report process
                    trans_id = i

                    r_data.append(Pill.replace_null({
                        'id': trans_id,
                        'trans_id': elements[33],
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
                    }))

                    ## - drug process
                    ndc_id = str(elements[22])

                    if ndc_id not in drugs:
                        d_data.append(Pill.replace_null({
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
                        }))
                        drugs.append(ndc_id)
                    # print(elements[41])

                    ## - specifies process

                    specifies_data.append({
                        "trans_id": trans_id,
                        "ndc_no": ndc_id
                    })

                    ## - reporter process
                    dea = elements[0]
                    b_name = elements[2]
                    inc = 2 * i + id_start

                    a_id, addresses_generated = Pill.process_reporter_address(a_data, addresses, raw_addresses,
                                                                              addresses_generated, elements, inc)

                    b_id = Pill.process_business(b_data, b_name, businesses, dea, inc)

                    Pill.process_is_located(a_id, b_id, is_loc_data, is_located)

                    bus_act = elements[1]
                    role = "REPORTER"

                    Pill.process_reports(b_id, bus_act, reports_data, role, trans_id)

                    ## - buyer process

                    dea = elements[10]
                    b_name = elements[12]
                    inc = 2 * i + 1 + id_start

                    a_id, addresses_generated = Pill.process_buyer_address(a_data, addresses, raw_addresses,
                                                                           addresses_generated, elements, inc)

                    b_id = Pill.process_business(b_data, b_name, businesses, dea, inc)

                    Pill.process_is_located(a_id, b_id, is_loc_data, is_located)

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

                end_time = time.time()

                print("time passed: {}".format(round(end_time - start_time, 3)))
                print("executed {} rows from {}: {}%".format(i, lines, round(i / lines * 100, 2)))
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
                for sub_k, sub_v in v.items():
                    file.write('{}#{}\n'.format(sub_k, sub_v))

    @staticmethod
    def add_data_parallel(db: Database, start: int, end = None):
        file_path = "../data/arcos_all_washpost.tsv"
        address_path = "../data/temp/c_address.txt"

        with open(file_path, 'r') as file:
            chunk = 200000

            # lines = sum(1 for i in open(file_path, 'rb'))
            lines = 178598027
            print("number of columns: {}".format(lines))

            chunk_amount = int(float(lines) / chunk)
            print("{}".format(chunk_amount))

            i = 0

            start_time = time.time()

            for a_chunk in range(chunk_amount):

                start = (a_chunk * chunk) + 1
                end = start + chunk - 1

                output = []

                for line in file:
                    if i == 0:
                        i += 1
                        continue
                    output.append(line)
                    i += 1

                    if i == end:
                        break
                Pill.parse_cols(output, db)
                print("executed {} rows from {}: {}%".format(i, lines, round(i / lines * 100, 2)))
                break
                ## hand to processe

            print("{}s".format(time.time()-start_time))

    @staticmethod
    def parse_cols(cols:[], db:Database):
        a_query = "INSERT INTO Address (zip, city, street, street_number, county, state, address_name, longitude, latitude, addl_co_info) " \
                  "SELECT * FROM (SELECT " \
                  "%(zip)s AS zip, " \
                  "%(city)s AS city, " \
                  "%(street)s AS street, " \
                  "%(street_number)s AS street_number, " \
                  "%(county)s AS county, " \
                  "%(state)s AS state, " \
                  "%(address_name)s AS address_name, " \
                  "%(longitude)s AS longitude, " \
                  "%(latitude)s AS latitude, " \
                  "%(addl_co_info)s AS addl_co_info) AS tmp" \
                  " WHERE NOT EXISTS (" \
                  "SELECT zip FROM Address WHERE zip = %(zip)s AND street = %(street)s AND street_number = %(street_number)s" \
                  ") LIMIT 1"
        a_data = []
        i = 0

        for col in cols:
            elements = col.replace("\n", "").split('\t')
            buy_add1 = elements[14]
            buy_add2 = elements[15]

            (street_name, street_num, additional) = Pill.process_address(buy_add1, buy_add2)

            if not additional and not elements[3] or (additional == "null" and additional == "null"):
                additional = None
            elif additional and elements:
                additional = "\n".join([additional, elements[3]])
            else:
                additional = additional if additional != "null" and additional else elements[3]

            # buy_add
            a_data.append({
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
            i += 1
        print("finished")

        db.querymany(a_query, a_data)



    @staticmethod
    def replace_null(obj):
        for k, v in obj.items():
            if v == "null":
                obj[k] = None
        return obj

    @staticmethod
    def process_reports(b_id, bus_act, reports_data, role, trans_id):
        reports_data.append({
            "b_id": b_id,
            "trans_id": trans_id,
            "act": bus_act,
            "role": role
        })

    @staticmethod
    def process_is_located(a_id, b_id, is_loc_data, is_locaded_list):
        if str(a_id) + str(b_id) not in is_locaded_list:
            is_loc_data.append({
                'a_id': a_id,
                'b_id': b_id
            })
            is_locaded_list.append(str(a_id) + str(b_id))

    @staticmethod
    def process_business(b_data, b_name, businesses, dea, inc):
        if dea not in businesses:
            # does not exist in db
            b_id = inc

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
    def process_buyer_address(a_data, addresses, raw_addresses, addresses_generated, elements, inc):
        buy_add1 = elements[14]
        buy_add2 = elements[15]

        zip = elements[18]

        raw_key = "{},{},{}".format(zip, buy_add1, buy_add2)

        if raw_key in raw_addresses[zip[0]]:
            return raw_addresses[zip[0]][raw_key], addresses_generated

        (street_name, street_num, additional) = Pill.process_address(buy_add1, buy_add2)

        if str(zip) + street_name + street_num not in addresses[zip[0]]:
            a_id = inc
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
                'zip': zip,
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
            key = str(zip) + street_name + street_num
            pos = zip[0]
            addresses[pos][key] = a_id
            raw_addresses[pos]["{},{},{}".format(zip, buy_add1, buy_add2)] = a_id

        else:
            a_id = addresses[zip[0]][str(elements[18]) + street_name + street_num]

        return a_id, addresses_generated

    @staticmethod
    def process_reporter_address(a_data, addresses, raw_addresses, addresses_generated, elements, inc):
        rep_add1 = elements[4]
        rep_add2 = elements[5]

        zip = elements[8]

        raw_key = "{},{},{}".format(zip, rep_add1, rep_add2)

        if raw_key in raw_addresses[zip[0]]:
            return raw_addresses[zip[0]][raw_key], addresses_generated

        (street_name, street_num, additional) = Pill.process_address(rep_add1, rep_add2)

        if str(zip) + street_name + street_num not in addresses[zip[0]]:
            a_id = inc

            addresses_generated += 1
            if not additional and not elements[3] or (additional == "null" and additional == "null"):
                additional = None
            elif additional and elements:
                additional = "\n".join([additional, elements[3]])
            else:
                additional = additional if additional != "null" and additional else elements[3]

            if a_id == 60:
                print(inc)

            # rep_add
            a_data.append({
                'id': a_id,
                'zip': zip,
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
            key = str(zip) + street_name + street_num
            pos = zip[0]
            addresses[pos][key] = a_id
            raw_addresses[pos]["{},{},{}".format(zip, rep_add1, rep_add2)] = a_id

        else:
            a_id = addresses[zip[0]][str(elements[8]) + street_name + street_num]
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
        if len(parts) >= 2 and (len(parts[1]) == 1 or (len(parts[1]) == 2 and parts[1][1] == ".")):
            num += " " + parts[1]
            rest = parts[2:]
        rest = " ".join(rest)

        return num, rest.replace(" ST ", " STREET ")
