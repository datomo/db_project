import time

from database import Database


class Pill:
    # 21833 error for inc_num deleted "-"
    '''
    is_loc_query = "INSERT INTO is_located VALUES(%(a_id)s, %(b_id)s)"
    reports_query = "INSERT INTO reports VALUES(%(b_id)s, %(trans_id)s, %(act)s, %(role)s)"
    specifies_query = "INSERT INTO specifies VALUES (%(trans_id)s, %(ndc_no)s)"

    ## - specifies process

    specifies_data.append({
        "trans_id": trans_id,
        "ndc_no": ndc_id
    })


    Pill.process_is_located(a_id, b_id, is_loc_data, is_located)

    bus_act = elements[1]
    role = "REPORTER"

    Pill.process_reports(b_id, bus_act, reports_data, role, trans_id)


        print("time passed: {}".format(round(end_time - start_time, 3)))
        print("executed {} rows from {}: {}%".format(i, lines, round(i / lines * 100, 2)))
        print("exuted: d: {}, r: {}, spec: {}, b: {}, rep: {}, a: {}, is: {}".format(len(d_data),
                                                                                     len(r_data),
                                                                                     len(specifies_data),
                                                                                     len(b_data),
                                                                                     len(reports_data),
                                                                                     len(a_data),
                                                                                     len(is_loc_data)))

    '''
    @staticmethod
    def add_data_parallel(db: Database, start: int, end = None):
        file_path = "../data/arcos_all_washpost.tsv"
        address_path = "../data/temp/c_address.txt"

        with open(file_path, 'r') as file:
            chunk = 20000

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
        a_query = "INSERT IGNORE INTO Address VALUES(" \
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

        r_query = "INSERT INTO Report (transaction_id, correction_no, action_indicator,transaction_code,order_from_no,reporter_family,transaction_date,revised_company_name,measure,unit,quantity,dosage_unit) VALUES (" \
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
        b_query = "INSERT IGNORE INTO Business VALUES(" \
                  "%(business_name)s, " \
                  "%(reviewed_business_id)s, " \
                  "%(dea_no)s)"
        d_query = "INSERT IGNORE INTO Drug VALUES(" \
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
        a_data = []
        r_data = []
        b_data = []
        d_data = []

        for col in cols:
            elements = col.replace("\n", "").split('\t')
            positions = [4, 5, 3, 8, 6, 9, 7]
            Pill.process_address(a_data, elements, positions)
            positions = [14, 15, 13, 18, 16, 19, 17]
            Pill.process_address(a_data, elements, positions)
            Pill.proccess_report(elements, r_data)

            dea = elements[0]
            b_name = elements[2]
            Pill.process_business(b_data, b_name, dea)

            dea = elements[10]
            b_name = elements[12]
            Pill.process_business(b_data, b_name, dea)

            Pill.process_drug(d_data, elements, str(elements[22]))

        print("finished")

        db.querymany(a_query, a_data)
        db.querymany(r_query, r_data)
        db.querymany(b_query, b_data)
        db.querymany(d_query, d_data)

        print("finished")

        db.query("ALTER TABLE Address DROP PRIMARY KEY, ADD COLUMN id BIGINT(15) PRIMARY KEY AUTO_INCREMENT ")
        db.query("ALTER TABLE Business DROP PRIMARY KEY, ADD COLUMN id BIGINT(15) PRIMARY KEY AUTO_INCREMENT ")

    @staticmethod
    def process_drug(d_data, elements, ndc_id):
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

    @staticmethod
    def proccess_report(elements, r_data):
        r_data.append(Pill.replace_null({
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

    @staticmethod
    def process_address(a_data, elements, positions):
        add1 = elements[positions[0]]
        add2 = elements[positions[1]]

        (street_name, street_num, additional) = Pill.handle_address(add1, add2)
        if not additional and not elements[positions[2]] or (additional == "null" and additional == "null"):
            additional = None
        elif additional and elements:
            additional = "\n".join([additional, elements[positions[3]]])
        else:
            additional = additional if additional != "null" and additional else elements[positions[2]]
        # buy_add
        a_data.append({
            'zip': elements[positions[3]],
            'city': elements[positions[4]],
            'street': street_name,
            'street_number': street_num,
            'county': elements[positions[5]],
            'state': elements[positions[6]],
            'address_name': additional,
            'longitude': None,
            'latitude': None,
            'addl_co_info': additional
        })

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
    def process_business(b_data, b_name, dea):

        b_data.append({
            'business_name': b_name,
            'reviewed_business_id': None,
            'dea_no': dea
        })

    @staticmethod
    def handle_address(add1, add2) -> (str, str, str):
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
