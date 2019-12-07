import math
import time

import db_parser
from database import Database


def process_address(a_data, elements, positions):
    add1 = elements[positions[0]]
    add2 = elements[positions[1]]

    street_name, street_num, additional = handle_address(add1, add2)

    return a_data


def handle_address(add1, add2) -> (str, str, str):
    if add1 == "null":
        return split_address(add2) + (None,)
    elif add2 == "null":
        return split_address(add1) + (None,)
    else:
        if add1 == "null" and add2 == "null":
            return None, None, None
        else:
            if "DBA" == add1[:2]:
                # add2 it is
                return split_address(add2) + (add1,)
            elif "DBA" == add2[:2]:
                # add1 it is
                return split_address(add1) + (add2,)
            else:
                if any(i.isdigit() for i in add1):
                    # add1
                    return split_address(add1) + (add2,)
                else:
                    # add2
                    return split_address(add2) + (add1,)


def split_address(add) -> (str, str):
    parts = add.replace(" ST ", " STREET ").split(" ")
    num = ""
    street = ""

    for part in parts:
        if all([i.isdigit() for i in part]) or len(part) == 1 or len(part.replace(".", "")) == 1:
            num += part + " "
        else:
            street += part + " "

    return street.strip(), num.strip()


def parse_db():
    db = Database()
    res = db.select("SELECT zip, street, street_number, id FROM Address")
    ids = [i[3] for i in res]
    keys = [str(i[0]) + str(i[1]) + str(i[2]) for i in res]
    global parsed_address
    parsed_address = dict(zip(keys, ids))

    res = db.select("SELECT business_name,DEA_No, id FROM Business")
    ids = [i[2] for i in res]
    keys = [str(i[0]) + str(i[1]) for i in res]
    global parsed_business
    parsed_business = dict(zip(keys, ids))

    print("Finished parsing")
    db.close_connection()


def process_cols(cols: [], start):
    i = start
    is_located_query = "INSERT IGNORE INTO is_located VALUES(%(a_id)s, %(b_id)s)"
    is_located_data = []
    reports_data = []
    specifies_data = []

    global parsed_address, parsed_business

    for col in cols:
        elements = col.replace("\n", "").split('\t')

        zip_ = elements[8]
        street, street_num, additional = handle_address(elements[4], elements[5])
        dea = elements[0]
        b_name = elements[2]
        b_id = parsed_business[str(b_name) + str(dea)]

        is_located_data.append({
            "a_id": parsed_address[str(zip_) + str(street) + str(street_num)],
            "b_id": b_id
        })

        bus_act = elements[1]
        role = "REPORTER"

        reports_data.append({
            "business_id": b_id,
            "transaction_id": i + 1,
            "bus_act": bus_act,
            "role": role
        })


        zip_ = elements[18]
        street, street_num, additional = handle_address(elements[14], elements[15])
        dea = elements[10]
        b_name = elements[12]
        b_id = parsed_business[str(b_name) + str(dea)]

        is_located_data.append({
            "a_id": parsed_address[str(zip_) + str(street) + str(street_num)],
            "b_id": b_id
        })

        bus_act = elements[11]
        role = "BUYER"

        reports_data.append({
            "business_id": b_id,
            "transaction_id": i + 1,
            "bus_act": bus_act,
            "role": role
        })

        specifies_data.append({
            "transaction_id": i + 1,
            "ndc_no": str(elements[22])
        })

        i += 1

    db = Database()
    print(len(is_located_data))
    is_located_data = [dict(t) for t in {tuple(d.items()) for d in is_located_data}]
    print(len(is_located_data))

    # db.querymany(is_located_query, is_located_data)
    # db.querymany(reports_query, reports_data)
    # db.querymany(specifies_query, specifies_data)

    with open("../data/temp/is_located.txt", "w+") as file:
        file.truncate()
        for item in is_located_data:
            file.write('{},{}\n'.format(item["a_id"], item["b_id"]))

    db.query("LOAD DATA LOCAL INFILE '../data/temp/is_located.txt' INTO TABLE is_located "
             "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

    with open("../data/temp/reports.txt", "w+") as file:
        file.truncate()
        for item in reports_data:
            file.write('{},{},{},{}\n'.format(item["business_id"], item["transaction_id"], item["bus_act"], item["role"]))

    db.query("LOAD DATA LOCAL INFILE '../data/temp/reports.txt' INTO TABLE reports "
             "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

    with open("../data/temp/specifies.txt", "w+") as file:
        file.truncate()
        for item in specifies_data:
            file.write('{},{}\n'.format(item["transaction_id"], item["ndc_no"]))

    db.query("LOAD DATA LOCAL INFILE '../data/temp/specifies.txt' INTO TABLE specifies "
             "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")




if __name__ == '__main__':
    # file_path = "../data/arcos_all_washpost.tsv"
    file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"

    parse_db()
    db = Database()
    queries = db_parser.transform_sql("./sql/create_rel_tables.sql")
    db.query_all(queries)

    with open(file_path, 'r') as file:
        chunk = 2000000
        lines = sum(1 for i in open(file_path, 'rb'))

        print("number of columns: {}".format(lines))

        chunk_amount = math.ceil(float(lines) / chunk)
        print("{}".format(chunk_amount))

        i = 0

        start_time = time.time()

        for a_chunk in range(chunk_amount):

            output = []
            j = 0
            for line in file:

                if i == 0:
                    i += 1
                    continue
                output.append(line)
                i += 1
                j += 1

                if j >= chunk:
                    break

            print("{} id: {}".format(i, i - j))
            process_cols(output, i - j)
