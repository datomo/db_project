import math
import os
import pickle
import shutil
import time

from pill import Pill

file_path = "../data/arcos_all_washpost.tsv"
# file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"
file_prefix = "./states"
business_location = "./business"


def process_cols(cols):
    # addresses = {}
    businesses = {}

    for col in cols:
        elements = col.replace("\n", "").split('\t')
        positions = [4, 5, 3, 8, 6, 9, 7]
        a_1 = process_address(elements, positions)
        positions = [14, 15, 13, 18, 16, 19, 17]
        a_2 = process_address(elements, positions)

        dea = elements[0]
        b_name = elements[2]
        b_1 = process_business(b_name, dea)

        dea = elements[10]
        b_name = elements[12]
        b_2 = process_business(b_name, dea)

        state_1 = str(a_1["state"]) if a_1["state"] else "#"
        state_2 = str(a_2["state"]) if a_2["state"] else "#"
        zip_1 = str(a_1["zip"]) if a_1["zip"] else "#"
        zip_2 = str(a_2["zip"]) if a_1["zip"] else "#"
        city_1 = str(a_1["city"]) if a_1["city"] else "#"
        city_2 = str(a_2["city"]) if a_1["city"] else "#"

        key_1 = "{}-{}-{}".format(state_1, city_1.replace("/", "##"), zip_1)
        key_2 = "{}-{}-{}".format(state_2, city_2.replace("/", "##"), zip_2)
        '''if key_1 not in addresses:
            addresses[key_1] = []'''
        if key_1 not in businesses:
            businesses[key_1] = []
        '''if key_2 not in addresses:
            addresses[key_2] = []'''
        if key_2 not in businesses:
            businesses[key_2] = []

        # addresses[key_1].append(a_1)
        # addresses[key_2].append(a_2)
        businesses[key_1].append(b_1.update(a_1))
        businesses[key_2].append(b_2.update(a_2))

    # save_as_pickle(addresses, file_prefix)
    save_as_pickle(businesses, business_location)


def save_as_pickle(obj, location):
    for k in obj:
        state_file = open(location + "/" + k + ".pkl", "ab+")
        for address in obj[k]:
            pickle.dump(address, state_file)
        state_file.flush()
        state_file.close()


def process_business(b_name, dea):
    return {
        'business_name': b_name,
        'reviewed_business_id': None,
        'dea_no': dea
    }


def process_address(elements, positions):
    add1 = elements[positions[0]]
    add2 = elements[positions[1]]

    (street_name, street_num, additional) = Pill.handle_address(add1, add2)
    if not additional and not elements[positions[2]] or (additional == "null" and additional == "null"):
        additional = None
    elif additional and elements:
        additional = "\n".join([additional, elements[positions[2]]])
    else:
        additional = additional if additional != "null" and additional else elements[positions[2]]
    # buy_add
    return {
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
    }


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


def split_address(add) -> (str, str):
    parts = add.replace(" ST", " STREET").replace(" RD.", " Road").replace(" RD", " Road").split(" ")
    num = ""
    street = ""

    for part in parts:
        if all([i.isdigit() for i in part]) or len(part) == 1 or len(part.replace(".", "")) == 1:
            num += part + " "
        else:
            street += part + " "

    return street.strip(), num.strip()


if __name__ == '__main__':
    if os.path.exists(business_location):
        shutil.rmtree(business_location)
    os.makedirs(business_location)

    with open(file_path, 'r') as file:
        chunk = 2000000
        # lines = sum(1 for i in open(file_path, 'rb'))
        lines = 178598027
        print("number of columns: {}".format(lines))

        chunk_amount = math.ceil(float(lines) / chunk)
        print("{}".format(chunk_amount))

        i = 0

        start_time = time.time()

        results = []
        for a_chunk in range(chunk_amount):
            start_time = time.time()
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

            process_cols(output)
            print("finished chunk {} after {}s".format(a_chunk, round(time.time() - start_time, 2)))
