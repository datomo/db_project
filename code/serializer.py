import math
import os
import pickle
import time

from old.pill import Pill

file_path = "../data/arcos_all_washpost.tsv"
# file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"
output_folder = "./output"
output_file = "output"


def process_cols(cols, i):
    columns = []
    for col in cols:
        elements = col.replace("\n", "").split('\t')
        positions = [4, 5, 3, 8, 6, 9, 7]
        a_1 = process_address(elements, positions)
        positions = [14, 15, 13, 18, 16, 19, 17]
        a_2 = process_address(elements, positions)
        report = process_report(elements)
        dea = elements[0]
        b_name = elements[2]
        b_1 = process_business(b_name, dea)

        dea = elements[10]
        b_name = elements[12]
        b_2 = process_business(b_name, dea)

        drug = process_drug(elements, str(elements[22]))

        columns.append(
            {'a_1': a_1, 'a_2': a_2, 'b_1': b_1, 'r_1': elements[1], 'b_2': b_2, 'r_2': elements[11], 'r': report,
             'drug': drug, 'id': i})
        i += 1
    save_as_pickle(columns)


def save_as_pickle(cols):
    state_file = open(output_file + "/" + output_folder + ".pkl", "ab+")
    for col in cols:
        pickle.dump(col, state_file)
    state_file.flush()
    state_file.close()


def process_drug(elements, ndc_id):
    return replace_null((
        ndc_id,
        elements[38],
        elements[41],
        elements[31],
        elements[34],
        elements[29],
        elements[21],
        elements[23],
        elements[35],
        elements[37]
    ))


def process_report(elements):
    return replace_null((
        elements[33],
        elements[28],
        elements[26],
        elements[20],
        elements[27],
        elements[40],
        elements[30],
        elements[39],
        elements[36],
        elements[25],
        elements[24],
        elements[32]
    ))


def replace_null(obj):
    res = tuple()
    for v in obj:
        res += (v,) if v and v != "null" else (None,)
    return res


def process_business(b_name, dea):
    return replace_null((
        b_name,
        dea
    ))


def process_address(elements, positions):
    add1 = elements[positions[0]]
    add2 = elements[positions[1]]

    (street_name, street_num, additional) = handle_address(add1, add2)
    if not additional and not elements[positions[2]] or (additional == "null" and additional == "null"):
        additional = None
    elif additional and elements:
        additional = "\n".join([additional, elements[positions[2]]])
    else:
        additional = additional if additional != "null" and additional else elements[positions[2]]
    # buy_add
    return replace_null((
        elements[positions[3]],
        elements[positions[4]],
        street_name,
        street_num,
        elements[positions[5]],
        elements[positions[6]],
        additional,
        None,
        None,
        additional
    ))


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

        street = street.strip()
        num = num.strip()
    return street if street != "" else None, num if num != "" else None


if __name__ == '__main__':
    if os.path.isfile("{}/{}.pkl".format(output_folder, output_file)):
        os.remove("{}/{}.pkl".format(output_folder, output_file))

    with open(file_path, 'r') as file:
        chunk = 2000000
        # lines = sum(1 for i in open(file_path, 'rb'))
        lines = 178598027
        print("number of columns: {}".format(lines))

        chunk_amount = math.ceil(float(lines) / chunk)
        print("{}".format(chunk_amount))

        # id cant be 0 needs to start at 1
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

            process_cols(output, i - chunk)
            print("finished chunk {} after {}s".format(a_chunk, round(time.time() - start_time, 2)))
