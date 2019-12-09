import os
import time
import math

file_path = "../data/arcos_all_washpost.tsv"
# file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"
file_prefix = "states"


class Sorter:

    @staticmethod
    def start_sorting():
        global file_prefix
        if not os.path.exists(file_prefix):
            os.makedirs(file_prefix)

        global file_path
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

                Sorter.process_cols(output)
                print("Chunk finished {}, time needed".format(a_chunk, round(time.time() - start_time, 2)))

    @staticmethod
    def process_cols(cols, ):
        addresses = {}
        other = []
        global file_prefix

        for col in cols:
            elements = col.replace("\n", "").split('\t')
            if elements[7] == elements[17] and elements[7]:
                addresses[elements] = col
            else:
                other += other

            for k, v in addresses:
                with open(file_prefix + "/" + k + ".txt", "a+") as state_file:
                    for address in addresses[k]:
                        state_file.write(address)

            with open(file_prefix + "/else.txt", "a+") as placeholder_file:
                for o in other:
                    placeholder_file.write(o)
