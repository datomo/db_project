import os

file_path = "../data/arcos_all_washpost.tsv"
#file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"

class Sorter:

    @staticmethod
    def start_sorting(): 
        file_prefix = "states"
        if not os.path.exists(file_prefix):
            os.makedirs(file_prefix)

        global file_path   
        with open(file_path, 'r') as file:

            for col in file:
                elements = col.replace("\n", "").split('\t')
                if elements[7] == elements[17]:
                    with open(file_prefix + "/" + elements[7] + ".txt", "a+") as state_file:
                        state_file.write(elements[7])
                else:
                    with open(file_prefix + "/else.txt", "a+") as placeholder_file:
                        placeholder_file.write(placeholder_file)