file_path = "../data/arcos_all_washpost.tsv"
#file_path = "../data/arcos-az-maricopa-04013-itemized.tsv"

class Sorter:

    @staticmethod
    def start_sorting(): 
        global file_path   
        with open(file_path, 'r') as file:

            for col in file:
                elements = col.replace("\n", "").split('\t')
                if elements[7] == elements[17]:
                    with open(elements[7], "a+") as state_file:
                        state_file.write(elements[7])
                else:
                    with open("else", "a+") as placeholder_file:
                        placeholder_file.write(placeholder_file)