import logging

from Helper import Helper

logging.basicConfig(level=logging.DEBUG)


class Splitter:
    input_file = "output.pkl"

    def __init__(self):
        self.target_address = "./output/{}".format("address")
        self.target_business = "./output/{}".format("businesses")
        self.target_drug = "./output/{}".format("drug")
        Helper.clear_folder(self.target_address)
        Helper.clear_folder(self.target_business)
        Helper.clear_folder(self.target_drug)

        Helper.chunk_file_pkl(2000000, "./output/{}".format(self.input_file), self.split_cols)

    def split_cols(self, cols):
        addresses = {}
        businesses = set()
        drugs = set()
        logging.debug("started on cols")
        for col in cols:
            # write addresses with pre filtering in dict

            a_1 = col["a_1"]
            a_2 = col["a_2"]
            # 5 = state, 0 = zip, city = 4
            key_1 = Helper.generate_key((a_1[0], a_1[5], a_1[4]))
            key_2 = Helper.generate_key((a_2[0], a_2[5], a_2[4]))
            # fill address in dedicated key
            if key_1 not in addresses:
                addresses[key_1] = set()
            addresses[key_1].add(a_1)

            if key_2 not in addresses:
                addresses[key_2] = set()
            addresses[key_2].add(a_2)
            
            b_1 = col["b_1"]
            b_2 = col["b_2"]
            # only add to business
            businesses.add(b_1)
            businesses.add(b_2)

            drugs.add(col["drug"])
        logging.debug("finished cols")

        Helper.write_dict_sets(addresses, self.target_address)
        Helper.write_list(list(businesses), self.target_business, "businesses")
        Helper.write_list(list(drugs), self.target_drug, "drug")


def main():
    splitter = Splitter()


main()
