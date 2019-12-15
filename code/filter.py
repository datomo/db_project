from os import listdir
from os.path import isfile, join

from Helper import Helper


class Filter:
    filter_folder = "filtered"

    def __init__(self):
        self.source_address = "./output/{}".format("address")
        self.source_business = "./output/{}".format("businesses")

        Helper.clear_folder(self.source_business + "/" + self.filter_folder)
        Helper.clear_folder(self.source_address + "/" + self.filter_folder)

        files_address = Helper.get_files_in_folder(self.source_address)

        files_business = Helper.get_files_in_folder(self.source_business)

        [Helper.filter_file(f, self.source_address, self.filter_folder) for f in files_address]
        [Helper.filter_file(f, self.source_business, self.filter_folder) for f in files_business]


def main():
    f = Filter()


main()
