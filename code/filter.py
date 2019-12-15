from Helper import Helper


class Filter:
    filter_folder = "filtered"

    def __init__(self):
        self.source_address = "./output/{}".format("address")
        self.source_business = "./output/{}".format("businesses")
        self.source_drug = "./output/{}".format("drug")

        # Helper.clear_folder(self.source_business + "/" + self.filter_folder)
        # Helper.clear_folder(self.source_address + "/" + self.filter_folder)
        Helper.clear_folder(self.source_drug + "/" + self.filter_folder)

        files_address = Helper.get_files_in_folder(self.source_address)
        files_business = Helper.get_files_in_folder(self.source_business)
        files_drug = Helper.get_files_in_folder(self.source_drug)

        # [Helper.filter_file(f, self.source_address, self.filter_folder) for f in files_address]
        # [Helper.filter_file(f, self.source_business, self.filter_folder) for f in files_business]
        [Helper.filter_file(f, self.source_drug, self.filter_folder, filter_pos=0) for f in files_drug]


def main():
    f = Filter()


main()
