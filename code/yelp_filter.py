import json

from Helper import Helper
from database import Database


class Yelp:
    file_path = "../data/yelp_academic_dataset_business.json"

    def __init__(self):
        self.db = Database()

        Helper.clear_folder("output/yelp")
        i = 1
        output = []
        with open(self.file_path, encoding="utf8", mode="r") as file:
            for line in file:
                obj = json.loads(line)
                obj["id"] = i
                output.append(obj)
                i += 1
        Helper.write_list(output, "output/yelp", "yelp")



def main():
    y = Yelp()


main()
