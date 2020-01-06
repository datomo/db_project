import json

from Helper import Helper
from database import Database


class DataAnalyzer:
    def __init__(self):
        Helper.create_folder("results")
        self.db = Database()

        self.get_crime_data()

    def get_crime_data(self):
        zip_list = self.db.select("SELECT zip, count(*) FROM address "
                                  "INNER JOIN (occurred_at "
                                  "INNER JOIN crime ON occurred_at.crime_id = crime.id) "
                                  "ON address.id = occurred_at.address_id "
                                  "group by zip "
                                  "order by zip;")

        zip_dict = dict(zip_list)

        max_val = max(zip_dict.values())

        zip_dict = {k: round(v / max_val, 4) for k, v in zip_dict.items()}

        with open("results/crime.json", "w+") as file:
            file.write(json.dumps(zip_dict))


def main():
    d = DataAnalyzer()


main()
