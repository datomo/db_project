import json

from Helper import Helper
from database import Database


class DataAnalyzerPill:
    def __init__(self):
        self.db = Database()

        self.get_crime_data()

    def get_crime_data(self):
        triple_zip_list = self.db.select("SELECT zip, quantity, dosage_unit "
                                         "FROM address INNER JOIN "
                                         "(reports INNER JOIN report ON reports.transaction_id = report.id) "
                                         "ON address.id = reports.address_id "
                                         "WHERE address.state = 'AZ' && address.city = 'Phoenix' && role = 'BUYER' && zip LIKE '850%'"
                                         "order by zip;")

        zip_list = [[k, q * d] for (k, q, d) in triple_zip_list]

        zip_dict = {}
        for (zip, v) in zip_list:
            if zip in zip_dict:
                zip_dict[zip] = zip_dict[zip] + v
            else:
                zip_dict[zip] = v

        zip_dict = Helper.normalize_data(zip_dict)
        with open("results/pill.json", "w+") as file:
            file.write(json.dumps(zip_dict))


def main():
    d = DataAnalyzerPill()


main()
