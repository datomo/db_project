import json

from database import Database


class DataAnalyzer:
    def __init__(self):
        self.db = Database()

        self.get_population_data()

    def get_population_data(self):
        zip_list = self.db.select("SELECT zip, population FROM Zip_Population WHERE city = 'Phoenix'")

        zip_dict = dict(zip_list)

        '''max_val = max(zip_dict.values())

        zip_dict = {k: round(v / max_val, 4) for k, v in zip_dict.items()}'''

        with open("results/population.json", "w+") as file:
            file.write(json.dumps(zip_dict))


def main():
    d = DataAnalyzer()


main()
