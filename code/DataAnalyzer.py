import json

from Helper import Helper
from database import Database


class DataAnalyzer:
    def __init__(self):
        Helper.create_folder("results")
        self.db = Database()

        self.get_crime_data()
        self.get_population_data()

        self.proportize_data()
        self.write_data()

    def get_crime_data(self):
        self.crime_list = self.db.select("SELECT zip, count(*) FROM address "
                                         "INNER JOIN (occurred_at "
                                         "INNER JOIN crime ON occurred_at.crime_id = crime.id) "
                                         "ON address.id = occurred_at.address_id WHERE zip LIKE '850%'"
                                         "group by zip "
                                         "order by zip;")

        self.crime_dict = dict(self.crime_list)

    def get_population_data(self):
        self.population_list = self.db.select("SELECT zip, population FROM Zip_Population WHERE city = 'Phoenix'")

        self.population_dict = dict(self.population_list)

    # calculate the crime rate by acounting the population of the zip location
    def proportize_data(self):

        self.crime_prop_dict = {}

        for key in self.crime_dict:
            if key in self.population_dict:
                self.crime_prop_dict[key] = self.crime_dict[key] / (self.population_dict[key]/1000)

        self.crime_prop_dict = Helper.normalize_data(self.crime_prop_dict)
        self.crime_dict = Helper.normalize_data(self.crime_dict)
        self.population_dict = Helper.normalize_data(self.population_dict)


    def write_data(self):
        with open("results/crime_population.json", "w+") as file:
            file.write(json.dumps(self.crime_prop_dict))
        with open("results/crime.json", "w+") as file:
            file.write(json.dumps(self.crime_dict))
        with open("results/population.json", "w+") as file:
            file.write(json.dumps(self.population_dict))


def main():
    d = DataAnalyzer()


main()
