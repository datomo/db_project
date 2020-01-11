from Helper import Helper
from database import Database


class ZipPopulation:
    file = "../data/plz-population-phoenix.csv"
    p_query = "INSERT INTO Zip_Population VALUES(%s, %s, \"Phoenix\")"



    def __init__(self):
        self.db = Database()
        self.db.drop_table("Zip_Population")
        Helper.create_tables("./sql/create_zip_population.sql", self.db)

        p_data = []
        with open(self.file, "r") as file:
            next(file)
            for f in file:
                splits = f.replace("\n", "").split(";")
                p_data.append([splits[0], splits[1]])
        print(p_data)

        self.db.querymany(self.p_query, p_data)


def main():
    p = ZipPopulation()


main()
