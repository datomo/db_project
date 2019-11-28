from itertools import islice

from database import Database

initalize_tables = False
file_path = "../data/crime-data_crime-data_crimestat.csv"

def main():
    db = Database()

    with open(file_path, 'r') as file:
        for line in islice(file, 1, None):
            data = line[:-1].replace('"', '').split(",")
            print(data[4])

    db.close_connection()



main()