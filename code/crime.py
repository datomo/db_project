from itertools import islice

from database import Database


class Crime:

    @staticmethod
    def add_data(db: Database):
        file_path = "../data/crime-data_crime-data_crimestat.csv"
        i = 0

        with open(file_path, 'r') as file:
            chunk = 1000
            for line in islice(file, 1, None):
                data = line[:-1].replace('"', '').split(",")
                data = ["NULL" if x == "" else x for x in data]
                words = data[4].split(" ")
                words = [' '.join(words[:2]), ' '.join(words[-2:])]

                db.query("INSERT INTO Crime VALUES('{}',{},'{}','{}','{}','{}')".format("c" + str(i), data[0], data[6], data[1], data[2],
                                                              data[3]))
                if i % chunk == 0:
                    print("finished {}".format(i))
                i += 1
                '''print("INSERT INTO Crime VALUES({},{},{},{},{},{})".format("c" + str(i), data[0], data[6], data[1], data[2],
                                                                     data[3]))'''