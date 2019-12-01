from itertools import islice

from database import Database


class Crime:

    @staticmethod
    def add_data(db: Database):
        file_path = "../data/crime-data_crime-data_crimestat.csv"
        i = 1
        query = ""

        with open(file_path, 'r') as file:
            chunk = 100
            executed = False
            for line in islice(file, 1, None):
                data = line[:-1].replace('"', '').split(",")
                data = ["NULL" if x == "" else x for x in data]
                words = data[4].split(" ")
                words = [' '.join(words[:2]), ' '.join(words[-2:])]

                #print(query)

                if i != 1 and  i % chunk == 0:
                    print(query)
                    db.query(query)
                    executed = True

                if i % chunk == 0 or i == 1:
                    query = "INSERT INTO Crime"
                    #print(query)
                    query = "{} VALUES('{}',{},'{}','{}','{}','{}')".format(query, "c" + str(i), data[0], data[6], data[1], data[2],
                                                                   data[3])
                    #print(query)

                else:
                    query = "{}, ('{}',{},'{}','{}','{}','{}')".format(query, "c" + str(i), data[0], data[6], data[1], data[2],
                                                           data[3])
                    #print(query)
                    executed = False

                i += 1
            if not executed:
                db.query(query)
                print(query)