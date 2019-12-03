from itertools import islice

from database import Database
import time


class Crime:

    @staticmethod
    def add_data(db: Database):
        file_path = "../data/crime-data_crime-data_crimestat.csv"
        i = 1

        lines = sum(1 for i in open(file_path, 'rb'))
        print("number of columns: {}".format(lines))

        with open(file_path, 'r') as file:
            chunk = 10000
            found_address = 0

            o_query = "INSERT INTO occured_at VALUES(%(a_id)s, %(c_id)s)"
            a_query = """INSERT INTO Address VALUES( %(id)s, %(zip)s, 
            %(city)s, %(street)s, %(number)s, %(county)s, 
            %(state)s, %(null)s, %(null)s, %(null)s, %(null)s)"""
            c_query = "INSERT INTO Crime VALUES(%(id)s,%(inc)s,%(premise)s,%(on)s,%(to)s,%(cat)s)"
            c_data = []
            o_data = []
            a_data = []

            start = time.time()

            for line in islice(file, 1, None):
                data = line[:-1].replace('"', '').split(",")
                data = ["NULL" if x == "" else x for x in data]
                words = data[4].split(" ")
                words = [' '.join(words[:2]), ' '.join(words[-2:])]

                a_id = "a" + str(i)
                c_id = "c" + str(i)
                executed = False

                # exists = db.exists("Address", {"'zip' = {}".format(data[5]), "'street' = '{}'".format(words[1]), "'street_number' = '{}'".format(words[0])})


                res = db.select_one(
                    "SELECT address_id FROM Address WHERE zip = {} AND street = '{}' AND street_number = '{}'".format(
                        data[5],
                        words[1],
                        words[0]))
                if res:
                    id = res[0]
                    found_address += 1
                else:
                    id = None

                if i % chunk == 0:
                    end = time.time()
                    print("time passed: {}".format(end - start))
                    print("executed {} rows from {}: {}%".format(i, lines, round(i / lines * 100, 2)))
                    print("found addresses in chunk: " + str(found_address))
                    found_address = 0

                    db.querymany(c_query, c_data)
                    db.querymany(a_query, a_data)
                    db.querymany(o_query, o_data)

                    c_data = []
                    o_data = []
                    a_data = []

                    executed = True
                    start = time.time()

                else:
                    # address additional values
                    if not id:
                        a_data.append({
                            'id': a_id,
                            'zip': data[5],
                            'street': words[1],
                            'number': words[0],
                            'city': 'pheonix',
                            'county': 'maricopa',
                            'state': 'arizona',
                            'null': None
                        })

                c_data.append({
                    'id': c_id,
                    'inc': data[0],
                    'premise': data[6],
                    'on': data[1],
                    'to': data[2],
                    'cat': data[3]
                })

                if id:
                    a_id = id

                o_data.append({
                    # id is found at start
                    'a_id': a_id,
                    'c_id': c_id
                })

                i += 1

            if not executed:
                db.query(c_query)
                db.query(a_query)
                db.query(o_query)
                print("finished...")
