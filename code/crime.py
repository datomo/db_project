from itertools import islice

from database import Database
import time


class Crime:
    # 21833 error for inc_num deleted "-"
    @staticmethod
    def add_data(db: Database):
        file_path = "../data/crime-data_crime-data_crimestat.csv"
        i = 1
        addresses = {}

        lines = sum(1 for i in open(file_path, 'rb'))
        print("number of columns: {}".format(lines))

        with open(file_path, 'r') as file:
            chunk = 100000
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
                data = [None if x == "" else x for x in data]
                words = data[4].split(" ")
                words = [' '.join(words[:2]), ' '.join(words[-2:])]

                a_id = i
                c_id = i
                executed = False

                # exists = db.exists("Address", {"'zip' = {}".format(data[5]), "'street' = '{}'".format(words[1]), "'street_number' = '{}'".format(words[0])})

                '''res = db.select_one(
                    "SELECT address_id FROM Address WHERE zip = {} AND street = '{}' AND street_number = '{}'".format(
                        data[5],
                        words[1],
                        words[0]))'''
                zip = data[5] if data[5] else "NULL"
                key = str(zip + words[1] + words[0])

                if key in addresses:
                    res = addresses[key]
                else:
                    res = None

                if res:
                    id = res
                    found_address += 1
                else:
                    id = None

                if i % chunk == 0:
                    end = time.time()
                    print("time passed: {}".format(round(end - start, 3)))
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
                            'city': 'PHEONIX',
                            'county': 'MARICOPA',
                            'state': 'AZ',
                            'null': None
                        })

                        zip = data[5] if data[5] else "NULL"

                        key = str(zip + words[1] + words[0])
                        addresses[key] = a_id

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
                db.querymany(c_query, c_data)
                db.querymany(a_query, a_data)
                db.querymany(o_query, o_data)
                print("finished...")

            with open("../data/temp/c_address.txt", "w+") as file:
                for k,v in addresses.items():
                    file.write('{}#{}\n'.format(k,v))
