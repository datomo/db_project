import logging
import time

logging.basicConfig(level=logging.DEBUG)

from Helper import Helper
from database import Database


class Crime_Address:
    a_query = "INSERT INTO Address (zip, street, street_number,city, county, state, address_name, longitude, latitude, addl_co_info) VALUES( " \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s)"

    c_query = "INSERT INTO Crime (id, inc_number, premise_type, occurred_on, occurred_to, ucr_crime_category) VALUES(" \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s," \
              "%s)"
    o_query = "INSERT INTO occurred_at VALUES(" \
              "%s," \
              "%s)"

    def __init__(self):
        self.db = Database()
        self.get_address_ids()

        self.db.drop_table("occurred_at")
        self.db.drop_table("crime")

        Helper.create_tables("./sql/create_crime.sql", self.db)
        Helper.create_tables("./sql/create_occurred_at.sql", self.db)

        self.time = time.time()
        self.start = time.time()
        Helper.chunk_file_pkl(2000000, "./output/crime/crime.pkl", self.parse_cols)
        logging.debug("finished parsing all csv after {}s".format(round(time.time() - self.start), 2))

    def get_address_ids(self):
        self.addresses = self.db.select("SELECT street, street_number, zip, state, id FROM Address WHERE state='AZ';")
        self.addresses = Helper.parse_tuplelist_to_dict(self.addresses)

    def parse_cols(self, lines):
        a_data = set()
        c_data = []
        for line in lines:
            data = line["address"]
            a_key = Helper.generate_key([data[1], data[2], data[0], data[5]], delimiter="", remove_whitespace=True,
                                        replace_char="None")

            if a_key not in self.addresses:
                a_data.add(line["address"])

        self.db.querymany(self.a_query, a_data)

        self.get_address_ids()

        o_data = []
        for line in lines:
            data = line["address"]
            a_key = Helper.generate_key([data[1], data[2], data[0], data[5]], delimiter="", remove_whitespace=True,
                                        replace_char="None")

            a_id = self.addresses[a_key]

            c_data.append((line["id"],) + line["crime"])

            o_data.append((a_id,) + (line["id"],))

        self.db.querymany(self.c_query, c_data)

        self.db.querymany(self.o_query, o_data)


def main():
    c = Crime_Address()


main()
