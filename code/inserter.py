import logging

from Helper import Helper
from database import Database

logging.basicConfig(level=logging.DEBUG)


class Inserter:
    target_folder = "./output/{}/filtered"
    db = Database()

    a_query = "INSERT INTO Address(" \
              "zip, city, street, street_number, county, state, address_name, longitude, latitude, addl_co_info) VALUES(" \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    b_query = "INSERT INTO Business(business_name, reviewed_business_id, dea_no) VALUES(%s, %s, %s)"

    def __init__(self):
        self.address_folder = self.target_folder.format("address")
        self.business_folder = self.target_folder.format("businesses")

        self.db.drop_table("is_located")
        self.db.drop_table("address")
        self.db.drop_table("business")

        Helper.create_tables("./sql/create_address.sql", self.db)
        Helper.create_tables("./sql/create_business.sql", self.db)
        logging.debug("Finished creating tables")

        addresses = Helper.merge_files_in_unique_list(
            Helper.get_files_in_folder(self.address_folder),
            self.address_folder)

        # print(addresses)

        self.db.querymany(self.a_query, Helper.tuplelist_to_listlist(addresses))
        logging.debug("Finished inserting addresses")

        business = Helper.merge_files_in_unique_list(
            Helper.get_files_in_folder(self.business_folder),
            self.business_folder)

        self.db.querymany(self.b_query, Helper.tuplelist_to_listlist(business))
        logging.debug("Finished inserting businesses")


def main():
    i = Inserter()

main()
