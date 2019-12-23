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

    d_query = "INSERT INTO Drug(" \
              "ndc_no, " \
              "combined_labeler_name," \
              "dos_str," \
              "calc_base_wt_in_gm," \
              "product_name," \
              "strength," \
              "drug_code," \
              "drug_name," \
              "ingredient_name," \
              "mme_conversion_factor) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    def __init__(self):
        self.address_folder = self.target_folder.format("address")
        self.business_folder = self.target_folder.format("businesses")
        self.drug_folder = self.target_folder.format("drug")

        self.db.drop_table("is_located")
        self.db.drop_table("address")
        self.db.drop_table("business")

        Helper.create_tables("./sql/create_address.sql", self.db)
        Helper.create_tables("./sql/create_business.sql", self.db)
        Helper.create_tables("./sql/create_drug.sql", self.db)
        logging.debug("Finished creating tables")

        addresses = Helper.merge_files_in_unique_list(
            Helper.get_files_in_folder(self.address_folder),
            self.address_folder)

        self.db.querymany(self.a_query, Helper.tuplelist_to_listlist(addresses))
        logging.debug("Finished inserting addresses")

        business = Helper.merge_files_in_unique_list(
            Helper.get_files_in_folder(self.business_folder),
            self.business_folder)

        self.db.querymany(self.b_query, Helper.tuplelist_to_listlist(business))
        logging.debug("Finished inserting businesses")

        drug = Helper.merge_files_in_unique_list(
            Helper.get_files_in_folder(self.drug_folder),
            self.drug_folder)

        drug = Helper.tuplelist_to_listlist(drug)
        # wrong value
        # drug.pop(3985)
        self.db.querymany(self.d_query, drug)
        logging.debug("Finished inserting drug")


def main():
    i = Inserter()


main()
