import logging

from Helper import Helper
from database import Database


class Yelp_Add_Info_Business:
    file_name = "./output/yelp/yelp.pkl"

    abi_query = "INSERT INTO Add_Business_Info(reviewed_business_id, is_open, review_count, avg_stars, categories, hours, attributes) VALUES(" \
                "%s, " \
                "%s, " \
                "%s, " \
                "%s, " \
                "%s, " \
                "%s, " \
                "%s)"

    def __init__(self):
        self.db = Database()
        self.db.drop_table("Add_Business_Info")
        Helper.create_tables("./sql/create_table_add_info.sql", self.db)

        self.inserted_ids = {}

        Helper.chunk_file_pkl(2000000, self.file_name, self.parse_cols)

    def parse_cols(self, cols):
        abi_data = []

        for col in cols:

            # check if we already have additional data for this rating
            id = col["business_id"]
            if id not in self.inserted_ids:
                abi_data.append((
                    id,
                    col["is_open"],
                    col["review_count"],
                    col["stars"],
                    col["categories"],
                    str(col["hours"]),
                    str(col["attributes"])
                ))
                self.inserted_ids[id] = col["stars"]
            else:
                # check if we have a different star rating
                if self.inserted_ids[id] != col["stars"]:
                    logging.error("discrepancy for entry with id: {}".format(id))


        self.db.querymany(self.abi_query, abi_data)


def main():
    y = Yelp_Add_Info_Business()

main()
