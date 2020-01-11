from Helper import Helper
from database import Database


class Yelp_Business:
    file_name = "./output/yelp/yelp.pkl"
    a_query = "INSERT INTO Business(business_name, DEA_No) VALUES(" \
              "%s, " \
              "%s)"

    def __init__(self):
        self.db = Database()

        Helper.chunk_file_pkl(2000000, self.file_name, self.parse_cols)

    def parse_cols(self, cols):

        self.get_business_ids()
        b_data = set()

        for col in cols:

            if col["name"].replace(" ", "") not in self.businesses:
                b_data.add((
                    col["name"],
                    None
                ))

        self.db.querymany(self.a_query, list(b_data))


    def get_business_ids(self):
        self.businesses = self.db.select("SELECT business_name, id FROM Business;")
        self.businesses = Helper.parse_tuplelist_to_dict(self.businesses)

def main():
    y = Yelp_Business()

main()