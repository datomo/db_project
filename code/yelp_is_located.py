from Helper import Helper
from database import Database


class Yelp_Is_Located:
    file_name = "./output/yelp/yelp.pkl"
    a_query = "INSERT INTO is_located(address_id, business_id, reviewed_business_id) VALUES(" \
              "%s, " \
              "%s, " \
              "%s)"

    update_query = "UPDATE is_located SET reviewed_business_id = %s WHERE id = %s"

    def __init__(self):
        self.db = Database()

        self.db.drop_table("is_located")
        Helper.create_tables("./sql/create_is_located.sql", self.db)

        Helper.chunk_file_pkl(2000000, self.file_name, self.parse_cols)

    def parse_cols(self, cols):

        self.get_existing_data()
        i_data = set()
        i_o_data = set()

        for col in cols:

            street, street_number = Helper.parse_yelp_street(col["address"])
            key = Helper.generate_key([street, street_number, col["postal_code"], col["state"]], delimiter="",
                                      remove_whitespace=True,
                                      replace_char="")

            a_id = self.addresses[key]
            b_id = self.businesses[col['name'].replace(" ", "")]

            if str(a_id)+str(b_id) not in self.is_located:
                i_data.add((
                    a_id,
                    b_id,
                    col['business_id']
                ))

            else:
                i_o_data.add((
                    col['business_id'],
                    self.is_located[str(a_id)+str(b_id)]
                ))

        self.db.querymany(self.a_query, list(i_data))
        self.db.querymany(self.update_query, list(i_o_data))


    def get_existing_data(self):
        self.businesses = self.db.select("SELECT business_name, id FROM Business;")
        self.businesses = Helper.parse_tuplelist_to_dict(self.businesses)

        self.addresses = self.db.select("SELECT street, street_number, zip, state, id FROM Address;")
        self.addresses = Helper.parse_tuplelist_to_dict(self.addresses)

        self.is_located = self.db.select("SELECT address_id, business_id, id FROM is_located;")
        self.is_located = Helper.parse_tuplelist_to_dict(self.is_located)

def main():
    y = Yelp_Is_Located()

main()