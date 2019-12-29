from Helper import Helper
from database import Database


class Yelp_Address:
    file_name = "./output/yelp/yelp.pkl"
    a_query = "INSERT INTO Address(zip,city,street,street_number,county,state,address_name,longitude,latitude,addl_co_info) VALUES(" \
              "%s, " \
              "%s, " \
              "%s, " \
              "%s, " \
              "%s, " \
              "%s, " \
              "%s, " \
              "%s, " \
              "%s, " \
              "%s)"

    def __init__(self):
        self.db = Database()

        Helper.chunk_file_pkl(2000000, self.file_name, self.parse_cols)

    def parse_cols(self, cols):

        self.get_address_ids()
        a_data = set()

        for col in cols:
            # print(col)
            street, street_number = Helper.parse_yelp_street(col["address"])

            print(street)
            print(street_number)

            key = Helper.generate_key([street, street_number, col["postal_code"], col["state"]], delimiter="", remove_whitespace=True,
                                        replace_char="None")

            if key not in self.addresses:
                a_data.add((
                    col["postal_code"],
                    col["city"],
                    street,
                    street_number,
                    None,
                    col["state"],
                    None,
                    col['longitude'],
                    col['latitude'],
                    None
                ))
        self.db.querymany(self.a_query, list(a_data))






    def get_address_ids(self):
        self.addresses = self.db.select("SELECT street, street_number, zip, state, id FROM Address;")
        self.addresses = Helper.parse_tuplelist_to_dict(self.addresses)

def main():
    y = Yelp_Address()

main()
