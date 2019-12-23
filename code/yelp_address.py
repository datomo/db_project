from Helper import Helper
from database import Database


class Yelp_Address:
    file_name = "./output/yelp/yelp.pkl"
    a_query = "INSERT IGNORE INTO Address(zip,city,street,street_number,county,state,address_name,longitude,latitude,addl_co_info) VALUES(" \
              "%(zip)s, " \
              "%(city)s, " \
              "%(street)s, " \
              "%(street_number)s, " \
              "%(county)s, " \
              "%(state)s, " \
              "%(address_name)s, " \
              "%(longitude)s, " \
              "%(latitude)s, " \
              "%(addl_co_info)s)"

    def __init__(self):
        self.db = Database()

        Helper.chunk_file_pkl(2000000, self.file_name, self.parse_cols)

    def parse_cols(self, cols):

        self.get_address_ids()
        a_data = []

        for col in cols:
            # print(col)
            street, street_number = self.parse_street(col["address"])

            print(street)
            print(street_number)

            key = Helper.generate_key([street, street_number, col["zip"], col["state"]], delimiter="", remove_whitespace=True,
                                        replace_char="None")

            if key not in self.addresses:
                a_data.append({
                    'zip': col["postal_code"],
                    'city': col["city"],
                    'street': street,
                    'street_number': street_number,
                    'county': None,
                    'state': col["state"],
                    'address_name': None,
                    'longitude': col['longitude'],
                    'latitude': col['latitude'],
                    'addl_co_info': None
                })

            self.db.querymany(self.a_query, a_data)



    def parse_street(self, street):
        splits = street.split()

        street = []
        number =  []


        for split in splits:
            if(split.isdigit()):
                number.append(split)
            elif(len(split) <= 1):
                number.append(split)
            else:
                street.append("Street" if split == "St" else split)

        return " ".join(street) if number != "" else None, " ".join(number) if number != "" else None


    def get_address_ids(self):
        self.addresses = self.db.select("SELECT street, street_number, zip, state, id FROM Address;")
        self.addresses = Helper.parse_tuplelist_to_dict(self.addresses)

def main():
    y = Yelp_Address()

main()
