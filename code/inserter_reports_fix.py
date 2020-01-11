from Helper import Helper
from database import Database


class Pill:
    file = "../data/arcos-az-maricopa-04013-itemized.tsv"
    select_transaction = "SELECT transaction_id, id FROM report WHERE transaction_id IN({})"
    r_query = "INSERT IGNORE INTO reports(address_id,business_id, transaction_id, bus_act, role)  VALUES(%s, %s, %s, %s, %s)"

    def __init__(self):
        self.db = Database()
        self.addresses = self.db.select("SELECT street, street_number, zip, state, id FROM Address where state='AZ';")
        self.addresses = Helper.parse_tuplelist_to_dict(self.addresses)

        self.businesses = self.db.select("SELECT DEA_No, id FROM Business;")
        self.businesses = Helper.parse_tuplelist_to_dict(self.businesses)
        print("got all")

        output = []
        with open(self.file) as file:
            next(file)
            for line in file:
                output.append(line)

        self.process_cols(output, 0)

    def process_cols(self, cols, i):
        columns = []
        ids = []
        trans_ids = set()
        addresses = set()
        businesses = set()
        for col in cols:
            elements = col.replace("\n", "").split('\t')
            positions = [4, 5, 3, 8, 6, 9, 7]
            a_1 = Pill.process_address(elements, positions)
            positions = [14, 15, 13, 18, 16, 19, 17]
            a_2 = Pill.process_address(elements, positions)
            report = Pill.process_report(elements)
            dea = elements[0]
            b_name = elements[2]
            b_1 = Pill.process_business(b_name, dea)

            dea = elements[10]
            b_name = elements[12]
            b_2 = Pill.process_business(b_name, dea)

            drug = Pill.process_drug(elements, str(elements[22]))

            data = {'a_1': a_1, 'a_2': a_2, 'b_1': b_1, 'r_1': elements[1], 'b_2': b_2, 'r_2': elements[11],
                    'r': report,
                    'drug': drug, 'id': i}
            columns.append(data)

            a_key = "".join(str(i) for i in [data['a_1'][2], data['a_1'][3], data['a_1'][0], data['a_1'][5]]) \
                .replace(" ", "")
            if a_key in self.addresses:
                # print("test")
                a_id, b_id = self.get_ids(a_1, b_1)

                ids.append((a_id, b_id, data['r'][0], elements[1], 'REPORTER'))
                trans_ids.add(data['r'][0])

            # else:
                # print("double")
                # print(a_1)

            a_key = "".join(str(i) for i in [data['a_2'][2], data['a_2'][3], data['a_2'][0], data['a_2'][5]]) \
                .replace(" ", "")
            if a_key in self.addresses:
                # print("test")
                a_id, b_id = self.get_ids(a_2, b_2)

                ids.append((a_id, b_id, data['r'][0], elements[11], 'BUYER'))
                trans_ids.add(data['r'][0])

            # else:
                # print("double")
                # print(a_id)
            # print("error")

        print(self.select_transaction.format(str(list(trans_ids))[1:-1]))

        id_trans_ids = self.db.select(self.select_transaction.format(str(list(trans_ids))[1:-1]))
        id_trans_ids = Helper.parse_tuplelist_to_dict(id_trans_ids)

        complete = []
        for entry in ids:
            if entry[2] in id_trans_ids:
                complete.append((entry[0], entry[1], id_trans_ids[entry[2]], entry[3], entry[4]))

        print(complete)

        self.db.querymany(self.r_query, complete)

    def get_ids(self, address, business):
        a_key = "".join(str(i) for i in [address[2], address[3], address[0], address[5]]) \
            .replace(" ", "")
        a_id = self.addresses[a_key]

        b_key = "".join(str(i) for i in [business[1]]) \
            .replace(" ", "")
        b_id = self.businesses[b_key]

        return a_id, b_id

    @staticmethod
    def process_drug(elements, ndc_id):
        return Pill.replace_null((
            ndc_id,
            elements[38],
            elements[41],
            elements[31],
            elements[34],
            elements[29],
            elements[21],
            elements[23],
            elements[35],
            elements[37]
        ))

    @staticmethod
    def process_report(elements):
        return Pill.replace_null((
            elements[33],
            elements[28],
            elements[26],
            elements[20],
            elements[27],
            elements[40],
            elements[30],
            elements[39],
            elements[36],
            elements[25],
            elements[24],
            elements[32]
        ))

    @staticmethod
    def replace_null(obj):
        res = tuple()
        for v in obj:
            res += (v,) if v and v != "null" else (None,)
        return res

    @staticmethod
    def process_business(b_name, dea):
        return Pill.replace_null((
            b_name,
            dea
        ))

    @staticmethod
    def process_address(elements, positions):
        add1 = elements[positions[0]]
        add2 = elements[positions[1]]

        (street_name, street_num, additional) = Pill.handle_address(add1, add2)
        if not additional and not elements[positions[2]] or (additional == "null" and additional == "null"):
            additional = None
        elif additional and elements:
            additional = "\n".join([additional, elements[positions[2]]])
        else:
            additional = additional if additional != "null" and additional else elements[positions[2]]
        # buy_add
        return Pill.replace_null((
            elements[positions[3]],
            elements[positions[4]],
            street_name,
            street_num,
            elements[positions[5]],
            elements[positions[6]],
            additional,
            None,
            None,
            additional
        ))

    @staticmethod
    def handle_address(add1, add2) -> (str, str, str):
        if add1 == "null":
            return Pill.split_address(add2) + (None,)
        elif add2 == "null":
            return Pill.split_address(add1) + (None,)
        else:
            if add1 == "null" and add2 == "null":
                return None, None, None
            else:
                if "DBA" == add1[:2]:
                    # add2 it is
                    return Pill.split_address(add2) + (add1,)
                elif "DBA" == add2[:2]:
                    # add1 it is
                    return Pill.split_address(add1) + (add2,)
                else:
                    if any(i.isdigit() for i in add1):
                        # add1
                        return Pill.split_address(add1) + (add2,)
                    else:
                        # add2
                        return Pill.split_address(add2) + (add1,)

    @staticmethod
    def split_address(add) -> (str, str):
        parts = add.replace(" ST", " STREET").replace(" RD.", " Road").replace(" RD", " Road").split(" ")
        num = ""
        street = ""

        for part in parts:
            if all([i.isdigit() for i in part]) or len(part) == 1 or len(part.replace(".", "")) == 1:
                num += part + " "
            else:
                street += part + " "

            street = street.strip()
            num = num.strip()
        return street if street != "" else None, num if num != "" else None


def main():
    p = Pill()


main()
