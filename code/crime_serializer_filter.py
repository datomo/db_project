import logging

from Helper import Helper

logging.basicConfig(level=logging.DEBUG)

class Crime:
    file_path = "../data/crime-data_crime-data_crimestat.csv"

    def __init__(self):

        output = []
        with open(self.file_path, "r") as file:

            i = 0
            for line in file:
                if i == 0:
                    i += 1
                    continue
                output.append(line)
                i += 1
        self.parse_cols(output)

    def parse_cols(self, lines):
        i = 1
        output = []
        for line in lines:
            if i == 0:
                i += 1
                continue
            data = [el.replace("\"", "").replace("\n", "") for el in line.split(",")]
            data = [None if x == "" else x for x in data]
            words = data[4].split(" ")
            words = [' '.join(words[:2]), ' '.join(words[-2:])]
            zip = data[5] if data[5] else "000000"

            output.append({"id": i, "address": self.get_address_data(words, zip), "crime": self.get_crime_data(data)})
            i += 1
        Helper.write_list(output, "output/crime", "crime")

    def get_address_data(self, words, zip):
        return (
            zip,
            words[1],
            words[0],
            'PHEONIX',
            'MARICOPA',
            'AZ',
            None,
            None,
            None,
            None,
        )

    def get_crime_data(self, data):
        return (
            data[0],
            data[6],
            data[1],
            data[2],
            data[3]
        )


def main():
    c = Crime()


main()
