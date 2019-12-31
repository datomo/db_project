import json

from Helper import Helper
from database import Database


class Yelp_Review:
    file_path = "../data/yelp_academic_dataset_review.json"

    r_query = "INSERT INTO Review(review_id, user_id, business_id, date, text, cool, funny, useful, stars) VALUES(" \
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
        self.db.drop_table("review")
        Helper.create_tables("./sql/create_review.sql", self.db)


        i = 0
        self.j = 0
        chunk = 200000
        output = []
        with open(self.file_path, encoding="utf8", mode="r") as file:
            for line in file:
                obj = json.loads(line)
                output.append(obj)
                i += 1
                if i > chunk:
                    self.parse_cols(output)
                    output = []
                    i = 0
            self.parse_cols(output)
        self.db.load_infile_utf8("./output/yelp/review.txt", "Review")

    def parse_cols(self, output):
        print('starting chunk {}'.format(self.j))
        y_data = []
        for col in output:
            y_data.append((
                col['review_id'],
                col['user_id'],
                col['business_id'],
                col['date'],
                col['text'],
                col['cool'],
                col['funny'],
                col['useful'],
                col['stars']
            ))

        Helper.append_to_csv(y_data, "./output/yelp/review.txt", "utf8")

        # self.db.querymany(self.r_query, y_data)
        self.j += 1


def main():
    y = Yelp_Review()


main()
