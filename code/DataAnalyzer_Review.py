import json

from Helper import Helper
from database import Database


class DataAnalyzerReview:
    def __init__(self):
        self.db = Database()

        self.get_crime_data()

    def get_crime_data(self):
        """ mit Zip, Anzahl reviews und avg stars"""
        review_avg_count = self.db.select("SELECT address.zip, count(*), CAST( AVG(avg_stars) AS FLOAT) "
                                          "FROM address INNER JOIN "
                                          "(is_located INNER JOIN add_business_info "
                                          "ON is_located.reviewed_business_id = add_business_info.reviewed_business_id) "
                                          "ON address.id = is_located.address_id "
                                          "WHERE address.state = 'AZ' && address.city = 'Phoenix' && zip LIKE '850%'"
                                          "group by zip "
                                          "order by zip;")

        """mit zip und avg stars"""
        review_avg = self.db.select("SELECT address.zip, CAST( AVG(avg_stars) AS FLOAT) "
                                    "FROM address INNER JOIN "
                                    "(is_located INNER JOIN add_business_info "
                                    "ON is_located.reviewed_business_id = add_business_info.reviewed_business_id) "
                                    "ON address.id = is_located.address_id "
                                    "WHERE address.state = 'AZ' && address.city = 'Phoenix' && zip LIKE '850%'"
                                    "group by zip "
                                    "order by zip;")

        review_avg_dict = dict(filter(lambda e: e[0] != "", review_avg))

        review_avg_count_dict = {k: v for k, c, v in filter(lambda e: e[1] >= 15, review_avg_count)}

        review_avg_dict = Helper.normalize_data(review_avg_dict)

        review_avg_count_dict = Helper.normalize_data(review_avg_count_dict)

        with open("results/review.json", "w+") as file:
            file.write(json.dumps(review_avg_dict))

        with open("results/review_filtered.json", "w+") as file:
            file.write(json.dumps(review_avg_count_dict))


def main():
    d = DataAnalyzerReview()


main()
