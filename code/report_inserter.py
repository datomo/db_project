import logging
import time

from Helper import Helper
from database import Database

logging.basicConfig(level=logging.DEBUG)


class ReportInserter:
    r_query = "INSERT INTO Report (transaction_id, correction_no, action_indicator,transaction_code,order_from_no,reporter_family,transaction_date,revised_company_name,measure,unit,quantity,dosage_unit) VALUES (" \
              "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    s_query = "INSERT INTO specifies VALUES (%s, %s)"

    target_folder = "./output/relations/{}"
    iterations = 0
    parse_txt = True

    def __init__(self):
        self.db = Database()

        self.db.drop_table("reports")
        self.db.drop_table("specifies")
        self.db.drop_table("report")

        Helper.create_tables("./sql/create_report.sql", self.db)
        Helper.create_tables("./sql/create_specifies.sql", self.db)
        Helper.create_tables("./sql/create_reports.sql", self.db)

        # reparse txt files
        if self.parse_txt:
            self.addresses = self.db.select("SELECT street, street_number, zip, state, id FROM Address;")
            self.addresses = Helper.parse_tuplelist_to_dict(self.addresses)

            self.businesses = self.db.select("SELECT DEA_No, id FROM Business;")
            self.businesses = Helper.parse_tuplelist_to_dict(self.businesses)

            Helper.clear_folder("./output/relations")

            self.time = time.time()
            self.start = time.time()
            Helper.chunk_file_pkl(2000000, "./output/output.pkl", self.parse_cols)
            logging.debug("finished parsing all csv after {}s".format(round(time.time() - self.start), 2))

        self.start = time.time()
        self.db.load_infile(self.target_folder.format("report.txt"), "Report")
        logging.debug("finished loading report after {}s".format(round(time.time() - self.start), 2))

        self.db.load_infile(self.target_folder.format("reports.txt"), "reports")
        logging.debug("finished loading reports csv after {}s".format(round(time.time() - self.start), 2))

        #self.db.load_infile(self.target_folder.format("specifies.txt"), "specifies")
        logging.debug("finished loading all csv after {}s".format(round(time.time() - self.start), 2))

    def parse_cols(self, cols):
        reports = []
        reports_rel = []
        specifies = []
        for col in cols:
            id = (col['id'],)
            report = col["r"]

            reports.append(id + report)
            # street = 2, street_number = 3, zip = 0, state = 5
            a_id, b_id = self.get_ids(col)
            reports_rel.append(id + (b_id,) + (a_id,) + (col["r_1"],) + ("REPORTER",))

            a_id, b_id = self.get_ids(col)
            reports_rel.append(id + (b_id,) + (a_id,) + (col["r_2"],) + ("BUYER",))

            specifies.append(id + (col["drug"][0],))

        logging.debug("Finished sorting chunk {} in {}s".format(self.iterations, round(time.time() - self.time, 2)))

        Helper.append_to_csv(reports, self.target_folder.format("report.txt"))
        Helper.append_to_csv(reports_rel, self.target_folder.format("reports.txt"))
        # Helper.append_to_csv_special(specifies, self.target_folder.format("specifies.txt"))
        self.db.querymany(self.s_query, specifies)

        logging.debug("Finished chunk {} in {}s".format(self.iterations, round(time.time() - self.time, 2)))
        self.time = time.time()
        self.iterations += 1

    def get_ids(self, col):
        a_key = "".join(str(i) for i in [col['a_1'][2], col['a_1'][3], col['a_1'][0], col['a_1'][5]]) \
            .replace(" ", "")
        a_id = self.addresses[a_key]

        b_key = "".join(str(i) for i in [col['b_1'][2]]) \
            .replace(" ", "")
        b_id = self.businesses[b_key]

        return a_id, b_id


def main():
    i = ReportInserter()


main()
