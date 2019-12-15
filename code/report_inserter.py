from Helper import Helper
from database import Database


class ReportInserter:
    r_query = "INSERT INTO Report (transaction_id, correction_no, action_indicator,transaction_code,order_from_no,reporter_family,transaction_date,revised_company_name,measure,unit,quantity,dosage_unit) VALUES (" \
              "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    def __init__(self):
        self.db = Database()
        Helper.create_tables("./sql/create_report.sql")
        Helper.create_tables("./sql/create_specifies.sql")
        Helper.create_tables("./sql/create_reports.sql")

        self.db.drop_table("report")
        self.db.drop_table("specifies")
        self.db.drop_table("reports")

        # TODO: get all addresses and business and drugs

        Helper.chunk_file_pkl(2000000, "./output/output.pkl", self.parse_cols)

    def parse_cols(self, cols):
        reports = []
        for col in cols:
            id = (col['id'],)
            report = col["r"]

            reports.append(id + report)

        self.db.querymany(self.r_query, Helper.tuplelist_to_listlist(reports))



