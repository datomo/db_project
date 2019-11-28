import re

class Parser:

    @staticmethod
    def transform_sql(file_path) -> [str]:
        file = open(file_path, 'r')
        sql = file.read()
        file.close()

        return re.sub(' +', ' ', sql.replace("\n", "")).split(";")

    @staticmethod
    def transform_tsv():
        pass

    @staticmethod
    def transform_json():
        pass

    @staticmethod
    def transform_csv():
        pass