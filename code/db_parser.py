import re


def transform_sql(file_path) -> [str]:
    file = open(file_path, 'r')
    sql = file.read()
    file.close()

    return re.sub(' +', ' ', sql.replace("\n", "")).split(";")


def transform_tsv():
    pass

def transform_json():
    pass


def transform_csv():
    pass


def test(self):
    pass
