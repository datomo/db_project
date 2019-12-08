

if __name__=="__main__":
    global file_path

    Pill_relations.parse_db(db)
    queries = db_parser.transform_sql("./sql/create_rel_tables.sql")
    db.query_all(queries)

    with open(file_path, 'r') as file:
        chunk = 2000000
        lines = sum(1 for i in open(file_path, 'rb'))

        print("number of columns: {}".format(lines))

        chunk_amount = math.ceil(float(lines) / chunk)
        print("{}".format(chunk_amount))

        i = 0

        start_time = time.time()

        for a_chunk in range(chunk_amount):

            output = []
            j = 0
            for line in file:

                if i == 0:
                    i += 1
                    continue
                output.append(line)
                i += 1
                j += 1

                if j >= chunk:
                    break

            Pill_relations.process_cols(output, i - j, db)
            print("Chunk finished {}".format(a_chunk))
