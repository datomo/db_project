CREATE TABLE IF NOT EXISTS specifies(
    transaction_id BIGINT(15),
    ndc_id VARCHAR(15),
    PRIMARY KEY (transaction_id, ndc_id),
    FOREIGN KEY (transaction_id) REFERENCES Report(id),
    FOREIGN KEY (ndc_id) REFERENCES Drug(ndc_no)
);