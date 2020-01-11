CREATE TABLE IF NOT EXISTS Report(
    id BIGINT(15) AUTO_INCREMENT,
    transaction_id BIGINT(15),
    correction_no INTEGER,
    action_indicator VARCHAR(2),
    transaction_code VARCHAR(2),
    order_from_no VARCHAR(20),
    reporter_family VARCHAR(255),
    transaction_date VARCHAR(255),
    revised_company_name VARCHAR(255),
    measure VARCHAR(255),
    unit VARCHAR(20),
    quantity INTEGER,
    dosage_unit INTEGER,
    PRIMARY KEY (id)
);