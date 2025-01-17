CREATE TABLE IF NOT EXISTS reports(
    transaction_id BIGINT(15),
    business_id BIGINT(15),
    address_id INTEGER,
    bus_act VARCHAR(255),
    role VARCHAR(255),
    PRIMARY KEY (transaction_id, business_id, address_id, role),
    FOREIGN KEY (address_id) REFERENCES Address(id),
    FOREIGN KEY (business_id) REFERENCES Business(id),
    FOREIGN KEY (transaction_id) REFERENCES  Report(id)
);