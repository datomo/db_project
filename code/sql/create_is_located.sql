CREATE TABLE IF NOT EXISTS is_located(
    id BIGINT(15) AUTO_INCREMENT,
    address_id INTEGER,
    business_id BIGINT(15),
    PRIMARY KEY (id),
    FOREIGN KEY (address_id) REFERENCES Address(id),
    FOREIGN KEY (business_id)  REFERENCES Business(id)
);