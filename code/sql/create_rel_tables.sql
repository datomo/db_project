CREATE TABLE IF NOT EXISTS is_located(
  address_id BIGINT(15),
  business_id BIGINT(15),
  PRIMARY KEY (address_id, business_id),
  FOREIGN KEY (address_id) REFERENCES Address(address_id),
  FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

CREATE TABLE IF NOT EXISTS specifies(
    transaction_id BIGINT(15),
    ndc_id VARCHAR(15),
    PRIMARY KEY (transaction_id, ndc_id),
    FOREIGN KEY (transaction_id) REFERENCES Report(id),
    FOREIGN KEY (ndc_id) REFERENCES Drug(ndc_no)
);

CREATE TABLE IF NOT EXISTS reports(
    business_id BIGINT(15),
    transaction_id BIGINT(15),
    bus_act VARCHAR(255),
    role VARCHAR(255),
    PRIMARY KEY (business_id, transaction_id, role),
    FOREIGN KEY (business_id) REFERENCES Business(business_id),
    FOREIGN KEY (transaction_id) REFERENCES  Report(id)
);