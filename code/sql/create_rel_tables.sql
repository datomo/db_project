CREATE TABLE IF NOT EXISTS is_located(
  address_id BIGINT(15) REFERENCES Address(address_id),
  business_id BIGINT(15) REFERENCES Business(business_id),
  PRIMARY KEY (address_id, business_id)
);

CREATE TABLE IF NOT EXISTS rates_a(
  review_id INTEGER REFERENCES Review(review_id),
  business_id BIGINT(15) REFERENCES Business(business_id),
  PRIMARY KEY (review_id, business_id)
);

CREATE TABLE IF NOT EXISTS specifies(
    transaction_id BIGINT(15) REFERENCES Report(id),
    ndc_id VARCHAR(15) REFERENCES Drug(ndc_no),
    PRIMARY KEY (transaction_id, ndc_id)
);

CREATE TABLE IF NOT EXISTS reports(
    business_id BIGINT(15) REFERENCES Business(business_id),
    transaction_id BIGINT(15) REFERENCES  Report(id),
    bus_act VARCHAR(255),
    role VARCHAR(255),
    PRIMARY KEY (business_id, transaction_id, role)
);