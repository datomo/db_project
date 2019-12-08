CREATE TABLE IF NOT EXISTS Crime(
    inc_number VARCHAR(20),
    premise_type VARCHAR(255),
    occurred_on VARCHAR(255),
    occurred_to VARCHAR(255),
    ucr_crime_category VARCHAR(255),
    PRIMARY KEY (inc_number, ucr_crime_category, premise_type)
);

CREATE TABLE IF NOT EXISTS occured_at(
    address_id BIGINT(15),
    crime_id BIGINT(15),
    PRIMARY KEY (address_id, crime_id),
    FOREIGN KEY (address_id) REFERENCES Address(id),
    FOREIGN KEY (crime_id) REFERENCES Crime(crime_id
);