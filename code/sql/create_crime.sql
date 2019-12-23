CREATE TABLE IF NOT EXISTS Crime(
    id BIGINT(15),
    inc_number VARCHAR(20),
    premise_type VARCHAR(255),
    occurred_on VARCHAR(255),
    occurred_to VARCHAR(255),
    ucr_crime_category VARCHAR(255),
    PRIMARY KEY (id)
);