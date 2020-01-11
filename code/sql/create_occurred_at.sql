CREATE TABLE IF NOT EXISTS occurred_at
(
    address_id INTEGER ,
    crime_id BIGINT(15),
    PRIMARY KEY (address_id, crime_id),
    FOREIGN KEY (address_id) REFERENCES Address(id),
    FOREIGN KEY (crime_id) REFERENCES Crime(id)
);