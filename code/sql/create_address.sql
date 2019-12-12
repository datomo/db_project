CREATE TABLE IF NOT EXISTS Address (
    id INTEGER auto_increment,
    zip INTEGER,
    city VARCHAR(255),
    street VARCHAR(255),
    street_number VARCHAR(255),
    county VARCHAR(255),
    state VARCHAR(255),
    address_name VARCHAR(255),
    longitude INTEGER,
    latitude INTEGER,
    addl_co_info VARCHAR(255),
    PRIMARY KEY (id)
);