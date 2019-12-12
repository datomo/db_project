CREATE TABLE IF NOT EXISTS Business(
    id BIGINT(15) AUTO_INCREMENT,
    business_name VARCHAR(255),
    reviewed_business_id VARCHAR(255),
    DEA_No VARCHAR(255),
    PRIMARY KEY (id)
);