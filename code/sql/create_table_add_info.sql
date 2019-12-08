CREATE TABLE IF NOT EXISTS Add_Business_Info(
    business_id BIGINT(15),
    is_open VARCHAR(255),
    review_count INTEGER,
    avg_stars DECIMAL,
    categories VARCHAR(255),
    hours VARCHAR(255),
    attributes VARCHAR(255),
    PRIMARY KEY (business_id),
    FOREIGN KEY (business_id) REFERENCES Business(id)
);