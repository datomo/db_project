CREATE TABLE IF NOT EXISTS Add_Business_Info(
    reviewed_business_id VARCHAR(24),
    is_open VARCHAR(255),
    review_count INTEGER,
    avg_stars DECIMAL,
    categories TEXT,
    hours VARCHAR(255),
    attributes TEXT,
    PRIMARY KEY (reviewed_business_id),
    FOREIGN KEY (reviewed_business_id) REFERENCES is_located(reviewed_business_id)
);