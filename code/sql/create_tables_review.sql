CREATE TABLE Review(
  review_id VARCHAR(24),
  user_id VARCHAR(24),
  business_id VARCHAR(24),
  date VARCHAR(255),
  text TEXT,
  cool INTEGER,
  funny INTEGER,
  useful INTEGER,
  stars INTEGER,
  PRIMARY KEY (review_id)
);