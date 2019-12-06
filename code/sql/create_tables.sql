CREATE TABLE Address(
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
    PRIMARY KEY (zip, street, street_number)
);

CREATE TABLE Drug(
  ndc_no VARCHAR(20),
  combined_labeler_name VARCHAR(255),
  dos_str FLOAT(9,2),
  calc_base_wt_in_gm FLOAT,
  product_name VARCHAR(255), 
  strength INTEGER,
  drug_code INTEGER, 
  drug_name VARCHAR(255),
  ingredient_name VARCHAR(255), 
  mme_conversion_factor INTEGER,
  PRIMARY KEY (ndc_no),
  INDEX(ndc_no)
);

CREATE TABLE Crime(
    crime_id BIGINT(15),
    inc_number DECIMAL(20,0),
    premise_type VARCHAR(255), 
    occurred_on VARCHAR(255),
    occurred_to VARCHAR(255), 
    ucr_crime_category VARCHAR(255),
    PRIMARY KEY (crime_id)
);

CREATE TABLE Review(
  review_id INTEGER,
  user_id INTEGER, 
  date VARCHAR(255), 
  text TEXT,
  cool INTEGER, 
  funny INTEGER, 
  useful INTEGER,
  stars INTEGER,
  PRIMARY KEY (review_id)
);

CREATE TABLE Report(
    id BIGINT(15) AUTO_INCREMENT,
    transaction_id BIGINT(15),
    correction_no INTEGER,
    action_indicator VARCHAR(2),
    transaction_code VARCHAR(2),
    order_from_no VARCHAR(20),
    reporter_family VARCHAR(255),
    transaction_date VARCHAR(255), 
    revised_company_name VARCHAR(255),
    measure VARCHAR(255), 
    unit INTEGER,
    quantity INTEGER,
    dosage_unit INTEGER,
    PRIMARY KEY (id)
);

CREATE TABLE Business(
    business_name VARCHAR(255),
    reviewed_business_id VARCHAR(255),
    DEA_No VARCHAR(255),
    PRIMARY KEY (DEA_No, business_name)
);

CREATE TABLE Add_Business_Info(
    abi_id INTEGER, 
    is_open INTEGER, 
    review_count INTEGER,
    avg_stars Integer, 
    categories VARCHAR(255),
    hours INTEGER, 
    attributes VARCHAR(255),
    PRIMARY KEY (abi_id)
);
