CREATE TABLE Address(
    address_id INTEGER, 
    zip INTEGER,
    city VARCHAR(255), 
    street VARCHAR(255),
    street_number INTEGER, 
    county VARCHAR(255),
    state VARCHAR(255), 
    address_name VARCHAR(255),
    longitude INTEGER, 
    latitude INTEGER,
    addl_co_info VARCHAR(255),
    PRIMARY KEY (address_id)
);

CREATE TABLE Drug(
  ndc_no INTEGER, 
  combined_labeler_name VARCHAR(255),
  dos_str FLOAT, 
  calc_base_wt_in_gm FLOAT,
  product_name VARCHAR(255), 
  strength INTEGER,
  drug_code INTEGER, 
  drug_name VARCHAR(255),
  ingredient_name VARCHAR(255), 
  mme_conversion_factor INTEGER,
  PRIMARY KEY (ndc_no)
);

CREATE TABLE Crime(
    crime_id INTEGER, 
    inc_number INTEGER,
    premise_type VARCHAR(255), 
    occurred_on VARCHAR(255),
    occurred_to VARCHAR(255), 
    ucr_crime_category VARCHAR(255),
    PRIMARY KEY (crime_id)
);

CREATE TABLE Review(
  review_id INTEGER, 
  business_id INTEGER REFERENCES Business(business_id),
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
    transaction_id INTEGER, 
    correction_no INTEGER,
    action_indicator INTEGER, 
    transaction_code INTEGER,
    order_from_no INTEGER, 
    reporter_family VARCHAR(255),
    transaction_date VARCHAR(255), 
    revised_company_name VARCHAR(255),
    measure VARCHAR(255), 
    unit INTEGER, quantity INTEGER,
    dosage_unit INTEGER,
    PRIMARY KEY (transaction_id)
);

CREATE TABLE Business(
    business_id INTEGER, 
    business_name VARCHAR(255),
    PRIMARY KEY (business_id)
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

CREATE TABLE is_located(
  address_id INTEGER REFERENCES Address(address_id),
  business_name VARCHAR(255) REFERENCES Business(business_name),
  PRIMARY KEY (address_id)
);

CREATE TABLE rates_a(
  review_id INTEGER REFERENCES Review(review_id),
  business_id INTEGER REFERENCES Business(business_id),
  PRIMARY KEY (review_id, business_id)
);

CREATE TABLE occured_at(
    address_id INTEGER REFERENCES Address(address_id),
    business_id INTEGER REFERENCES Business(business_id),
    PRIMARY KEY (address_id, business_id)
);

CREATE TABLE  specifies(
    transaction_id INTEGER REFERENCES Report(transaction_id),
    ndc_id INTEGER REFERENCES Drug(ndc_no),
    PRIMARY KEY (transaction_id, ndc_id)
);

CREATE TABLE reports(
    business_id INTEGER REFERENCES Business(business_id),
    transaction_id INTEGER REFERENCES  Report(transaction_id),
    bus_act INTEGER, role VARCHAR(255),
    PRIMARY KEY (business_id, transaction_id)
);

CREATE TABLE has(
    business_id INTEGER REFERENCES Business (business_id),
    abi_id INTEGER REFERENCES  Add_Business_Info(abi_id)
);