CREATE TABLE IF NOT EXISTS  Drug(
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
  PRIMARY KEY (ndc_no)
);