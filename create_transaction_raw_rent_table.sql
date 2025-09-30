CREATE TABLE transaction_raw_rent (
    id SERIAL PRIMARY KEY,
    
    date TIMESTAMP WITH TIME ZONE,
    size DECIMAL(20,2),
    price DECIMAL(20,2),
    end_date TIMESTAMP WITH TIME ZONE,
    start_date TIMESTAMP WITH TIME ZONE,
    size_land_int DECIMAL(20,2),
    price_per_size DECIMAL(20,2),
    transaction_type VARCHAR(1000),
    transaction_version VARCHAR(1000),
    
    loc_city_id BIGINT,
    loc_city_name VARCHAR(1000),
    loc_county_id BIGINT,
    loc_county_name VARCHAR(1000),
    loc_district_id BIGINT,
    loc_location_id BIGINT,
    loc_district_name VARCHAR(1000),
    loc_location_name VARCHAR(1000),
    loc_municipal_area VARCHAR(1000),
    
    prop_property_id BIGINT,
    prop_property_name VARCHAR(1000),
    
    attr_unit VARCHAR(1000),
    attr_floor VARCHAR(1000),
    attr_parking VARCHAR(1000),
    attr_land_number VARCHAR(1000),
    attr_no_of_rooms VARCHAR(1000),
    attr_balcony_area VARCHAR(1000),
    attr_building_name VARCHAR(1000),
    attr_building_number VARCHAR(1000),
    
    prop_type_name VARCHAR(1000),
    prop_subtype_name VARCHAR(1000),
    
    currency VARCHAR(50) DEFAULT 'aed',
    measurement VARCHAR(50),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transaction_raw_rent_date ON transaction_raw_rent(date);
CREATE INDEX idx_transaction_raw_rent_location ON transaction_raw_rent(loc_location_id);
CREATE INDEX idx_transaction_raw_rent_currency_measurement ON transaction_raw_rent(currency, measurement);
CREATE INDEX idx_transaction_raw_rent_created_at ON transaction_raw_rent(created_at);

CREATE UNIQUE INDEX idx_transaction_raw_rent_unique 
ON transaction_raw_rent (loc_location_id, currency, measurement, date, price, size) 
WHERE date IS NOT NULL AND price IS NOT NULL;
