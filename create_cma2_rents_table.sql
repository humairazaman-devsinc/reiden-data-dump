-- CMA2 Rents Table Schema
-- Handles CMA2-rents API data with location hierarchy and property details

CREATE TABLE IF NOT EXISTS cma2_rents (
    id SERIAL PRIMARY KEY,
    
    -- API Parameters
    property_id INTEGER,  -- Property ID (optional parameter)
    country_code VARCHAR(5) NOT NULL,
    alias VARCHAR(20) NOT NULL,  -- last-five, last-ten, last-fifteen
    currency VARCHAR(5) NOT NULL,  -- usd, aed, eur, try
    measurement VARCHAR(10) NOT NULL,  -- int, imp
    property_type VARCHAR(50) NOT NULL,  -- Office
    property_subtype VARCHAR(100) NOT NULL,  -- Apartment, Villa, Office, etc.
    
    -- Location Hierarchy (from API response)
    city_id INTEGER,
    city_name VARCHAR(255),
    county_id INTEGER,
    county_name VARCHAR(255),
    district_id INTEGER,
    location_id INTEGER,
    district_name VARCHAR(255),
    location_name VARCHAR(255),
    municipal_area VARCHAR(255),
    
    -- Transaction Data (to be populated from actual API response)
    -- These fields will be added based on the actual API response structure
    transaction_count INTEGER,
    average_rent DECIMAL(15,2),
    median_rent DECIMAL(15,2),
    min_rent DECIMAL(15,2),
    max_rent DECIMAL(15,2),
    rent_per_sqm DECIMAL(15,2),
    
    -- Raw API response
    raw_data JSONB,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    -- Using a partial unique index to handle NULL values properly
    CONSTRAINT unique_cma2_rents_record UNIQUE(property_id, country_code, alias, currency, measurement, property_type, property_subtype, 
           city_id, county_id, district_id, location_id)
);

-- Create indexes for better query performance
-- Single column indexes for frequently queried fields
CREATE INDEX IF NOT EXISTS idx_cma2_rents_property_id ON cma2_rents(property_id);
CREATE INDEX IF NOT EXISTS idx_cma2_rents_created_at ON cma2_rents(created_at);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_cma2_rents_api_params ON cma2_rents(alias, currency, measurement, property_type, property_subtype);
CREATE INDEX IF NOT EXISTS idx_cma2_rents_location_hierarchy ON cma2_rents(country_code, city_id, county_id, district_id, location_id);
CREATE INDEX IF NOT EXISTS idx_cma2_rents_property_location ON cma2_rents(property_id, city_id, county_id, district_id);

-- Partial indexes for common filtering patterns
CREATE INDEX IF NOT EXISTS idx_cma2_rents_office_medical ON cma2_rents(property_id, created_at) 
    WHERE property_type = 'Office' AND property_subtype IN ('Medical Office', 'Office');
