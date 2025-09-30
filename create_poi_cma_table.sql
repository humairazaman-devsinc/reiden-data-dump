-- Create POI CMA table for storing POI CMA data from Reidin API
-- This table stores data from the /poi/poi_cma endpoint
-- Based on actual API response structure

CREATE TABLE IF NOT EXISTS poi_cma (
    id SERIAL PRIMARY KEY,
    property_id INTEGER NOT NULL,
    measurement VARCHAR(10) NOT NULL,
    lang VARCHAR(5) NOT NULL,
    lat VARCHAR(50),
    lon VARCHAR(50),
    
    -- POI data fields from actual API response
    poi_subtype_name VARCHAR(255),
    poi_type_name VARCHAR(255),
    poi_name VARCHAR(500),
    poi_review INTEGER,
    poi_rating DECIMAL(3,2),
    poi_distance DECIMAL(15,10),
    
    
    -- Raw API response
    raw_data JSONB,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE(property_id, measurement, lang, poi_name, poi_type_name, poi_subtype_name, lat, lon)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_poi_cma_property_id ON poi_cma(property_id);
CREATE INDEX IF NOT EXISTS idx_poi_cma_measurement ON poi_cma(measurement);
CREATE INDEX IF NOT EXISTS idx_poi_cma_lang ON poi_cma(lang);
CREATE INDEX IF NOT EXISTS idx_poi_cma_poi_type_name ON poi_cma(poi_type_name);
CREATE INDEX IF NOT EXISTS idx_poi_cma_poi_subtype_name ON poi_cma(poi_subtype_name);
CREATE INDEX IF NOT EXISTS idx_poi_cma_poi_distance ON poi_cma(poi_distance);
CREATE INDEX IF NOT EXISTS idx_poi_cma_created_at ON poi_cma(created_at);

