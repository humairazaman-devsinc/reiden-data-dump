-- Create tables for Reidin API data

-- Table for level-heirarchy
CREATE TABLE IF NOT EXISTS location_hierarchy (
    level_id INTEGER PRIMARY KEY,
    level_name VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO location_hierarchy (level_id, level_name) VALUES
(1, 'Country'),
(2, 'City'),
(3, 'County'),
(4, 'District')
ON CONFLICT (level_id) DO NOTHING;


-- Table for locations
CREATE TABLE IF NOT EXISTS locations (
    location_id INTEGER PRIMARY KEY,
    location_name VARCHAR(255),
    location_level INTEGER REFERENCES location_hierarchy(level_id),
    location_parent_id INTEGER REFERENCES locations(location_id),
    country_code VARCHAR(2),
    city_id INTEGER REFERENCES locations(location_id),
    city_name VARCHAR(255),
    county_id INTEGER REFERENCES locations(location_id),
    county_name VARCHAR(255),
    district_id INTEGER REFERENCES locations(location_id),
    district_name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Table for indicator groups
CREATE TABLE IF NOT EXISTS indicator_groups (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    parent_id INTEGER REFERENCES indicator_groups(group_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO indicator_groups (id, name, parent_id) VALUES
(35, 'Population', 2),
(2, 'Population', NULL)
ON CONFLICT (group_id) DO NOTHING;


-- Table for indicator data
CREATE TABLE IF NOT EXISTS indicator_data (
    indicator_id INTEGER,
    location_id INTEGER,
    indicator_name VARCHAR(255),
    indicator_group_id INTEGER[],
    unit_name VARCHAR(100),
    area_wide_status BOOLEAN,
    city_wide_status BOOLEAN,
    country_wide_status BOOLEAN,
    data_frequency VARCHAR(50),
    update_frequency VARCHAR(50),
    import_date TIMESTAMP,
    import_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);

-- Table for time series data
CREATE TABLE IF NOT EXISTS time_series_data (
    id INTEGER PRIMARY KEY,
    indicator_data_id INTEGER REFERENCES indicator_data(id),
    date DATE,
    value_aed_imp DECIMAL(15, 4),
    value_aed_int DECIMAL(15, 4),
    value_eur_imp DECIMAL(15, 4),
    value_eur_int DECIMAL(15, 4),
    value_try_imp DECIMAL(15, 4),
    value_try_int DECIMAL(15, 4),
    value_usd_imp DECIMAL(15, 4),
    value_usd_int DECIMAL(15, 4),
    value_text TEXT,
    differance_aed DECIMAL(15, 4),
    differance_eur DECIMAL(15, 4),
    differance_try DECIMAL(15, 4),
    differance_usd DECIMAL(15, 4),
    date_year INTEGER,
    date_month INTEGER,
    date_day INTEGER,
    date_quarter INTEGER,
    date_semi_annually INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_locations_location_level ON locations(location_level);
CREATE INDEX IF NOT EXISTS idx_locations_parent_id ON locations(location_parent_id);
CREATE INDEX IF NOT EXISTS idx_indicator_data_location_id ON indicator_data(location_id);
CREATE INDEX IF NOT EXISTS idx_indicator_data_indicator_id ON indicator_data(indicator_id);
CREATE INDEX IF NOT EXISTS idx_time_series_indicator_data_id ON time_series_data(indicator_data_id);
CREATE INDEX IF NOT EXISTS idx_time_series_date ON time_series_data(date);
