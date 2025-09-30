-- DROP TABLE IF EXISTS location CASCADE;
-- DROP TABLE IF EXISTS property CASCADE;
-- DROP TABLE IF EXISTS property_details CASCADE;
-- DROP TABLE IF EXISTS indicator_aliased CASCADE;

-- Table for location from locations endpoint
CREATE TABLE IF NOT EXISTS location (
    location_id BIGINT PRIMARY KEY,
    city_id BIGINT,
    city_name VARCHAR(255),
    country_code VARCHAR(10),
    county_id BIGINT,
    county_name VARCHAR(255),
    description TEXT,
    district_id BIGINT,
    district_name VARCHAR(255),
    location_name VARCHAR(255),
    geo_point JSONB,
    photo_path TEXT,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);




-- -- Table for party details from property/location/{location_id} endpoint
-- CREATE TABLE IF NOT EXISTS party (
--     id BIGINT PRIMARY KEY,
--     nature_id BIGINT,
--     nature_name VARCHAR(255),
--     nature_name_local VARCHAR(255),
--     nature_parent_id BIGINT,
--     property_id BIGINT REFERENCES property(id),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- -- Table for image details from property/location/{location_id} endpoint
-- CREATE TABLE IF NOT EXISTS image (
--     id BIGINT PRIMARY KEY,
--     unit_id BIGINT,
--     "order" INTEGER,
--     is_primary BOOLEAN,
--     type JSONB,
--     path TEXT,
--     property_id BIGINT REFERENCES property(id),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- -- Table for unit details from property/location/{location_id} endpoint
-- CREATE TABLE IF NOT EXISTS unit (
--     id BIGINT PRIMARY KEY,
--     parent_number_of_unit INTEGER,
--     number_of_bedroom INTEGER,
--     number_of_unit INTEGER,
--     size DECIMAL(10, 2),
--     size_min DECIMAL(10, 2),
--     size_max DECIMAL(10, 2),
--     property_id BIGINT REFERENCES property(id),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- -- Table for attribute details from property/location/{location_id} endpoint
-- CREATE TABLE IF NOT EXISTS attribute (
--     id BIGINT PRIMARY KEY,
--     name VARCHAR(255),
--     label_local VARCHAR(255),
--     label VARCHAR(255),
--     group_id BIGINT,
--     group_name VARCHAR(255),
--     type VARCHAR(50),
--     value_array JSONB,
--     value_object JSONB,
--     value_bool BOOLEAN,
--     value_text TEXT,
--     value_date DATE,
--     property_id BIGINT REFERENCES property(id),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Table for property details from property/location/{location_id} endpoint
CREATE TABLE IF NOT EXISTS property (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    name_local VARCHAR(255),
    description TEXT,
    description_local TEXT,
    internal_status_id BIGINT,
    loc_point JSONB,
    geo_point JSONB,
    loc_polygon JSONB,

    level JSONB,
    status JSONB,
    types JSONB,
    main_type_name VARCHAR(255),
    main_subtype_name VARCHAR(255),

    parent_id BIGINT,
    parent_ids BIGINT[],
    parents JSONB,

    location_id BIGINT,
    location JSONB,
    locations JSONB,

    attributes JSONB,
    units JSONB,
    developer_prices JSONB,
    images JSONB,
    primary_image TEXT,
    parties JSONB,

    search_terms VARCHAR(255)[],
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    import_date TIMESTAMP,
    import_type VARCHAR(50),

    dld_status VARCHAR(255),
    elapsed_time_status INTEGER,

    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Table for property_details from property/{property_id} endpoint
CREATE TABLE IF NOT EXISTS property_details (
    id BIGSERIAL PRIMARY KEY,
    property_id BIGINT REFERENCES property(id) UNIQUE,
    name VARCHAR(255),
    name_local VARCHAR(255),
    description TEXT,
    description_local TEXT,
    developer_prices JSONB,
    parties JSONB,
    images JSONB,
    units JSONB,                    -- Get from here
    attributes JSONB,
    level JSONB,
    geo_point JSONB,
    loc_point JSONB,
    location_id BIGINT,
    location JSONB,
    locations JSONB,
    parent_id BIGINT,
    parent_ids BIGINT[],
    parents JSONB,
    primary_image TEXT,
    search_terms VARCHAR(255)[],
    status JSONB,
    types JSONB,
    elapsed_time_status INTEGER,
    dld_status VARCHAR(255),
    updated_on TIMESTAMP,
    gla DECIMAL(15, 2),             -- Get from here
    office_gla DECIMAL(15, 2),      -- Get from here
    typical_gla_floor DECIMAL(15, 2), -- Get from here
    built_up_area DECIMAL(15, 2),   -- Get from here
    building_height DECIMAL(15, 2),        -- Get from here
    land_area DECIMAL(15, 2),       -- Get from here
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for property-level indicators (indicators/aliased)
CREATE TABLE IF NOT EXISTS indicator_aliased (
    series_id BIGINT PRIMARY KEY,
    series_name VARCHAR(255),
    series_name_local VARCHAR(255),
    currency JSONB,
    data_frequency JSONB,
    update_frequency JSONB,
    unit JSONB,

    location_id BIGINT,
    location JSONB,

    property_id BIGINT,
    property JSONB,

    indicator JSONB,
    indicator_groups JSONB,
    last_value JSONB,
    timepoints JSONB,
    property_is_null BOOLEAN,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Table for transactions average data from {country_code}/transactions/avg/ endpoint
-- This properly normalizes the time-series data from the API
CREATE TABLE IF NOT EXISTS transactions_avg (
    id BIGSERIAL PRIMARY KEY,
    
    -- Location and property context
    location_id BIGINT NOT NULL,
    location_name VARCHAR(255),
    property_type VARCHAR(255) NOT NULL,
    activity_type VARCHAR(255) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    measurement VARCHAR(10) NOT NULL,
    no_of_bedrooms INTEGER,
    
    -- Time period
    date_period VARCHAR(10) NOT NULL, -- e.g., "2021-10", "2022-03"
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    
    -- Transaction metrics
    average_net_price DECIMAL(15, 2),
    average_unit_price DECIMAL(15, 2),
    total_count INTEGER,
    total_price DECIMAL(15, 2),
    price_per_sqm DECIMAL(15, 2),
    transaction_volume DECIMAL(15, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Raw data for reference (optional)
    raw_data JSONB,
    
    -- Unique constraint for upsert operations
    UNIQUE (location_id, property_type, activity_type, currency, measurement, no_of_bedrooms, date_period)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_transactions_avg_location_id ON transactions_avg (location_id);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_property_type ON transactions_avg (property_type);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_activity_type ON transactions_avg (activity_type);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_currency ON transactions_avg (currency);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_date_period ON transactions_avg (date_period);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_year_month ON transactions_avg (year, month);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_no_of_bedrooms ON transactions_avg (no_of_bedrooms);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_transactions_avg_location_property ON transactions_avg (location_id, property_type, activity_type);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_location_date ON transactions_avg (location_id, date_period);
CREATE INDEX IF NOT EXISTS idx_transactions_avg_property_date ON transactions_avg (property_type, activity_type, date_period);

-- Table for transaction history data from {country_code}/transactions/history/ endpoint
CREATE TABLE IF NOT EXISTS transaction_history (
    id BIGSERIAL PRIMARY KEY,
    
    -- Transaction details
    date_transaction TIMESTAMP NOT NULL,
    land_number VARCHAR(50) NOT NULL,
    municipal_area VARCHAR(255) NOT NULL,
    
    -- Location information
    loc_city_id BIGINT,
    loc_city_name VARCHAR(255),
    loc_county_id BIGINT,
    loc_county_name VARCHAR(255),
    loc_district_id BIGINT,
    loc_district_name VARCHAR(255),
    loc_location_id BIGINT,
    loc_location_name VARCHAR(255),
    loc_municipal_area VARCHAR(255),
    
    -- Property information
    property_id BIGINT,
    property_name VARCHAR(500),
    property_type_municipal_property_type VARCHAR(100),
    property_type_subtype_name VARCHAR(100),
    property_type_type_name VARCHAR(100),
    
    -- Financial information
    price_aed DECIMAL(15, 2),
    price_size_aed_imp DECIMAL(15, 2),
    size_imp DECIMAL(15, 2),
    size_land_imp DECIMAL(15, 2),
    
    -- Additional filters used
    transaction_type VARCHAR(100),
    building_number VARCHAR(50),
    building_name VARCHAR(500),
    unit VARCHAR(50),
    floor VARCHAR(50),
    unit_min_size DECIMAL(15, 2),
    unit_max_size DECIMAL(15, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Raw data for reference
    raw_data JSONB,
    
    -- Unique constraint for upsert operations
    UNIQUE (date_transaction, land_number, municipal_area, property_id, price_aed)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_transaction_history_date ON transaction_history (date_transaction);
CREATE INDEX IF NOT EXISTS idx_transaction_history_land_number ON transaction_history (land_number);
CREATE INDEX IF NOT EXISTS idx_transaction_history_municipal_area ON transaction_history (municipal_area);
CREATE INDEX IF NOT EXISTS idx_transaction_history_location_id ON transaction_history (loc_location_id);
CREATE INDEX IF NOT EXISTS idx_transaction_history_property_id ON transaction_history (property_id);
CREATE INDEX IF NOT EXISTS idx_transaction_history_property_type ON transaction_history (property_type_type_name);
CREATE INDEX IF NOT EXISTS idx_transaction_history_price ON transaction_history (price_aed);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_transaction_history_area_land ON transaction_history (municipal_area, land_number);
CREATE INDEX IF NOT EXISTS idx_transaction_history_date_area ON transaction_history (date_transaction, municipal_area);
CREATE INDEX IF NOT EXISTS idx_transaction_history_property_date ON transaction_history (property_type_type_name, date_transaction);

-- Table for transaction rent data from {country_code}/transactions/transaction_raw_rents/ endpoint
CREATE TABLE IF NOT EXISTS transaction_rent (
    id BIGSERIAL PRIMARY KEY,
    
    -- Transaction details
    start_date DATE,
    end_date DATE,
    date DATE,
    transaction_version VARCHAR(50),
    transaction_type VARCHAR(100),
    
    -- Location information
    location_id BIGINT NOT NULL,
    loc_city_id BIGINT,
    loc_city_name VARCHAR(255),
    loc_county_id BIGINT,
    loc_county_name VARCHAR(255),
    loc_district_id BIGINT,
    loc_district_name VARCHAR(255),
    loc_location_id BIGINT,
    loc_location_name VARCHAR(255),
    loc_municipal_area VARCHAR(255),
    
    -- Property information
    property_id BIGINT,
    property_name VARCHAR(500),
    property_type_name VARCHAR(100),
    property_subtype_name VARCHAR(100),
    
    -- Financial information
    price DECIMAL(15, 2),
    price_per_size DECIMAL(15, 2),
    size DECIMAL(15, 2),
    size_land_imp DECIMAL(15, 2),
    
    -- Property attributes
    attr_unit VARCHAR(50),
    attr_floor VARCHAR(50),
    attr_parking VARCHAR(100),
    attr_land_number VARCHAR(50),
    attr_no_of_rooms INTEGER,
    attr_balcony_area DECIMAL(15, 2),
    attr_building_name VARCHAR(500),
    attr_building_number VARCHAR(50),
    
    -- Additional filters used
    currency VARCHAR(10),
    measurement VARCHAR(10),
    bedroom INTEGER,
    size_range VARCHAR(50),
    price_range VARCHAR(50),
    rent_type VARCHAR(100),
    transaction_date VARCHAR(100),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Raw data for reference
    raw_data JSONB,
    
    -- Unique constraint for upsert operations
    UNIQUE (location_id, start_date, end_date, price, transaction_type, date)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_transaction_rent_location_id ON transaction_rent (location_id);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_property_id ON transaction_rent (property_id);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_start_date ON transaction_rent (start_date);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_end_date ON transaction_rent (end_date);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_date ON transaction_rent (date);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_transaction_type ON transaction_rent (transaction_type);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_property_type ON transaction_rent (property_type_name);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_price ON transaction_rent (price);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_transaction_rent_location_date ON transaction_rent (location_id, date);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_property_date ON transaction_rent (property_id, date);
CREATE INDEX IF NOT EXISTS idx_transaction_rent_type_date ON transaction_rent (transaction_type, date);

-- Transaction Sales table
CREATE TABLE IF NOT EXISTS transaction_sales (
    id BIGSERIAL PRIMARY KEY,
    
    -- Transaction details
    start_date DATE,
    end_date DATE,
    date DATE,
    transaction_version VARCHAR(50),
    transaction_type VARCHAR(100),
    
    -- Location information
    location_id BIGINT,
    loc_city_id BIGINT,
    loc_city_name VARCHAR(255),
    loc_county_id BIGINT,
    loc_county_name VARCHAR(255),
    loc_district_id BIGINT,
    loc_district_name VARCHAR(255),
    loc_location_id BIGINT,
    loc_location_name VARCHAR(255),
    loc_municipal_area VARCHAR(255),
    
    -- Property information
    property_id BIGINT,
    property_name VARCHAR(500),
    property_type_name VARCHAR(100),
    property_subtype_name VARCHAR(100),
    
    -- Financial information
    price DECIMAL(15, 2),
    price_per_size DECIMAL(15, 2),
    size DECIMAL(15, 2),
    size_land_imp DECIMAL(15, 2),
    
    -- Property attributes
    attr_unit VARCHAR(50),
    attr_floor VARCHAR(50),
    attr_parking VARCHAR(100),
    attr_land_number VARCHAR(50),
    attr_no_of_rooms INTEGER,
    attr_balcony_area DECIMAL(15, 2),
    attr_building_name VARCHAR(500),
    attr_building_number VARCHAR(50),
    
    -- Additional filters used
    currency VARCHAR(10),
    measurement VARCHAR(10),
    bedroom INTEGER,
    size_range VARCHAR(50),
    price_range VARCHAR(50),
    transaction_date VARCHAR(100),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Raw data for reference
    raw_data JSONB,
    
    -- Unique constraint for upsert operations
    UNIQUE (location_id, start_date, end_date, price, transaction_type, date)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_transaction_sales_location_id ON transaction_sales (location_id);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_property_id ON transaction_sales (property_id);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_start_date ON transaction_sales (start_date);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_end_date ON transaction_sales (end_date);

-- Table for CMA sales data from transactions/cma2-sales/ endpoint
CREATE TABLE IF NOT EXISTS cma_sales (
    id BIGSERIAL PRIMARY KEY,
    
    -- Property information
    property_id BIGINT NOT NULL,
    
    -- CMA parameters used in the query
    alias VARCHAR(50) NOT NULL,  -- last-five, last-ten, last-fifteen
    currency VARCHAR(10) NOT NULL,  -- aed, usd, eur, try
    measurement VARCHAR(10) NOT NULL,  -- imp, int
    property_type VARCHAR(100) NOT NULL,  -- Commercial, Land, Residential, Office
    property_subtype VARCHAR(200) NOT NULL,  -- Industrial-Warehouse, etc.
    
    -- Comparable property information (from the API response)
    comparable_property_id BIGINT NOT NULL,  -- The comparable property ID from response
    comparable_property_name VARCHAR(500),  -- Property name from response
    
    -- Transaction details from comparable property
    size DECIMAL(15, 2),  -- Size of the comparable property
    price DECIMAL(15, 2),  -- Price of the comparable property
    price_per_size DECIMAL(15, 2),  -- Price per size unit
    transaction_date TIMESTAMP,  -- When the transaction occurred
    
    -- Location information (flattened from nested location object)
    city_id INTEGER,
    city_name VARCHAR(255),
    county_id INTEGER,
    county_name VARCHAR(255),
    district_id INTEGER,
    district_name VARCHAR(255),
    location_id INTEGER,
    location_name VARCHAR(255),
    municipal_area VARCHAR(255),
    
    -- Property attributes
    activity_type VARCHAR(100),  -- Sales - Ready, Sales - Off-Plan
    no_of_bedrooms INTEGER,
    number_of_unit VARCHAR(50),  -- Unit number/identifier
    property_nature VARCHAR(100),  -- Building, Land, etc.
    number_of_floors VARCHAR(50),  -- Floor information
    
    -- Parent property information (if applicable)
    parent_property JSONB,  -- Parent property data as JSON
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Raw data for reference (complete API response)
    raw_data JSONB,
    
    -- Unique constraint for upsert operations
    UNIQUE (property_id, alias, currency, measurement, property_type, property_subtype, comparable_property_id)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_cma_sales_property_id ON cma_sales (property_id);
CREATE INDEX IF NOT EXISTS idx_cma_sales_alias ON cma_sales (alias);
CREATE INDEX IF NOT EXISTS idx_cma_sales_currency ON cma_sales (currency);
CREATE INDEX IF NOT EXISTS idx_cma_sales_measurement ON cma_sales (measurement);
CREATE INDEX IF NOT EXISTS idx_cma_sales_property_type ON cma_sales (property_type);
CREATE INDEX IF NOT EXISTS idx_cma_sales_property_subtype ON cma_sales (property_subtype);
CREATE INDEX IF NOT EXISTS idx_cma_sales_comparable_property_id ON cma_sales (comparable_property_id);
CREATE INDEX IF NOT EXISTS idx_cma_sales_transaction_date ON cma_sales (transaction_date);
CREATE INDEX IF NOT EXISTS idx_cma_sales_price ON cma_sales (price);
CREATE INDEX IF NOT EXISTS idx_cma_sales_city_id ON cma_sales (city_id);
CREATE INDEX IF NOT EXISTS idx_cma_sales_district_id ON cma_sales (district_id);

-- Table for transactions price data from transactions/price/ endpoint
CREATE TABLE IF NOT EXISTS transactions_price (
    id BIGSERIAL PRIMARY KEY,
    
    -- Location information
    location_id BIGINT NOT NULL,
    
    -- Transaction parameters
    property_type VARCHAR(100) NOT NULL,  -- residential, office
    activity_type VARCHAR(100) NOT NULL,  -- ready, off-plan
    property_id BIGINT,  -- Optional property ID
    property_sub_type VARCHAR(200),  -- apartment, villa, office
    no_of_bedrooms INTEGER,  -- Optional bedroom count
    
    -- Price data
    term VARCHAR(255),  -- The term/key from the results (e.g., month, year)
    value DECIMAL(15, 2),  -- The price value for the term
    additional_data JSONB,  -- Any additional data from the result item
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Raw data for reference
    raw_data JSONB,
    
    -- Unique constraint for upsert operations
    UNIQUE (location_id, property_type, activity_type, property_id, property_sub_type, no_of_bedrooms, term)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_transactions_price_location_id ON transactions_price (location_id);
CREATE INDEX IF NOT EXISTS idx_transactions_price_property_type ON transactions_price (property_type);
CREATE INDEX IF NOT EXISTS idx_transactions_price_activity_type ON transactions_price (activity_type);
CREATE INDEX IF NOT EXISTS idx_transactions_price_property_id ON transactions_price (property_id);
CREATE INDEX IF NOT EXISTS idx_transactions_price_property_sub_type ON transactions_price (property_sub_type);
CREATE INDEX IF NOT EXISTS idx_transactions_price_no_of_bedrooms ON transactions_price (no_of_bedrooms);
CREATE INDEX IF NOT EXISTS idx_transactions_price_term ON transactions_price (term);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_date ON transaction_sales (date);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_transaction_type ON transaction_sales (transaction_type);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_property_type ON transaction_sales (property_type_name);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_price ON transaction_sales (price);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_transaction_sales_location_date ON transaction_sales (location_id, date);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_property_date ON transaction_sales (property_id, date);
CREATE INDEX IF NOT EXISTS idx_transaction_sales_type_date ON transaction_sales (transaction_type, date);