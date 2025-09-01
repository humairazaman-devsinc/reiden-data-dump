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


-- Table for company details from company/{company_id} endpoint
CREATE TABLE IF NOT EXISTS company (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- -- Table for party details from property/location/{location_id} endpoint
-- CREATE TABLE IF NOT EXISTS party (
--     id BIGINT PRIMARY KEY,
--     nature_id BIGINT,
--     nature_name VARCHAR(255),
--     nature_name_local VARCHAR(255),
--     nature_parent_id BIGINT,
--     company_id BIGINT REFERENCES company(id),
--     company_name VARCHAR(255),
--     company_name_local VARCHAR(255),
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
    internal_status_id BIGINT,
    main_type_name VARCHAR(255),
    main_subtype_name VARCHAR(255),
    parent_id BIGINT,
    parent_ids INTEGER[],
    parents JSONB,

    location_id BIGINT,
    location JSONB,
    locations JSONB,

    status JSONB,
    level JSONB,

    description TEXT,
    description_local TEXT,
    types JSONB,
    parties JSONB,
    primary_image TEXT,
    images JSONB,
    units JSONB,
    developer_prices JSONB,
    search_terms VARCHAR(255)[],
    loc_point JSONB,
    geo_point JSONB,
    loc_polygon JSONB,
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    import_date TIMESTAMP,
    import_type VARCHAR(50),
    -- Double check following fields:
    dld_status VARCHAR(255),
    attributes JSONB,
    elapsed_time_status INTEGER,
    gla INTEGER,
    office_gla INTEGER,
    typical_gla_floor INTEGER,
    built_up_area DECIMAL(15, 2),
    building_height INTEGER,
    land_area DECIMAL(15, 2),
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
    units JSONB,
    attributes JSONB,
    level JSONB,
    geo_point JSONB,
    loc_point JSONB,
    location_id BIGINT,
    locations JSONB,
    parent_id BIGINT,
    parent_ids INTEGER[],
    parents JSONB,
    primary_image TEXT,
    search_terms VARCHAR(255)[],
    status JSONB,
    types JSONB,
    elapsed_time_status INTEGER,
    dld_status VARCHAR(255),
    updated_on TIMESTAMP,
    gla INTEGER,
    office_gla INTEGER,
    typical_gla_floor INTEGER,
    built_up_area DECIMAL(15, 2),
    building_height INTEGER,
    land_area DECIMAL(15, 2),
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- -- Table for property list from property/property_list endpoint
-- CREATE TABLE IF NOT EXISTS property_list (
--     id BIGINT PRIMARY KEY,
--     internal_status_id BIGINT,
--     location_id BIGINT,
--     locations JSONB,
--     main_subtype_name VARCHAR(255),
--     main_type_name VARCHAR(255),
--     parent_info INTEGER[],
--     property_id BIGINT,
--     property_name VARCHAR(255),
--     property_nature JSONB
-- );


-- Table for indicator groups from indicators/groups endpoint
CREATE TABLE IF NOT EXISTS indicator_group (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    name_local VARCHAR(255),
    parent_id BIGINT,
    indicator_id BIGINT NOT NULL, -- FK to indicator tables
    indicator_table_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for property-level indicators (indicators/aliased)
CREATE TABLE IF NOT EXISTS indicator_aliased (
    series_id BIGINT PRIMARY KEY,
    series_name VARCHAR(255),
    series_name_local VARCHAR(255),
    location_id BIGINT,
    location JSONB,
    currency JSONB,
    data_frequency JSONB,
    update_frequency JSONB,
    unit JSONB,

    property_id BIGINT,
    property JSONB,

    indicator_id BIGINT,
    indicator JSONB,
    indicator_name TEXT,
    last_value JSONB,
    timepoints JSONB,
    property_is_null BOOLEAN,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for area-level indicators (indicators/area-aliased)
CREATE TABLE IF NOT EXISTS indicator_area_aliased (
    -- Filter Details
    series_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    series_name VARCHAR(255),
    series_name_local VARCHAR(255),
    alias VARCHAR(255),
    property_type VARCHAR(255),
    price_type VARCHAR(255),
    no_of_bedrooms INT,

    -- Filtered Data Details
    indicator JSONB,
    data_frequency JSONB,
    last_value JSONB,
    indicator_property_type VARCHAR(255),
    indicator_property_subtype VARCHAR(255),
    property_is_null BOOLEAN,
    property JSONB,
    unit JSONB,
    location JSONB,
    currency JSONB,
    timepoints JSONB,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);