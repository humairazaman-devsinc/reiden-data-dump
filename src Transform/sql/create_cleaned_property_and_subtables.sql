-- Properties table
CREATE TABLE cleaned_property (
    property_id BIGINT PRIMARY KEY,
    name TEXT,
    name_local TEXT,
    description TEXT,
    description_local TEXT,
    internal_status_id INT,
    loc_point POINT,
    property_level VARCHAR(255),
    property_status VARCHAR(255),
    property_type VARCHAR(255),
    main_type_name VARCHAR(255),
    main_subtype_name VARCHAR(255),
    parent_id BIGINT,
    location_id BIGINT,
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    import_date TIMESTAMP,
    import_type VARCHAR(255)
);

-- Developer prices table
CREATE TABLE property_developer_prices (
    id BIGSERIAL PRIMARY KEY,
    property_id BIGINT REFERENCES cleaned_property(property_id),
    quarter_date DATE,
    price_size_aed_int NUMERIC,
    price_size_min_usd_int NUMERIC,
    price_size_max_aed_int NUMERIC
);

-- Units table
CREATE TABLE property_units (
    id BIGSERIAL PRIMARY KEY,
    property_id BIGINT REFERENCES cleaned_property(property_id),
    unit_count INT,
    bedroom_count INT,
    size NUMERIC,
    size_min NUMERIC,
    size_max NUMERIC,
    created_at TIMESTAMP,
    updated_on TIMESTAMP
);

-- Attributes table
CREATE TABLE property_attributes (
    id BIGSERIAL PRIMARY KEY,
    property_id BIGINT REFERENCES cleaned_property(property_id),
    group_name VARCHAR(255),
    name VARCHAR(255),
    label_local VARCHAR(255),
    label VARCHAR(255),
    type VARCHAR(255),
    value_array JSONB,
    value_object JSONB,
    value_bool BOOLEAN,
    value_text TEXT,
    value_date DATE
);

-- Images table
CREATE TABLE images (
    id BIGSERIAL PRIMARY KEY,
    property_id BIGINT REFERENCES cleaned_property(property_id),
    unit_id VARCHAR(255),
    order INT,
    is_primary BOOLEAN,
    path TEXT,
    type_name VARCHAR(255),
);