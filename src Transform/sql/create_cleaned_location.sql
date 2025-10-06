CREATE TABLE cleaned_location (
    location_id BIGINT PRIMARY KEY,
    location_name VARCHAR(255) NOT NULL,
    city_id BIGINT NOT NULL,
    county_id BIGINT NOT NULL,
    district_id BIGINT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    description TEXT,
    geo_point POINT,
    photo_path TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES location (location_id),
    CONSTRAINT fk_county FOREIGN KEY (county_id) REFERENCES location (location_id),
    CONSTRAINT fk_district FOREIGN KEY (district_id) REFERENCES location (location_id)
);
