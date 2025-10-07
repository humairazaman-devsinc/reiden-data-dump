-- public.cma2_rents definition

-- Drop table

-- DROP TABLE public.cma2_rents;

CREATE TABLE public.cma2_rents (
	id serial4 NOT NULL,
	property_id int4 NULL,
	country_code varchar(5) NOT NULL,
	alias varchar(20) NOT NULL,
	currency varchar(5) NOT NULL,
	measurement varchar(10) NOT NULL,
	property_type varchar(50) NOT NULL,
	property_subtype varchar(100) NOT NULL,
	city_id int4 NULL,
	city_name varchar(255) NULL,
	county_id int4 NULL,
	county_name varchar(255) NULL,
	district_id int4 NULL,
	location_id int4 NULL,
	district_name varchar(255) NULL,
	location_name varchar(255) NULL,
	municipal_area varchar(255) NULL,
	transaction_count int4 NULL,
	average_rent numeric(15, 2) NULL,
	median_rent numeric(15, 2) NULL,
	min_rent numeric(15, 2) NULL,
	max_rent numeric(15, 2) NULL,
	rent_per_sqm numeric(15, 2) NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT cma2_rents_pkey PRIMARY KEY (id),
	CONSTRAINT unique_cma2_rents_record UNIQUE (property_id, country_code, alias, currency, measurement, property_type, property_subtype, city_id, county_id, district_id, location_id)
);
CREATE INDEX idx_cma2_rents_api_params ON public.cma2_rents USING btree (alias, currency, measurement, property_type, property_subtype);
CREATE INDEX idx_cma2_rents_created_at ON public.cma2_rents USING btree (created_at);
CREATE INDEX idx_cma2_rents_location_hierarchy ON public.cma2_rents USING btree (country_code, city_id, county_id, district_id, location_id);
CREATE INDEX idx_cma2_rents_office_medical ON public.cma2_rents USING btree (property_id, created_at) WHERE (((property_type)::text = 'Office'::text) AND ((property_subtype)::text = ANY ((ARRAY['Medical Office'::character varying, 'Office'::character varying])::text[])));
CREATE INDEX idx_cma2_rents_property_id ON public.cma2_rents USING btree (property_id);
CREATE INDEX idx_cma2_rents_property_location ON public.cma2_rents USING btree (property_id, city_id, county_id, district_id);


-- public.cma_data definition

-- Drop table

-- DROP TABLE public.cma_data;

CREATE TABLE public.cma_data (
	property_id int8 NULL,
	query_property_type varchar(255) NULL,
	query_activity_type varchar(255) NULL,
	alias varchar(100) NULL,
	currency varchar(100) NULL,
	measurement varchar(100) NULL,
	term text NULL,
	value text NULL,
	additional_data jsonb NULL,
	raw_data jsonb NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL
);
CREATE UNIQUE INDEX cma_data_unique_upsert_idx ON public.cma_data USING btree (property_id, query_property_type, query_activity_type, alias, term);


-- public.cma_rents definition

-- Drop table

-- DROP TABLE public.cma_rents;

CREATE TABLE public.cma_rents (
	property_id int8 NULL,
	alias varchar(255) NULL,
	currency varchar(10) NULL,
	measurement varchar(50) NULL,
	property_type varchar(100) NULL,
	property_subtype varchar(100) NULL,
	comparable_property_id int8 NULL,
	price numeric(20, 4) NULL,
	price_per_size numeric(20, 4) NULL,
	comparable_size numeric(20, 4) NULL,
	transaction_date timestamptz NULL,
	activity_type varchar(255) NULL,
	"location" jsonb NULL,
	comparable_no_of_bedrooms int4 NULL,
	number_of_floors varchar(100) NULL,
	number_of_unit varchar(100) NULL,
	parent_property jsonb NULL,
	property_name text NULL,
	property_nature varchar(255) NULL,
	rent_type varchar(255) NULL,
	raw_data jsonb NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT cma_rents_unique UNIQUE (property_id, alias, currency, measurement, property_type, property_subtype, comparable_property_id, transaction_date)
);


-- public.cma_sales definition

-- Drop table

-- DROP TABLE public.cma_sales;

CREATE TABLE public.cma_sales (
	id bigserial NOT NULL,
	property_id int8 NOT NULL,
	alias varchar(50) NOT NULL,
	currency varchar(10) NOT NULL,
	measurement varchar(10) NOT NULL,
	property_type varchar(100) NOT NULL,
	property_subtype varchar(200) NOT NULL,
	comparable_property_id int8 NOT NULL,
	comparable_property_name varchar(500) NULL,
	"size" numeric(15, 2) NULL,
	price numeric(15, 2) NULL,
	price_per_size numeric(15, 2) NULL,
	transaction_date timestamp NULL,
	city_id int4 NULL,
	city_name varchar(255) NULL,
	county_id int4 NULL,
	county_name varchar(255) NULL,
	district_id int4 NULL,
	district_name varchar(255) NULL,
	location_id int4 NULL,
	location_name varchar(255) NULL,
	municipal_area varchar(255) NULL,
	activity_type varchar(100) NULL,
	no_of_bedrooms int4 NULL,
	number_of_unit varchar(50) NULL,
	property_nature varchar(100) NULL,
	number_of_floors varchar(50) NULL,
	parent_property jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	raw_data jsonb NULL,
	CONSTRAINT cma_sales_pkey PRIMARY KEY (id),
	CONSTRAINT cma_sales_property_id_alias_currency_measurement_property_t_key UNIQUE (property_id, alias, currency, measurement, property_type, property_subtype, comparable_property_id)
);
CREATE INDEX idx_cma_sales_alias ON public.cma_sales USING btree (alias);
CREATE INDEX idx_cma_sales_city_id ON public.cma_sales USING btree (city_id);
CREATE INDEX idx_cma_sales_comparable_property_id ON public.cma_sales USING btree (comparable_property_id);
CREATE INDEX idx_cma_sales_currency ON public.cma_sales USING btree (currency);
CREATE INDEX idx_cma_sales_district_id ON public.cma_sales USING btree (district_id);
CREATE INDEX idx_cma_sales_measurement ON public.cma_sales USING btree (measurement);
CREATE INDEX idx_cma_sales_price ON public.cma_sales USING btree (price);
CREATE INDEX idx_cma_sales_property_id ON public.cma_sales USING btree (property_id);
CREATE INDEX idx_cma_sales_property_subtype ON public.cma_sales USING btree (property_subtype);
CREATE INDEX idx_cma_sales_property_type ON public.cma_sales USING btree (property_type);
CREATE INDEX idx_cma_sales_transaction_date ON public.cma_sales USING btree (transaction_date);


-- public.indicator_aliased definition

-- Drop table

-- DROP TABLE public.indicator_aliased;

CREATE TABLE public.indicator_aliased (
	series_id int8 NOT NULL,
	series_name varchar(255) NULL,
	series_name_local varchar(255) NULL,
	currency jsonb NULL,
	data_frequency jsonb NULL,
	update_frequency jsonb NULL,
	unit jsonb NULL,
	location_id int8 NULL,
	"location" jsonb NULL,
	property_id int8 NULL,
	property jsonb NULL,
	"indicator" jsonb NULL,
	indicator_groups jsonb NULL,
	"last_value" jsonb NULL,
	timepoints jsonb NULL,
	property_is_null bool NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT indicator_aliased_pkey PRIMARY KEY (series_id)
);


-- public.indicator_location definition

-- Drop table

-- DROP TABLE public.indicator_location;

CREATE TABLE public.indicator_location (
	id serial4 NOT NULL,
	location_id int4 NOT NULL,
	currency varchar(10) NOT NULL,
	measurement varchar(10) NOT NULL,
	page_number int4 NOT NULL,
	indicator_id int4 NULL,
	indicator_name varchar(500) NULL,
	indicator_value numeric(20, 4) NULL,
	indicator_value_date varchar(50) NULL,
	indicator_value_difference numeric(20, 4) NULL,
	level_id int4 NULL,
	level_name varchar(100) NULL,
	location_name varchar(255) NULL,
	property_id int4 NULL,
	property_name varchar(255) NULL,
	property_nature varchar(100) NULL,
	property_subtype varchar(100) NULL,
	property_type varchar(100) NULL,
	internal_status_id int4 NULL,
	parent_info jsonb NULL,
	timepoints jsonb NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT indicator_location_location_id_currency_measurement_page_nu_key UNIQUE (location_id, currency, measurement, page_number, indicator_id),
	CONSTRAINT indicator_location_pkey PRIMARY KEY (id)
);


-- public."location" definition

-- Drop table

-- DROP TABLE public."location";

CREATE TABLE public."location" (
	location_id int8 NOT NULL,
	city_id int8 NULL,
	city_name varchar(255) NULL,
	country_code varchar(10) NULL,
	county_id int8 NULL,
	county_name varchar(255) NULL,
	district_id int8 NULL,
	district_name varchar(255) NULL,
	location_name varchar(255) NULL,
	geo_point jsonb NULL,
	photo_path text NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT now() NULL,
	description varchar NULL,
	text_embedding public.vector NULL,
	searchable_text text NULL,
	CONSTRAINT location_pkey PRIMARY KEY (location_id)
);
CREATE INDEX idx_location_city_county ON public.location USING btree (city_name, county_name);
COMMENT ON INDEX public.idx_location_city_county IS 'Composite index for city/county filtering queries';
CREATE INDEX idx_location_district ON public.location USING btree (district_name) WHERE (district_name IS NOT NULL);
COMMENT ON INDEX public.idx_location_district IS 'Index for district filtering queries';
CREATE INDEX idx_location_searchable_text_gin ON public.location USING gin (to_tsvector('english'::regconfig, searchable_text));
COMMENT ON INDEX public.idx_location_searchable_text_gin IS 'Full-text search index on combined location text';
CREATE INDEX idx_location_text_embedding_cosine ON public.location USING ivfflat (text_embedding vector_cosine_ops) WITH (lists='100');
COMMENT ON INDEX public.idx_location_text_embedding_cosine IS 'Vector index for cosine similarity search on text embeddings';

-- Table Triggers

create trigger update_location_searchable_text before
insert
    or
update
    on
    public.location for each row execute function generate_location_searchable_text();


-- public.payment_plans definition

-- Drop table

-- DROP TABLE public.payment_plans;

CREATE TABLE public.payment_plans (
	id serial4 NOT NULL,
	property_id int4 NOT NULL,
	payment_plan_id int4 NULL,
	plan_name varchar(500) NULL,
	plan_description text NULL,
	plan_type varchar(100) NULL,
	plan_status varchar(100) NULL,
	total_amount numeric(20, 4) NULL,
	currency varchar(10) NULL,
	down_payment numeric(20, 4) NULL,
	down_payment_percentage numeric(5, 2) NULL,
	installment_count int4 NULL,
	installment_amount numeric(20, 4) NULL,
	installment_frequency varchar(50) NULL,
	start_date date NULL,
	end_date date NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	raw_data jsonb NULL,
	CONSTRAINT payment_plans_pkey PRIMARY KEY (id),
	CONSTRAINT payment_plans_property_id_payment_plan_id_key UNIQUE (property_id, payment_plan_id)
);


-- public.poi_cma definition

-- Drop table

-- DROP TABLE public.poi_cma;

CREATE TABLE public.poi_cma (
	id serial4 NOT NULL,
	property_id int4 NOT NULL,
	measurement varchar(10) NOT NULL,
	lang varchar(5) NOT NULL,
	lat varchar(50) NULL,
	lon varchar(50) NULL,
	poi_subtype_name varchar(255) NULL,
	poi_type_name varchar(255) NULL,
	poi_name varchar(500) NULL,
	poi_review int4 NULL,
	poi_rating numeric(3, 2) NULL,
	poi_distance numeric(15, 10) NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT poi_cma_pkey PRIMARY KEY (id),
	CONSTRAINT poi_cma_property_id_measurement_lang_poi_name_poi_type_name_key UNIQUE (property_id, measurement, lang, poi_name, poi_type_name, poi_subtype_name, lat, lon)
);
CREATE INDEX idx_poi_cma_created_at ON public.poi_cma USING btree (created_at);
CREATE INDEX idx_poi_cma_lang ON public.poi_cma USING btree (lang);
CREATE INDEX idx_poi_cma_measurement ON public.poi_cma USING btree (measurement);
CREATE INDEX idx_poi_cma_poi_distance ON public.poi_cma USING btree (poi_distance);
CREATE INDEX idx_poi_cma_poi_subtype_name ON public.poi_cma USING btree (poi_subtype_name);
CREATE INDEX idx_poi_cma_poi_type_name ON public.poi_cma USING btree (poi_type_name);
CREATE INDEX idx_poi_cma_property_id ON public.poi_cma USING btree (property_id);


-- public.transaction_list definition

-- Drop table

-- DROP TABLE public.transaction_list;

CREATE TABLE public.transaction_list (
	id serial4 NOT NULL,
	transaction_id varchar(255) NOT NULL,
	date_transaction varchar(50) NULL,
	price numeric(18, 2) NULL,
	price_per_size numeric(18, 2) NULL,
	"size" numeric(18, 2) NULL,
	size_land numeric(18, 2) NULL,
	location_id int4 NOT NULL,
	loc_city_id int4 NULL,
	loc_city_name varchar(255) NULL,
	loc_county_id int4 NULL,
	loc_county_name varchar(255) NULL,
	loc_district_id int4 NULL,
	loc_district_name varchar(255) NULL,
	loc_location_id int4 NULL,
	loc_location_name varchar(255) NULL,
	loc_municipal_area varchar(255) NULL,
	property_id int4 NULL,
	property_name varchar(255) NULL,
	property_type_name varchar(255) NULL,
	property_subtype_name varchar(255) NULL,
	municipal_property_type varchar(255) NULL,
	attr_unit varchar(50) NULL,
	attr_floor varchar(50) NULL,
	attr_parking varchar(50) NULL,
	attr_land_number varchar(50) NULL,
	attr_no_of_rooms int4 NULL,
	attr_balcony_area numeric(18, 2) NULL,
	attr_building_name varchar(255) NULL,
	attr_building_number varchar(50) NULL,
	query_property_type varchar(50) NOT NULL,
	query_activity_type varchar(50) NOT NULL,
	query_currency varchar(10) NOT NULL,
	query_measurement varchar(10) NOT NULL,
	raw_data text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT transaction_list_location_id_query_property_type_query_acti_key UNIQUE (location_id, query_property_type, query_activity_type, query_currency, query_measurement, transaction_id),
	CONSTRAINT transaction_list_pkey PRIMARY KEY (id)
);


-- public.transaction_raw_rent definition

-- Drop table

-- DROP TABLE public.transaction_raw_rent;

CREATE TABLE public.transaction_raw_rent (
	id serial4 NOT NULL,
	"date" timestamptz NULL,
	"size" numeric(20, 2) NULL,
	price numeric(20, 2) NULL,
	end_date timestamptz NULL,
	start_date timestamptz NULL,
	size_land_int numeric(20, 2) NULL,
	price_per_size numeric(20, 2) NULL,
	transaction_type varchar(1000) NULL,
	transaction_version varchar(1000) NULL,
	loc_city_id int8 NULL,
	loc_city_name varchar(1000) NULL,
	loc_county_id int8 NULL,
	loc_county_name varchar(1000) NULL,
	loc_district_id int8 NULL,
	loc_location_id int8 NULL,
	loc_district_name varchar(1000) NULL,
	loc_location_name varchar(1000) NULL,
	loc_municipal_area varchar(1000) NULL,
	prop_property_id int8 NULL,
	prop_property_name varchar(1000) NULL,
	attr_unit varchar(1000) NULL,
	attr_floor varchar(1000) NULL,
	attr_parking varchar(1000) NULL,
	attr_land_number varchar(1000) NULL,
	attr_no_of_rooms varchar(1000) NULL,
	attr_balcony_area varchar(1000) NULL,
	attr_building_name varchar(1000) NULL,
	attr_building_number varchar(1000) NULL,
	prop_type_name varchar(1000) NULL,
	prop_subtype_name varchar(1000) NULL,
	currency varchar(50) DEFAULT 'aed'::character varying NULL,
	measurement varchar(50) NULL,
	raw_data jsonb NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT transaction_raw_rent_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_transaction_raw_rent_created_at ON public.transaction_raw_rent USING btree (created_at);
CREATE INDEX idx_transaction_raw_rent_currency_measurement ON public.transaction_raw_rent USING btree (currency, measurement);
CREATE INDEX idx_transaction_raw_rent_date ON public.transaction_raw_rent USING btree (date);
CREATE INDEX idx_transaction_raw_rent_location ON public.transaction_raw_rent USING btree (loc_location_id);
CREATE UNIQUE INDEX idx_transaction_raw_rent_unique ON public.transaction_raw_rent USING btree (loc_location_id, currency, measurement, date, price, size) WHERE ((date IS NOT NULL) AND (price IS NOT NULL));


-- public.transaction_raw_rent_backup definition

-- Drop table

-- DROP TABLE public.transaction_raw_rent_backup;

CREATE TABLE public.transaction_raw_rent_backup (
	id int4 NULL,
	location_id int4 NULL,
	currency varchar(10) NULL,
	measurement varchar(10) NULL,
	raw_data jsonb NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL,
	attr_balcony_area text NULL,
	attr_building_name text NULL,
	attr_building_number text NULL,
	attr_floor text NULL,
	attr_land_number text NULL,
	attr_no_of_rooms text NULL,
	attr_parking text NULL,
	attr_unit text NULL,
	"date" text NULL,
	end_date text NULL,
	loc_city_id text NULL,
	loc_city_name text NULL,
	loc_county_id text NULL,
	loc_county_name text NULL,
	loc_district_id text NULL,
	loc_district_name text NULL,
	loc_location_id text NULL,
	loc_location_name text NULL,
	loc_municipal_area text NULL,
	price text NULL,
	price_per_size text NULL,
	property_id text NULL,
	property_name text NULL,
	property_subtype_name text NULL,
	property_type_name text NULL,
	"size" text NULL,
	size_land_int text NULL,
	start_date text NULL,
	transaction_type text NULL,
	transaction_version text NULL,
	size_land_imp text NULL
);


-- public.transaction_sales definition

-- Drop table

-- DROP TABLE public.transaction_sales;

CREATE TABLE public.transaction_sales (
	id bigserial NOT NULL,
	start_date date NULL,
	end_date date NULL,
	"date" date NULL,
	transaction_version varchar(100) NULL,
	transaction_type varchar(200) NULL,
	location_id int8 NULL,
	loc_city_id int8 NULL,
	loc_city_name varchar(255) NULL,
	loc_county_id int8 NULL,
	loc_county_name varchar(255) NULL,
	loc_district_id int8 NULL,
	loc_district_name varchar(255) NULL,
	loc_location_id int8 NULL,
	loc_location_name varchar(255) NULL,
	loc_municipal_area varchar(255) NULL,
	property_id int8 NULL,
	property_name varchar(500) NULL,
	property_type_name varchar(200) NULL,
	property_subtype_name varchar(200) NULL,
	price numeric(15, 2) NULL,
	price_per_size numeric(15, 2) NULL,
	"size" numeric(15, 2) NULL,
	size_land_imp numeric(15, 2) NULL,
	attr_unit varchar(50) NULL,
	attr_floor varchar(50) NULL,
	attr_parking varchar(200) NULL,
	attr_land_number varchar(50) NULL,
	attr_no_of_rooms int4 NULL,
	attr_balcony_area numeric(15, 2) NULL,
	attr_building_name varchar(500) NULL,
	attr_building_number varchar(50) NULL,
	currency varchar(10) NULL,
	measurement varchar(10) NULL,
	bedroom int4 NULL,
	size_range varchar(50) NULL,
	price_range varchar(50) NULL,
	transaction_date varchar(200) NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	raw_data jsonb NULL,
	CONSTRAINT transaction_sales_location_id_start_date_end_date_price_tra_key UNIQUE (location_id, start_date, end_date, price, transaction_type, date),
	CONSTRAINT transaction_sales_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_transaction_sales_date ON public.transaction_sales USING btree (date);
CREATE INDEX idx_transaction_sales_end_date ON public.transaction_sales USING btree (end_date);
CREATE INDEX idx_transaction_sales_location_id ON public.transaction_sales USING btree (location_id);
CREATE INDEX idx_transaction_sales_price ON public.transaction_sales USING btree (price);
CREATE INDEX idx_transaction_sales_property_id ON public.transaction_sales USING btree (property_id);
CREATE INDEX idx_transaction_sales_property_type ON public.transaction_sales USING btree (property_type_name);
CREATE INDEX idx_transaction_sales_start_date ON public.transaction_sales USING btree (start_date);
CREATE INDEX idx_transaction_sales_transaction_type ON public.transaction_sales USING btree (transaction_type);


-- public.property definition

-- Drop table

-- DROP TABLE public.property;

CREATE TABLE public.property (
	id int8 NOT NULL,
	"name" varchar(255) NULL,
	name_local varchar(255) NULL,
	description text NULL,
	description_local text NULL,
	internal_status_id int8 NULL,
	loc_point jsonb NULL,
	geo_point jsonb NULL,
	loc_polygon jsonb NULL,
	"level" jsonb NULL,
	status jsonb NULL,
	"types" jsonb NULL,
	main_type_name varchar(255) NULL,
	main_subtype_name varchar(255) NULL,
	parent_id int8 NULL,
	parent_ids _int4 NULL,
	parents jsonb NULL,
	location_id int8 NULL,
	"location" jsonb NULL,
	locations jsonb NULL,
	"attributes" jsonb NULL,
	units jsonb NULL,
	developer_prices jsonb NULL,
	images jsonb NULL,
	primary_image text NULL,
	parties jsonb NULL,
	search_terms _varchar NULL,
	created_on timestamp NULL,
	updated_on timestamp NULL,
	import_date timestamp NULL,
	import_type varchar(50) NULL,
	dld_status varchar(255) NULL,
	elapsed_time_status int4 NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT now() NULL,
	available_units int4 DEFAULT 0 NULL,
	available_unit_pct numeric(10, 4) DEFAULT 0.0000 NULL,
	CONSTRAINT property_pkey PRIMARY KEY (id),
	CONSTRAINT property_location_fk FOREIGN KEY (location_id) REFERENCES public."location"(location_id)
);


-- public.transaction_history definition

-- Drop table

-- DROP TABLE public.transaction_history;

CREATE TABLE public.transaction_history (
	id bigserial NOT NULL,
	property_id int8 NULL,
	date_transaction timestamptz NULL,
	land_number text NULL,
	loc_city_id int4 NULL,
	loc_city_name text NULL,
	loc_county_id int4 NULL,
	loc_county_name text NULL,
	loc_district_id int4 NULL,
	loc_district_name text NULL,
	loc_location_id int4 NULL,
	loc_location_name text NULL,
	loc_municipal_area text NULL,
	price_aed numeric(20, 4) NULL,
	price_size_aed_imp numeric(20, 4) NULL,
	property_name text NULL,
	property_type_municipal_property_type text NULL,
	property_type_subtype_name text NULL,
	property_type_type_name text NULL,
	size_imp numeric(20, 4) NULL,
	size_land_imp numeric(20, 4) NULL,
	raw_data jsonb NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT transaction_history_pkey PRIMARY KEY (id),
	CONSTRAINT transaction_history_property_fk FOREIGN KEY (property_id) REFERENCES public.property(id)
);
CREATE UNIQUE INDEX transaction_history_unique_upsert_idx ON public.transaction_history USING btree (property_id, date_transaction);


-- public.transactions_avg definition

-- Drop table

-- DROP TABLE public.transactions_avg;

CREATE TABLE public.transactions_avg (
	id bigserial NOT NULL,
	location_id int8 NOT NULL,
	location_name varchar(255) NULL,
	property_type varchar(255) NOT NULL,
	activity_type varchar(255) NOT NULL,
	currency varchar(10) NOT NULL,
	measurement varchar(10) NOT NULL,
	no_of_bedrooms int4 NULL,
	date_period varchar(10) NOT NULL,
	"year" int4 NOT NULL,
	"month" int4 NOT NULL,
	average_net_price numeric(15, 2) NULL,
	average_unit_price numeric(15, 2) NULL,
	total_count int4 NULL,
	total_price numeric(15, 2) NULL,
	price_per_sqm numeric(15, 2) NULL,
	transaction_volume numeric(15, 2) NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	raw_data jsonb NULL,
	CONSTRAINT transactions_avg_location_id_property_type_activity_type_cu_key UNIQUE (location_id, property_type, activity_type, currency, measurement, no_of_bedrooms, date_period),
	CONSTRAINT transactions_avg_pkey PRIMARY KEY (id),
	CONSTRAINT transactions_avg_location_fk FOREIGN KEY (location_id) REFERENCES public."location"(location_id)
);
CREATE INDEX idx_transactions_avg_activity_type ON public.transactions_avg USING btree (activity_type);
CREATE INDEX idx_transactions_avg_currency ON public.transactions_avg USING btree (currency);
CREATE INDEX idx_transactions_avg_date_period ON public.transactions_avg USING btree (date_period);
CREATE INDEX idx_transactions_avg_location_date ON public.transactions_avg USING btree (location_id, date_period);
CREATE INDEX idx_transactions_avg_location_id ON public.transactions_avg USING btree (location_id);
CREATE INDEX idx_transactions_avg_location_property ON public.transactions_avg USING btree (location_id, property_type, activity_type);
CREATE INDEX idx_transactions_avg_no_of_bedrooms ON public.transactions_avg USING btree (no_of_bedrooms);
CREATE INDEX idx_transactions_avg_property_date ON public.transactions_avg USING btree (property_type, activity_type, date_period);
CREATE INDEX idx_transactions_avg_property_type ON public.transactions_avg USING btree (property_type);
CREATE INDEX idx_transactions_avg_year_month ON public.transactions_avg USING btree (year, month);


-- public.transactions_price definition

-- Drop table

-- DROP TABLE public.transactions_price;

CREATE TABLE public.transactions_price (
	id bigserial NOT NULL,
	location_id int8 NOT NULL,
	property_type varchar(100) NOT NULL,
	activity_type varchar(100) NOT NULL,
	property_id int8 NULL,
	property_sub_type varchar(200) NULL,
	no_of_bedrooms int4 NULL,
	term varchar(255) NULL,
	value numeric(15, 2) NULL,
	additional_data jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	raw_data jsonb NULL,
	CONSTRAINT transactions_price_location_id_property_type_activity_type__key UNIQUE (location_id, property_type, activity_type, property_id, property_sub_type, no_of_bedrooms, term),
	CONSTRAINT transactions_price_pkey PRIMARY KEY (id),
	CONSTRAINT transactions_price_location_fk FOREIGN KEY (location_id) REFERENCES public."location"(location_id)
);
CREATE INDEX idx_transactions_price_activity_type ON public.transactions_price USING btree (activity_type);
CREATE INDEX idx_transactions_price_location_id ON public.transactions_price USING btree (location_id);
CREATE INDEX idx_transactions_price_no_of_bedrooms ON public.transactions_price USING btree (no_of_bedrooms);
CREATE INDEX idx_transactions_price_property_id ON public.transactions_price USING btree (property_id);
CREATE INDEX idx_transactions_price_property_sub_type ON public.transactions_price USING btree (property_sub_type);
CREATE INDEX idx_transactions_price_property_type ON public.transactions_price USING btree (property_type);
CREATE INDEX idx_transactions_price_term ON public.transactions_price USING btree (term);