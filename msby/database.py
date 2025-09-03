import logging
import psycopg2

from psycopg2.extras import execute_values
from typing import List, Dict, Any
from config import Config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def get_connection(self):
        return psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
        )

    def insert_location_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert location data from locations endpoint"""
        if not data_list:
            return
        query = """
        INSERT INTO location (
            location_id, city_id, city_name, country_code, county_id, county_name,
            description, district_id, district_name, location_name, geo_point, photo_path, raw_data
        ) VALUES %s
        ON CONFLICT (location_id) DO UPDATE SET
            city_id = EXCLUDED.city_id,
            city_name = EXCLUDED.city_name,
            country_code = EXCLUDED.country_code,
            county_id = EXCLUDED.county_id,
            county_name = EXCLUDED.county_name,
            description = EXCLUDED.description,
            district_id = EXCLUDED.district_id,
            district_name = EXCLUDED.district_name,
            location_name = EXCLUDED.location_name,
            geo_point = EXCLUDED.geo_point,
            photo_path = EXCLUDED.photo_path,
            raw_data = EXCLUDED.raw_data
        """
        self._bulk_insert(query, data_list)

    def insert_company_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert company data from company/{company_id} endpoint"""
        if not data_list:
            return
        query = """
        INSERT INTO company (id, name) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name
        """
        self._bulk_insert(query, data_list)

    def insert_property_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert property data from property/location/{location_id} endpoint"""
        if not data_list:
            return
        query = """
        INSERT INTO property (
            id, name, name_local, description, description_local, internal_status_id,
            loc_point, geo_point, loc_polygon, level, status, types, main_type_name,
            main_subtype_name, parent_id, parent_ids, parents, location_id, location,
            locations, attributes, units, developer_prices, images, primary_image,
            parties, search_terms, created_on, updated_on, import_date, import_type,
            dld_status, elapsed_time_status, raw_data
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            name_local = EXCLUDED.name_local,
            description = EXCLUDED.description,
            description_local = EXCLUDED.description_local,
            internal_status_id = EXCLUDED.internal_status_id,
            loc_point = EXCLUDED.loc_point,
            geo_point = EXCLUDED.geo_point,
            loc_polygon = EXCLUDED.loc_polygon,
            level = EXCLUDED.level,
            status = EXCLUDED.status,
            types = EXCLUDED.types,
            main_type_name = EXCLUDED.main_type_name,
            main_subtype_name = EXCLUDED.main_subtype_name,
            parent_id = EXCLUDED.parent_id,
            parent_ids = EXCLUDED.parent_ids,
            parents = EXCLUDED.parents,
            location_id = EXCLUDED.location_id,
            location = EXCLUDED.location,
            locations = EXCLUDED.locations,
            attributes = EXCLUDED.attributes,
            units = EXCLUDED.units,
            developer_prices = EXCLUDED.developer_prices,
            images = EXCLUDED.images,
            primary_image = EXCLUDED.primary_image,
            parties = EXCLUDED.parties,
            search_terms = EXCLUDED.search_terms,
            created_on = EXCLUDED.created_on,
            updated_on = EXCLUDED.updated_on,
            import_date = EXCLUDED.import_date,
            import_type = EXCLUDED.import_type,
            dld_status = EXCLUDED.dld_status,
            elapsed_time_status = EXCLUDED.elapsed_time_status,
            raw_data = EXCLUDED.raw_data
        """
        self._bulk_insert(query, data_list)

    def insert_property_details_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert property details data from property/{property_id} endpoint"""
        if not data_list:
            return
        query = """
        INSERT INTO property_details (
            id, property_id, name, name_local, description, description_local,
            developer_prices, parties, images, units, attributes, level,
            geo_point, loc_point, location_id, location, locations, parent_id,
            parent_ids, parents, primary_image, search_terms, status,
            types, elapsed_time_status, dld_status, updated_on, gla,
            office_gla, typical_gla_floor, built_up_area, building_height,
            land_area, raw_data
        ) VALUES %s
        ON CONFLICT (property_id) DO UPDATE SET
            name = EXCLUDED.name,
            name_local = EXCLUDED.name_local,
            description = EXCLUDED.description,
            description_local = EXCLUDED.description_local,
            developer_prices = EXCLUDED.developer_prices,
            parties = EXCLUDED.parties,
            images = EXCLUDED.images,
            units = EXCLUDED.units,
            attributes = EXCLUDED.attributes,
            level = EXCLUDED.level,
            geo_point = EXCLUDED.geo_point,
            loc_point = EXCLUDED.loc_point,
            location_id = EXCLUDED.location_id,
            location = EXCLUDED.location,
            locations = EXCLUDED.locations,
            parent_id = EXCLUDED.parent_id,
            parent_ids = EXCLUDED.parent_ids,
            parents = EXCLUDED.parents,
            primary_image = EXCLUDED.primary_image,
            search_terms = EXCLUDED.search_terms,
            status = EXCLUDED.status,
            types = EXCLUDED.types,
            elapsed_time_status = EXCLUDED.elapsed_time_status,
            dld_status = EXCLUDED.dld_status,
            updated_on = EXCLUDED.updated_on,
            gla = EXCLUDED.gla,
            office_gla = EXCLUDED.office_gla,
            typical_gla_floor = EXCLUDED.typical_gla_floor,
            built_up_area = EXCLUDED.built_up_area,
            building_height = EXCLUDED.building_height,
            land_area = EXCLUDED.land_area,
            raw_data = EXCLUDED.raw_data
        """
        self._bulk_insert(query, data_list)

    def insert_indicator_group_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert indicator group data from indicators/groups endpoint"""
        if not data_list:
            return
        query = """
        INSERT INTO indicator_group (
            id, name, name_local, parent_id, indicator_id, indicator_table_name
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            name_local = EXCLUDED.name_local,
            parent_id = EXCLUDED.parent_id,
            indicator_id = EXCLUDED.indicator_id,
            indicator_table_name = EXCLUDED.indicator_table_name
        """
        self._bulk_insert(query, data_list)

    def insert_indicator_aliased_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert property-level indicator data from indicators/aliased endpoint"""
        if not data_list:
            return
        query = """
        INSERT INTO indicator_aliased (
            series_id, series_name, series_name_local, location_id, location,
            currency, data_frequency, update_frequency, unit, property_id,
            property, indicator, indicator_groups, last_value,
            timepoints, property_is_null, raw_data
        ) VALUES %s
        ON CONFLICT (series_id) DO UPDATE SET
            series_name = EXCLUDED.series_name,
            series_name_local = EXCLUDED.series_name_local,
            location_id = EXCLUDED.location_id,
            location = EXCLUDED.location,
            currency = EXCLUDED.currency,
            data_frequency = EXCLUDED.data_frequency,
            update_frequency = EXCLUDED.update_frequency,
            unit = EXCLUDED.unit,
            property_id = EXCLUDED.property_id,
            property = EXCLUDED.property,
            indicator = EXCLUDED.indicator,
            indicator_groups = EXCLUDED.indicator_groups,
            last_value = EXCLUDED.last_value,
            timepoints = EXCLUDED.timepoints,
            property_is_null = EXCLUDED.property_is_null,
            raw_data = EXCLUDED.raw_data
        """
        self._bulk_insert(query, data_list)

    def insert_indicator_area_aliased_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert area-level indicator data from indicators/area-aliased endpoint"""
        if not data_list:
            return
        query = """
        INSERT INTO indicator_area_aliased (
            series_id, series_name, series_name_local, alias, property_type, price_type,
            no_of_bedrooms, indicator, data_frequency, last_value,
            indicator_property_type, indicator_property_subtype, property_is_null,
            unit, property_id, property, location_id, location,
            indicator_groups, currency, timepoints, raw_data
        ) VALUES %s
        """
        self._bulk_insert(query, data_list)

    def _bulk_insert(self, query: str, data_list: List[Dict[str, Any]]) -> None:
        """Generic bulk insert method with error handling"""
        with self.get_connection() as conn, conn.cursor() as cur:
            try:
                values = [tuple(d.values()) for d in data_list]
                logger.info("Values: %s", values[0])
                execute_values(cur, query, values)
                conn.commit()
                logger.info("Inserted/updated %d rows", len(values))
            except Exception as e:
                conn.rollback()
                logger.error("Bulk insert failed: %s", e)
                raise

    def get_locations(self, country_code: str | None = None, limit: int | None = None) -> List[Dict[str, Any]]:
        """Retrieve locations with optional country code filter"""
        query = "SELECT location_id FROM location"
        params = []
        if country_code:
            query += " WHERE country_code = %s"
            params.append(country_code)
        if limit:
            query += " LIMIT %s"
            params.append(str(limit))
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_properties(self, limit: int | None = None) -> List[Dict[str, Any]]:
        """Retrieve all properties for a specific country"""
        query = """
        SELECT id FROM property
        """
        params = []
        # if country_code:
        #     query += " WHERE country_code = %s"
        #     params.append(country_code)
        if limit:
            query += " LIMIT %s"
            params.append(str(limit))
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_property_details(self, property_id: int) -> Dict[str, Any] | None:
        """Retrieve detailed property information"""
        query = "SELECT * FROM property_details WHERE property_id = %s"
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (property_id,))
            columns = [desc[0] for desc in cur.description]
            row = cur.fetchone()
            return dict(zip(columns, row)) if row else None
