import logging
import psycopg2
import json
import threading
import time

from psycopg2.extras import execute_values
from psycopg2 import pool
from typing import List, Dict, Any, Optional, Set
from .config import Config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        # Initialize connection pool for better performance
        self._pool = None
        self._pool_lock = threading.Lock()
    
    def _get_pool(self):
        """Get or create connection pool (thread-safe)"""
        if self._pool is None:
            with self._pool_lock:
                if self._pool is None:
                    try:
                        self._pool = psycopg2.pool.ThreadedConnectionPool(
                            minconn=20,  # Minimum connections
                            maxconn=200,  # Maximum connections (increased for very high-volume processing)
                            host=Config.DB_HOST,
                            port=Config.DB_PORT,
                            dbname=Config.DB_NAME,
                            user=Config.DB_USER,
                            password=Config.DB_PASSWORD,
                            connect_timeout=10,
                            application_name='cma_sales_importer',
                            options='-c statement_timeout=30000'  # 30 second statement timeout
                        )
                        logger.info("✅ Database connection pool initialized (10-100 connections)")
                    except Exception as e:
                        logger.error(f"❌ Failed to create connection pool: {e}")
                        raise
        return self._pool

    def get_connection(self):
        """Get connection from pool with timeout and retry logic"""
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                pool = self._get_pool()
                # Add timeout to prevent hanging
                conn = pool.getconn()
                if conn:
                    return conn
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt + 1} failed to get connection from pool: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"❌ All attempts failed to get connection from pool")
                    # Fallback to direct connection
                    return psycopg2.connect(
                        host=Config.DB_HOST,
                        port=Config.DB_PORT,
                        dbname=Config.DB_NAME,
                        user=Config.DB_USER,
                        password=Config.DB_PASSWORD,
                        connect_timeout=10,
                        application_name='cma_sales_importer',
                        options='-c statement_timeout=30000'
                    )

    def return_connection(self, conn):
        """Return connection to pool with proper error handling"""
        try:
            if conn and not conn.closed:
                if self._pool:
                    self._pool.putconn(conn)
                else:
                    # If no pool, close the connection
                    conn.close()
        except Exception as e:
            logger.error(f"❌ Failed to return connection to pool: {e}")
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception as close_error:
                logger.error(f"❌ Failed to close connection: {close_error}")
                pass

    def run_query(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Run a query and return the results"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error("Query failed: %s", e)
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def transform_data(self, update_query: str, params: Optional[List[Any]] = None) -> None:
        """Transform data in the database using a provided update query."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(update_query, params)
                conn.commit()
                logger.info("Data transformation completed successfully.")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error("Data transformation failed: %s", e)
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def bulk_insert(self, query: str, data_list: List[Dict[str, Any]]) -> None:
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
        self.bulk_insert(query, data_list)

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
        self.bulk_insert(query, data_list)

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
        self.bulk_insert(query, data_list)

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
        self.bulk_insert(query, data_list)


    def get_locations(self, country_code: str | None = None, limit: int | None = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve locations with optional country code filter"""
        query = "SELECT location_id FROM location"
        params = []
        if country_code:
            query += " WHERE country_code = %s"
            params.append(country_code)
        if offset > 0:
            query += " OFFSET %s"
            params.append(str(offset))
        if limit:
            query += " LIMIT %s"
            params.append(str(limit))
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_properties(self, limit: int | None = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve all properties for a specific country with proper LIMIT and OFFSET"""
        query = """
        SELECT id FROM property
        ORDER BY id
        """
        params = []
        # if country_code:
        #     query += " WHERE country_code = %s"
        #     params.append(country_code)
        if offset > 0:
            query += " OFFSET %s"
            params.append(str(offset))
        if limit:
            query += " LIMIT %s"
            params.append(str(limit))
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_property_of_type(self, property_type: str, limit: int | None = None) -> List[Dict[str, Any]]:
        """Retrieve all properties for a specific type"""
        query = """
        SELECT id FROM property WHERE types @> jsonb_build_array(jsonb_build_object('name', %s))
        """
        params = []
        params.append(property_type)
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

    def insert_transactions_avg_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert transactions average data using the improved normalized schema"""
        if not data_list:
            logger.warning("No data to insert")
            return
            
        # Prepare the insert query for the improved schema
        insert_query = """
        INSERT INTO transactions_avg (
            location_id, location_name, property_type, activity_type, 
            currency, measurement, no_of_bedrooms, date_period, 
            year, month, average_net_price, average_unit_price, 
            total_count, total_price, price_per_sqm, transaction_volume, raw_data
        ) VALUES %s ON CONFLICT (location_id, property_type, activity_type, currency, measurement, no_of_bedrooms, date_period) 
        DO UPDATE SET
            location_name = EXCLUDED.location_name,
            year = EXCLUDED.year,
            month = EXCLUDED.month,
            average_net_price = EXCLUDED.average_net_price,
            average_unit_price = EXCLUDED.average_unit_price,
            total_count = EXCLUDED.total_count,
            total_price = EXCLUDED.total_price,
            price_per_sqm = EXCLUDED.price_per_sqm,
            transaction_volume = EXCLUDED.transaction_volume,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            # Ensure data is in the correct order for bulk insert
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('location_id'),
                    record.get('location_name'),
                    record.get('property_type'),
                    record.get('activity_type'),
                    record.get('currency'),
                    record.get('measurement'),
                    record.get('no_of_bedrooms'),
                    record.get('date_period'),
                    record.get('year'),
                    record.get('month'),
                    record.get('average_net_price'),
                    record.get('average_unit_price'),
                    record.get('total_count'),
                    record.get('total_price'),
                    record.get('price_per_sqm'),
                    record.get('transaction_volume'),
                    record.get('raw_data')
                ]
                ordered_data.append(ordered_record)
            
            # Use execute_values directly for better control
            with self.get_connection() as conn, conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, insert_query, ordered_data)
                conn.commit()
                logger.info(f"Successfully inserted/updated {len(data_list)} transactions average records")
        except Exception as e:
            logger.error(f"Failed to insert transactions average data: {e}")
            raise

    def insert_transaction_history_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert transaction history data using the normalized schema"""
        if not data_list:
            logger.warning("No data to insert")
            return
            
        # Prepare the insert query for the transaction history schema
        insert_query = """
        INSERT INTO transaction_history (
            date_transaction, land_number, municipal_area,
            loc_city_id, loc_city_name, loc_county_id, loc_county_name,
            loc_district_id, loc_district_name, loc_location_id, loc_location_name, loc_municipal_area,
            property_id, property_name, property_type_municipal_property_type, property_type_subtype_name, property_type_type_name,
            price_aed, price_size_aed_imp, size_imp, size_land_imp,
            transaction_type, building_number, building_name, unit, floor, unit_min_size, unit_max_size,
            raw_data
        ) VALUES %s ON CONFLICT (date_transaction, land_number, municipal_area, property_id, price_aed) 
        DO UPDATE SET
            loc_city_id = EXCLUDED.loc_city_id,
            loc_city_name = EXCLUDED.loc_city_name,
            loc_county_id = EXCLUDED.loc_county_id,
            loc_county_name = EXCLUDED.loc_county_name,
            loc_district_id = EXCLUDED.loc_district_id,
            loc_district_name = EXCLUDED.loc_district_name,
            loc_location_id = EXCLUDED.loc_location_id,
            loc_location_name = EXCLUDED.loc_location_name,
            loc_municipal_area = EXCLUDED.loc_municipal_area,
            property_name = EXCLUDED.property_name,
            property_type_municipal_property_type = EXCLUDED.property_type_municipal_property_type,
            property_type_subtype_name = EXCLUDED.property_type_subtype_name,
            property_type_type_name = EXCLUDED.property_type_type_name,
            price_size_aed_imp = EXCLUDED.price_size_aed_imp,
            size_imp = EXCLUDED.size_imp,
            size_land_imp = EXCLUDED.size_land_imp,
            transaction_type = EXCLUDED.transaction_type,
            building_number = EXCLUDED.building_number,
            building_name = EXCLUDED.building_name,
            unit = EXCLUDED.unit,
            floor = EXCLUDED.floor,
            unit_min_size = EXCLUDED.unit_min_size,
            unit_max_size = EXCLUDED.unit_max_size,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            # Ensure data is in the correct order for bulk insert
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('date_transaction'),
                    record.get('land_number'),
                    record.get('municipal_area'),
                    record.get('loc_city_id'),
                    record.get('loc_city_name'),
                    record.get('loc_county_id'),
                    record.get('loc_county_name'),
                    record.get('loc_district_id'),
                    record.get('loc_district_name'),
                    record.get('loc_location_id'),
                    record.get('loc_location_name'),
                    record.get('loc_municipal_area'),
                    record.get('property_id'),
                    record.get('property_name'),
                    record.get('property_type_municipal_property_type'),
                    record.get('property_type_subtype_name'),
                    record.get('property_type_type_name'),
                    record.get('price_aed'),
                    record.get('price_size_aed_imp'),
                    record.get('size_imp'),
                    record.get('size_land_imp'),
                    record.get('transaction_type'),
                    record.get('building_number'),
                    record.get('building_name'),
                    record.get('unit'),
                    record.get('floor'),
                    record.get('unit_min_size'),
                    record.get('unit_max_size'),
                    record.get('raw_data')
                ]
                ordered_data.append(ordered_record)
            
            # Use execute_values directly for better control
            with self.get_connection() as conn, conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, insert_query, ordered_data)
                conn.commit()
                logger.info(f"Successfully inserted/updated {len(data_list)} transaction history records")
        except Exception as e:
            logger.error(f"Failed to insert transaction history data: {e}")
            raise
    def insert_transaction_raw_rent_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert transaction raw rent data using ultra-fast COPY method (Citus Data optimized)"""
        if not data_list:
            logger.warning("No data to insert")
            return
            
        # Use COPY for maximum performance - process in large batches
        batch_size = 50000  # Large batches for COPY efficiency
        total_batches = (len(data_list) + batch_size - 1) // batch_size
        
        # Define the exact column order for the new schema
        column_order = [
            'date', 'size', 'price', 'end_date', 'start_date', 'size_land_int', 'price_per_size',
            'transaction_type', 'transaction_version', 'loc_city_id', 'loc_city_name', 'loc_county_id',
            'loc_county_name', 'loc_district_id', 'loc_location_id', 'loc_district_name', 'loc_location_name',
            'loc_municipal_area', 'prop_property_id', 'prop_property_name', 'attr_unit', 'attr_floor',
            'attr_parking', 'attr_land_number', 'attr_no_of_rooms', 'attr_balcony_area', 'attr_building_name',
            'attr_building_number', 'prop_type_name', 'prop_subtype_name', 'currency', 'measurement', 'raw_data'
        ]
        
        # Process batches with COPY method
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(data_list))
            batch_data = data_list[start_idx:end_idx]
            
            # Use COPY for ultra-fast bulk insert
            self._bulk_insert_with_copy_optimized(batch_data, column_order)
            
            logger.info(f"🚀 COPY insert completed batch {batch_num + 1}/{total_batches}: {len(batch_data)} records")
        
        logger.info(f"✅ Successfully inserted {len(data_list)} transaction raw rent records using COPY method")

    def _bulk_insert_with_copy_optimized(self, batch_data: List[Dict[str, Any]], column_order: List[str]) -> None:
        """Ultra-fast bulk insert using PostgreSQL COPY with duplicate handling"""
        import io
        import json
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Create a StringIO buffer for COPY
                    output = io.StringIO()
                    
                    # Prepare data for COPY format (tab-delimited)
                    for item in batch_data:
                        row_data = []
                        for col in column_order:
                            value = item.get(col)
                            if value is None:
                                row_data.append('\\N')  # NULL representation for COPY
                            elif isinstance(value, dict):
                                # Convert dict to JSON string and escape for COPY
                                json_str = json.dumps(value).replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                                row_data.append(json_str)
                            else:
                                # Convert to string and escape for COPY
                                str_value = str(value).replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                                row_data.append(str_value)
                        
                        output.write('\t'.join(row_data) + '\n')
                    
                    # Reset buffer position
                    output.seek(0)
                    
                    # Use COPY for ultra-fast bulk insert
                    columns_str = ", ".join(column_order)
                    copy_query = f"COPY transaction_raw_rent ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
                    
                    try:
                        cursor.copy_expert(copy_query, output)
                        conn.commit()
                        logger.info(f"✅ COPY insert successful for {len(batch_data)} records")
                    except Exception as copy_error:
                        # If COPY fails due to duplicates, fall back to execute_values with ON CONFLICT
                        logger.warning(f"⚠️ COPY failed due to duplicates, falling back to execute_values: {copy_error}")
                        conn.rollback()
                        self._bulk_insert_with_execute_values_fallback(batch_data, column_order)
                    
        except Exception as e:
            logger.error(f"❌ Failed bulk insert: {e}")
            raise

    def _bulk_insert_with_execute_values_fallback(self, batch_data: List[Dict[str, Any]], column_order: List[str]) -> None:
        """Fallback method using execute_values with ON CONFLICT for duplicate handling"""
        import json
        
        # Build dynamic insert query with ON CONFLICT
        columns_str = ", ".join(column_order)
        insert_query = f"""
        INSERT INTO transaction_raw_rent ({columns_str}) 
        VALUES %s
        ON CONFLICT (loc_location_id, currency, measurement, date, price, size) 
        WHERE ((date IS NOT NULL) AND (price IS NOT NULL))
        DO NOTHING
        """
        
        # Prepare data for insertion
        values_list = []
        for item in batch_data:
            processed_item = {}
            for key, value in item.items():
                if isinstance(value, dict) and key != "raw_data":
                    processed_item[key] = json.dumps(value)
                elif key == "raw_data":
                    processed_item[key] = json.dumps(value) if isinstance(value, dict) else value
                else:
                    processed_item[key] = value
            
            # Create values tuple in the same order as column_order
            values = tuple(processed_item.get(col) for col in column_order)
            values_list.append(values)
        
        # Execute the insert with ON CONFLICT handling
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values_list)
                conn.commit()
                logger.info(f"✅ execute_values fallback successful for {len(batch_data)} records")

    def insert_transaction_sales_data(self, data_list: List[Dict[str, Any]]) -> None:
        """
        Insert transaction sales data into the database
        
        Args:
            data_list: List of transaction sales records to insert
        """
        if not data_list:
            logger.info("No transaction sales data to insert")
            return
        
        try:
            insert_query = """
                INSERT INTO transaction_sales (
                    start_date, end_date, date, transaction_version, transaction_type,
                    location_id, loc_city_id, loc_city_name, loc_county_id, loc_county_name,
                    loc_district_id, loc_district_name, loc_location_id, loc_location_name, loc_municipal_area,
                    property_id, property_name, property_type_name, property_subtype_name,
                    price, price_per_size, size, size_land_imp,
                    attr_unit, attr_floor, attr_parking, attr_land_number, attr_no_of_rooms,
                    attr_balcony_area, attr_building_name, attr_building_number,
                    currency, measurement, bedroom, size_range, price_range, transaction_date,
                    raw_data
                ) VALUES %s
                ON CONFLICT (location_id, start_date, end_date, price, transaction_type, date) DO NOTHING
            """
            
            # Prepare data in the correct order
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('start_date'),
                    record.get('end_date'),
                    record.get('date'),
                    record.get('transaction_version'),
                    record.get('transaction_type'),
                    record.get('location_id'),
                    record.get('loc_city_id'),
                    record.get('loc_city_name'),
                    record.get('loc_county_id'),
                    record.get('loc_county_name'),
                    record.get('loc_district_id'),
                    record.get('loc_district_name'),
                    record.get('loc_location_id'),
                    record.get('loc_location_name'),
                    record.get('loc_municipal_area'),
                    record.get('property_id'),
                    record.get('property_name'),
                    record.get('property_type_name'),
                    record.get('property_subtype_name'),
                    record.get('price'),
                    record.get('price_per_size'),
                    record.get('size'),
                    record.get('size_land_imp'),
                    record.get('attr_unit'),
                    record.get('attr_floor'),
                    record.get('attr_parking'),
                    record.get('attr_land_number'),
                    record.get('attr_no_of_rooms'),
                    record.get('attr_balcony_area'),
                    record.get('attr_building_name'),
                    record.get('attr_building_number'),
                    record.get('currency'),
                    record.get('measurement'),
                    record.get('bedroom'),
                    record.get('size_range'),
                    record.get('price_range'),
                    record.get('transaction_date'),
                    record.get('raw_data')
                ]
                ordered_data.append(ordered_record)
            
            # Use execute_values directly for better control
            with self.get_connection() as conn, conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, insert_query, ordered_data)
                conn.commit()
                logger.info(f"Successfully inserted {len(data_list)} transaction sales records")
        except Exception as e:
            logger.error(f"Failed to insert transaction sales data: {e}")
            raise

    def insert_cma_sales_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert CMA sales data into the database with proper schema and ON CONFLICT handling"""
        if not data_list:
            logger.info("No CMA sales data to insert")
            return
        
        try:
            import time
            logger.info(f"Starting batch insert of {len(data_list)} CMA sales records...")
            start_time = time.time()
            
            insert_query = """
                INSERT INTO cma_sales (
                    property_id, alias, currency, measurement, property_type, property_subtype,
                    comparable_property_id, comparable_property_name, size, price, price_per_size, transaction_date,
                    city_id, city_name, county_id, county_name, district_id, district_name,
                    location_id, location_name, municipal_area, activity_type, no_of_bedrooms,
                    number_of_unit, property_nature, number_of_floors, parent_property, raw_data
                ) VALUES %s
                ON CONFLICT (property_id, alias, currency, measurement, property_type, property_subtype, comparable_property_id)
                DO NOTHING
            """
            
            # Prepare ordered data for execute_values with proper JSON handling
            from psycopg2.extras import Json
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('property_id'),
                    record.get('alias'),
                    record.get('currency'),
                    record.get('measurement'),
                    record.get('property_type'),
                    record.get('property_subtype'),
                    record.get('comparable_property_id'),
                    record.get('comparable_property_name'),
                    record.get('size'),
                    record.get('price'),
                    record.get('price_per_size'),
                    record.get('transaction_date'),
                    record.get('city_id'),
                    record.get('city_name'),
                    record.get('county_id'),
                    record.get('county_name'),
                    record.get('district_id'),
                    record.get('district_name'),
                    record.get('location_id'),
                    record.get('location_name'),
                    record.get('municipal_area'),
                    record.get('activity_type'),
                    record.get('no_of_bedrooms'),
                    record.get('number_of_unit'),
                    record.get('property_nature'),
                    record.get('number_of_floors'),
                    Json(record.get('parent_property')) if record.get('parent_property') else None,
                    Json(record.get('raw_data')) if record.get('raw_data') else None
                ]
                ordered_data.append(ordered_record)
            
            # Use COPY for maximum performance with large batches
            if len(ordered_data) >= 5000:  # Use COPY for batches >=5k (always faster for bulk)
                self._bulk_insert_with_copy(ordered_data)
            else:  # Use execute_values for smaller batches
                conn = None
                try:
                    conn = self.get_connection()
                    with conn.cursor() as cur:
                        from psycopg2.extras import execute_values, Json
                        execute_values(cur, insert_query, ordered_data, page_size=1000)
                        conn.commit()
                finally:
                    if conn:
                        self.return_connection(conn)
                
                end_time = time.time()
                duration = end_time - start_time
                logger.info(f"✅ Successfully inserted {len(data_list)} CMA sales records in {duration:.2f} seconds")
        except Exception as e:
            logger.error(f"Failed to insert CMA sales data: {e}")
            raise

    def _bulk_insert_with_copy(self, ordered_data):
        """Ultra-fast bulk insert using PostgreSQL COPY for large batches"""
        import io
        import json
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                # Create a StringIO buffer for COPY
                buffer = io.StringIO()
                
                for row in ordered_data:
                    # Convert row to tab-separated values
                    # Handle None values and JSON fields
                    formatted_row = []
                    for i, value in enumerate(row):
                        if value is None:
                            formatted_row.append('\\N')  # PostgreSQL NULL representation
                        elif i in [26, 27]:  # parent_property and raw_data columns (JSON)
                            # Convert JSON objects to strings
                            if isinstance(value, dict):
                                formatted_row.append(json.dumps(value).replace('\t', ' ').replace('\n', ' '))
                            else:
                                formatted_row.append(str(value).replace('\t', ' ').replace('\n', ' '))
                        else:
                            formatted_row.append(str(value).replace('\t', ' ').replace('\n', ' '))
                    
                    buffer.write('\t'.join(formatted_row) + '\n')
                
                buffer.seek(0)
                
                # Use COPY for ultra-fast bulk insert
                cur.copy_from(
                    buffer,
                    'cma_sales',
                    columns=[
                        'property_id', 'alias', 'currency', 'measurement', 'property_type', 'property_subtype',
                        'comparable_property_id', 'comparable_property_name', 'size', 'price', 'price_per_size',
                        'transaction_date', 'city_id', 'city_name', 'county_id', 'county_name', 'district_id',
                        'district_name', 'location_id', 'location_name', 'municipal_area', 'activity_type',
                        'no_of_bedrooms', 'number_of_unit', 'property_nature', 'number_of_floors',
                        'parent_property', 'raw_data'
                    ],
                    sep='\t',
                    null='\\N'
                )
                conn.commit()
                
                logger.info(f"🚀 Ultra-fast COPY insert completed for {len(ordered_data)} records")
                
        except Exception as e:
            logger.error(f"❌ Failed COPY bulk insert: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def insert_transactions_price_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert transactions price data into the database"""
        if not data_list:
            logger.info("No transactions price data to insert")
            return
        
        try:
            insert_query = """
                INSERT INTO transactions_price (
                    location_id, property_type, activity_type, property_id, property_sub_type, no_of_bedrooms,
                    term, value, additional_data, raw_data
                ) VALUES %s
                ON CONFLICT (location_id, property_type, activity_type, property_id, property_sub_type, no_of_bedrooms, term)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    additional_data = EXCLUDED.additional_data,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare ordered data for execute_values
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('location_id'),
                    record.get('property_type'),
                    record.get('activity_type'),
                    record.get('property_id'),
                    record.get('property_sub_type'),
                    record.get('no_of_bedrooms'),
                    record.get('term'),
                    record.get('value'),
                    record.get('additional_data'),
                    record.get('raw_data')
                ]
                ordered_data.append(ordered_record)
            
            # Use execute_values directly for better control
            with self.get_connection() as conn, conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, insert_query, ordered_data)
                conn.commit()
                logger.info(f"Successfully inserted {len(data_list)} transactions price records")
        except Exception as e:
            logger.error(f"Failed to insert transactions price data: {e}")
            raise

    def insert_transaction_list_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert transaction list data into the database"""
        if not data_list:
            logger.info("No transaction list data to insert")
            return
        
        try:
            insert_query = """
                INSERT INTO transaction_list (
                    transaction_id, date_transaction, price, price_per_size, size, size_land,
                    location_id, loc_city_id, loc_city_name, loc_county_id, loc_county_name,
                    loc_district_id, loc_district_name, loc_location_id, loc_location_name, loc_municipal_area,
                    property_id, property_name, property_type_name, property_subtype_name, municipal_property_type,
                    attr_unit, attr_floor, attr_parking, attr_land_number, attr_no_of_rooms, attr_balcony_area,
                    attr_building_name, attr_building_number, query_property_type, query_activity_type,
                    query_currency, query_measurement, raw_data
                ) VALUES %s
                ON CONFLICT (location_id, query_property_type, query_activity_type, query_currency, query_measurement, transaction_id)
                DO UPDATE SET
                    date_transaction = EXCLUDED.date_transaction,
                    price = EXCLUDED.price,
                    price_per_size = EXCLUDED.price_per_size,
                    size = EXCLUDED.size,
                    size_land = EXCLUDED.size_land,
                    loc_city_id = EXCLUDED.loc_city_id,
                    loc_city_name = EXCLUDED.loc_city_name,
                    loc_county_id = EXCLUDED.loc_county_id,
                    loc_county_name = EXCLUDED.loc_county_name,
                    loc_district_id = EXCLUDED.loc_district_id,
                    loc_district_name = EXCLUDED.loc_district_name,
                    loc_location_id = EXCLUDED.loc_location_id,
                    loc_location_name = EXCLUDED.loc_location_name,
                    loc_municipal_area = EXCLUDED.loc_municipal_area,
                    property_id = EXCLUDED.property_id,
                    property_name = EXCLUDED.property_name,
                    property_type_name = EXCLUDED.property_type_name,
                    property_subtype_name = EXCLUDED.property_subtype_name,
                    municipal_property_type = EXCLUDED.municipal_property_type,
                    attr_unit = EXCLUDED.attr_unit,
                    attr_floor = EXCLUDED.attr_floor,
                    attr_parking = EXCLUDED.attr_parking,
                    attr_land_number = EXCLUDED.attr_land_number,
                    attr_no_of_rooms = EXCLUDED.attr_no_of_rooms,
                    attr_balcony_area = EXCLUDED.attr_balcony_area,
                    attr_building_name = EXCLUDED.attr_building_name,
                    attr_building_number = EXCLUDED.attr_building_number,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare ordered data for execute_values
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('transaction_id'),
                    record.get('date_transaction'),
                    record.get('price'),
                    record.get('price_per_size'),
                    record.get('size'),
                    record.get('size_land'),
                    record.get('location_id'),
                    record.get('loc_city_id'),
                    record.get('loc_city_name'),
                    record.get('loc_county_id'),
                    record.get('loc_county_name'),
                    record.get('loc_district_id'),
                    record.get('loc_district_name'),
                    record.get('loc_location_id'),
                    record.get('loc_location_name'),
                    record.get('loc_municipal_area'),
                    record.get('property_id'),
                    record.get('property_name'),
                    record.get('property_type_name'),
                    record.get('property_subtype_name'),
                    record.get('municipal_property_type'),
                    record.get('attr_unit'),
                    record.get('attr_floor'),
                    record.get('attr_parking'),
                    record.get('attr_land_number'),
                    record.get('attr_no_of_rooms'),
                    record.get('attr_balcony_area'),
                    record.get('attr_building_name'),
                    record.get('attr_building_number'),
                    record.get('query_property_type'),
                    record.get('query_activity_type'),
                    record.get('query_currency'),
                    record.get('query_measurement'),
                    record.get('raw_data')
                ]
                ordered_data.append(ordered_record)
            
            # Use execute_values directly for better control
            with self.get_connection() as conn, conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, insert_query, ordered_data)
                conn.commit()
                logger.info(f"Successfully inserted {len(data_list)} transaction list records")
        except Exception as e:
            logger.error(f"Failed to insert transaction list data: {e}")
            raise

    # def create_transaction_list_table(self) -> None:
    #     """Create the transaction_list table if it doesn't exist"""
    #     create_table_query = """
    #     CREATE TABLE IF NOT EXISTS transaction_list (
    #         id SERIAL PRIMARY KEY,
    #         transaction_id VARCHAR(255) NOT NULL,
    #         date_transaction VARCHAR(50),
    #         price DECIMAL(18,2),
    #         price_per_size DECIMAL(18,2),
    #         size DECIMAL(18,2),
    #         size_land DECIMAL(18,2),
    #         location_id INTEGER NOT NULL,
    #         loc_city_id INTEGER,
    #         loc_city_name VARCHAR(255),
    #         loc_county_id INTEGER,
    #         loc_county_name VARCHAR(255),
    #         loc_district_id INTEGER,
    #         loc_district_name VARCHAR(255),
    #         loc_location_id INTEGER,
    #         loc_location_name VARCHAR(255),
    #         loc_municipal_area VARCHAR(255),
    #         property_id INTEGER,
    #         property_name VARCHAR(255),
    #         property_type_name VARCHAR(255),
    #         property_subtype_name VARCHAR(255),
    #         municipal_property_type VARCHAR(255),
    #         attr_unit VARCHAR(50),
    #         attr_floor VARCHAR(50),
    #         attr_parking VARCHAR(50),
    #         attr_land_number VARCHAR(50),
    #         attr_no_of_rooms INTEGER,
    #         attr_balcony_area DECIMAL(18,2),
    #         attr_building_name VARCHAR(255),
    #         attr_building_number VARCHAR(50),
    #         query_property_type VARCHAR(50) NOT NULL,
    #         query_activity_type VARCHAR(50) NOT NULL,
    #         query_currency VARCHAR(10) NOT NULL,
    #         query_measurement VARCHAR(10) NOT NULL,
    #         raw_data TEXT,
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #         UNIQUE(location_id, query_property_type, query_activity_type, query_currency, query_measurement, transaction_id)
    #     );
    #     """
        
    #     try:
    #         with self.get_connection() as conn, conn.cursor() as cur:
    #             cur.execute(create_table_query)
    #             conn.commit()
    #             logger.info("Transaction list table created successfully")
    #     except Exception as e:
    #         logger.error(f"Failed to create transaction list table: {e}")
    #         raise

    def get_transaction_raw_rent_count(self) -> int:
        """Get the total count of records in transaction_raw_rent table"""
        try:
            with self.get_connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM transaction_raw_rent")
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get transaction raw rent count: {e}")
            return 0

    def get_duplicate_count(self) -> int:
        """Get the count of duplicate records in transaction_raw_rent table"""
        try:
            with self.get_connection() as conn, conn.cursor() as cur:
                # Count records that have duplicates based on the unique constraint
                cur.execute("""
                    SELECT COUNT(*) - COUNT(DISTINCT (location_id, currency, measurement, date, price, size))
                    FROM transaction_raw_rent 
                    WHERE date IS NOT NULL AND price IS NOT NULL
                """)
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get duplicate count: {e}")
            return 0

    def insert_poi_cma_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert POI CMA data into the database with proper schema and ON CONFLICT handling"""
        if not data_list:
            logger.info("No POI CMA data to insert")
            return
        
        try:
            import time
            logger.info(f"Starting batch insert of {len(data_list)} POI CMA records...")
            start_time = time.time()
            
            insert_query = """
                INSERT INTO poi_cma (
                    property_id, measurement, lang, lat, lon,
                    poi_subtype_name, poi_type_name, poi_name, poi_review, poi_rating, poi_distance,
                    raw_data
                ) VALUES %s
                ON CONFLICT (property_id, measurement, lang, poi_name, poi_type_name, poi_subtype_name, lat, lon)
                DO UPDATE SET
                    poi_review = EXCLUDED.poi_review,
                    poi_rating = EXCLUDED.poi_rating,
                    poi_distance = EXCLUDED.poi_distance,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare ordered data for execute_values with proper JSON handling
            from psycopg2.extras import Json
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('property_id'),
                    record.get('measurement'),
                    record.get('lang'),
                    record.get('lat'),
                    record.get('lon'),
                    record.get('poi_subtype_name'),
                    record.get('poi_type_name'),
                    record.get('poi_name'),
                    record.get('poi_review'),
                    record.get('poi_rating'),
                    record.get('poi_distance'),
                    Json(record.get('raw_data')) if record.get('raw_data') else None
                ]
                ordered_data.append(ordered_record)
            
            # Use execute_values for insertion
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cur:
                    from psycopg2.extras import execute_values, Json
                    execute_values(cur, insert_query, ordered_data, page_size=1000)
                    conn.commit()
            finally:
                if conn:
                    self.return_connection(conn)
            
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"✅ Successfully inserted {len(data_list)} POI CMA records in {duration:.2f} seconds")
        except Exception as e:
            logger.error(f"Failed to insert POI CMA data: {e}")
            raise

    def insert_cma2_rents_data(self, data_list: List[Dict[str, Any]]) -> None:
        """Insert CMA2-rents data into the database with proper schema and ON CONFLICT handling"""
        if not data_list:
            logger.info("No CMA2-rents data to insert")
            return
        
        try:
            import time
            logger.info(f"Starting batch insert of {len(data_list)} CMA2-rents records...")
            start_time = time.time()
            
            insert_query = """
                INSERT INTO cma2_rents (
                    property_id, country_code, alias, currency, measurement, property_type, property_subtype,
                    city_id, city_name, county_id, county_name, district_id, location_id,
                    district_name, location_name, municipal_area,
                    transaction_count, average_rent, median_rent, min_rent, max_rent, rent_per_sqm,
                    raw_data
                ) VALUES %s
                ON CONFLICT (property_id, country_code, alias, currency, measurement, property_type, property_subtype, 
                            city_id, county_id, district_id, location_id)
                DO UPDATE SET
                    city_name = EXCLUDED.city_name,
                    county_name = EXCLUDED.county_name,
                    district_name = EXCLUDED.district_name,
                    location_name = EXCLUDED.location_name,
                    municipal_area = EXCLUDED.municipal_area,
                    transaction_count = EXCLUDED.transaction_count,
                    average_rent = EXCLUDED.average_rent,
                    median_rent = EXCLUDED.median_rent,
                    min_rent = EXCLUDED.min_rent,
                    max_rent = EXCLUDED.max_rent,
                    rent_per_sqm = EXCLUDED.rent_per_sqm,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare ordered data for execute_values with proper JSON handling
            from psycopg2.extras import Json
            ordered_data = []
            for record in data_list:
                ordered_record = [
                    record.get('property_id'),
                    record.get('country_code'),
                    record.get('alias'),
                    record.get('currency'),
                    record.get('measurement'),
                    record.get('property_type'),
                    record.get('property_subtype'),
                    record.get('city_id'),
                    record.get('city_name'),
                    record.get('county_id'),
                    record.get('county_name'),
                    record.get('district_id'),
                    record.get('location_id'),
                    record.get('district_name'),
                    record.get('location_name'),
                    record.get('municipal_area'),
                    record.get('transaction_count'),
                    record.get('average_rent'),
                    record.get('median_rent'),
                    record.get('min_rent'),
                    record.get('max_rent'),
                    record.get('rent_per_sqm'),
                    Json(record.get('raw_data')) if record.get('raw_data') else None
                ]
                ordered_data.append(ordered_record)
            
            # Use execute_values for insertion
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cur:
                    from psycopg2.extras import execute_values, Json
                    execute_values(cur, insert_query, ordered_data, page_size=1000)
                    conn.commit()
            finally:
                if conn:
                    self.return_connection(conn)
            
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"✅ Successfully inserted {len(data_list)} CMA2-rents records in {duration:.2f} seconds")
        except Exception as e:
            logger.error(f"Failed to insert CMA2-rents data: {e}")
            raise

    # def create_transaction_raw_rent_table(self) -> None:
    #     """Create the transaction_raw_rent table if it doesn't exist"""
    #     create_table_query = """
    #     CREATE TABLE IF NOT EXISTS transaction_raw_rent (
    #         id SERIAL PRIMARY KEY,
    #         location_id INTEGER,
    #         currency VARCHAR(10),
    #         measurement VARCHAR(10),
    #         raw_data JSONB,
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #     );
    #     """
        
    #     try:
    #         with self.get_connection() as conn, conn.cursor() as cur:
    #             cur.execute(create_table_query)
    #             conn.commit()
    #             logger.info("Transaction raw rent table created/verified successfully")
    #     except Exception as e:
    #         logger.error(f"Failed to create transaction raw rent table: {e}")
    #         raise

    def insert_cma_data_dynamic(self, data_list: List[Dict[str, Any]], discovered_columns: Set[str]) -> None:
        """Insert CMA data with dynamic schema handling"""
        if not data_list:
            return
            
        try:
            start_time = time.time()
            logger.info(f"Starting batch insert of {len(data_list)} CMA records with dynamic schema...")
            
            # Create table if it doesn't exist with dynamic columns
            self._create_cma_table_dynamic(discovered_columns, data_list)
            
            # Prepare data for insertion
            ordered_data = []
            for record in data_list:
                # Ensure all discovered columns are present in the record
                ordered_record = []
                for column in discovered_columns:
                    value = record.get(column)
                    if value is None:
                        ordered_record.append(None)
                    elif isinstance(value, (dict, list)):
                        ordered_record.append(Json(value))
                    else:
                        ordered_record.append(value)
                ordered_data.append(tuple(ordered_record))
            
            # Build dynamic insert query
            columns_str = ', '.join([f'"{col}"' for col in discovered_columns])
            placeholders = ', '.join(['%s'] * len(discovered_columns))
            insert_query = f"""
                INSERT INTO cma_data ({columns_str})
                VALUES %s
                ON CONFLICT (property_id, alias, currency, measurement, property_type, activity_type) 
                DO UPDATE SET
                    {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in discovered_columns if col not in ['property_id', 'alias', 'currency', 'measurement', 'property_type', 'activity_type']])},
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Use execute_values for insertion
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cur:
                    from psycopg2.extras import execute_values
                    execute_values(cur, insert_query, ordered_data, page_size=1000)
                    conn.commit()
            finally:
                if conn:
                    self.return_connection(conn)
            
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"✅ Successfully inserted {len(data_list)} CMA records in {duration:.2f} seconds")
        except Exception as e:
            logger.error(f"Failed to insert CMA data: {e}")
            raise

    def _create_cma_table_dynamic(self, discovered_columns: Set[str], sample_data: List[Dict[str, Any]] = None) -> None:
        """Create CMA table with dynamic columns and proper data types"""
        try:
            # Start with base columns
            base_columns = [
                "id SERIAL PRIMARY KEY",
                "property_id INTEGER NOT NULL",
                "alias VARCHAR(50) NOT NULL",
                "currency VARCHAR(10) NOT NULL", 
                "measurement VARCHAR(10) NOT NULL",
                "property_type VARCHAR(50) NOT NULL",
                "activity_type VARCHAR(50) NOT NULL",
                "raw_data JSONB",
                "processed_at TIMESTAMP",
                "import_batch_id BIGINT",
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ]
            
            # Add discovered columns with proper data types
            for column in discovered_columns:
                if column not in ['property_id', 'alias', 'currency', 'measurement', 'property_type', 'activity_type', 'raw_data', 'processed_at', 'import_batch_id']:
                    # Determine column type based on sample data
                    column_type = self._determine_column_type(column, sample_data)
                    base_columns.append(f'"{column}" {column_type}')
            
            # Create table
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS cma_data (
                    {', '.join(base_columns)},
                    UNIQUE(property_id, alias, currency, measurement, property_type, activity_type)
                );
            """
            
            # Create indexes
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_cma_data_property_id ON cma_data(property_id);",
                "CREATE INDEX IF NOT EXISTS idx_cma_data_created_at ON cma_data(created_at);",
                "CREATE INDEX IF NOT EXISTS idx_cma_data_batch_id ON cma_data(import_batch_id);"
            ]
            
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cur:
                    cur.execute(create_table_query)
                    for index_query in index_queries:
                        cur.execute(index_query)
                    conn.commit()
                    logger.info("CMA data table created/verified successfully with dynamic schema")
            finally:
                if conn:
                    self.return_connection(conn)
                    
        except Exception as e:
            logger.error(f"Failed to create CMA data table: {e}")
            raise

    def _determine_column_type(self, column_name: str, sample_data: List[Dict[str, Any]] = None) -> str:
        """Determine the appropriate PostgreSQL data type for a column based on sample data"""
        if not sample_data:
            return "TEXT"  # Default fallback
        
        # Analyze sample data for this column
        values = []
        for record in sample_data:
            if column_name in record and record[column_name] is not None:
                values.append(record[column_name])
        
        if not values:
            return "TEXT"
        
        # Check if all values are integers
        try:
            for value in values:
                if isinstance(value, str) and not value.isdigit():
                    break
                int(value)
            return "INTEGER"
        except (ValueError, TypeError):
            pass
        
        # Check if all values are floats
        try:
            for value in values:
                float(value)
            return "DECIMAL(15,2)"  # For monetary values
        except (ValueError, TypeError):
            pass
        
        # Check if all values are booleans
        if all(isinstance(v, bool) for v in values):
            return "BOOLEAN"
        
        # Check if all values are dates/timestamps
        if all(isinstance(v, str) and ('T' in v or '-' in v) for v in values):
            return "TIMESTAMP"
        
        # Check for common numeric patterns
        if all(isinstance(v, (int, float)) for v in values):
            return "DECIMAL(15,2)"
        
        # Check for JSON objects/arrays
        if any(isinstance(v, (dict, list)) for v in values):
            return "JSONB"
        
        # Check string length to determine VARCHAR size
        max_length = max(len(str(v)) for v in values if v is not None)
        if max_length <= 50:
            return "VARCHAR(50)"
        elif max_length <= 255:
            return "VARCHAR(255)"
        elif max_length <= 1000:
            return "VARCHAR(1000)"
        else:
            return "TEXT"
