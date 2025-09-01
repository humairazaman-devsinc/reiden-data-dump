import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
from src.config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Class to manage PostgreSQL database interactions"""

    def __init__(self):
        self.connection = self._connect()

    def _connect(self):
        """Establish database connection"""
        try:
            connection = psycopg2.connect(Config.DATABASE_URL)
            logger.info("Successfully connected to PostgreSQL database")
            return connection
        except Exception as e:
            logger.error("Error connecting to database: %s", e)
            raise

    def get_cursor(self):
        """Get a cursor with RealDictCursor for easier data handling"""
        if not self.connection or self.connection.closed:
            self._connect()
        return self.connection.cursor(cursor_factory=RealDictCursor)

    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        cursor = self.get_cursor()
        try:
            cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                self.connection.commit()
                return cursor.rowcount
        except psycopg2.Error as e:  # More specific exception
            self.connection.rollback()
            logger.error("Error executing query: %s", e)
            raise
        finally:
            cursor.close()

    def insert_locations(self, locations_data):
        """Insert locations data in batch"""
        query = """
        INSERT INTO locations (
            location_id, location_name, country_code, city_id, city_name,
            county_id, county_name, district_id, district_name,
            latitude, longitude
        ) VALUES (
            %(location_id)s, %(location_name)s, %(country_code)s, %(city_id)s,
            %(city_name)s, %(county_id)s, %(county_name)s, %(district_id)s,
            %(district_name)s, %(latitude)s, %(longitude)s
        )
        ON CONFLICT (location_id) DO UPDATE SET
            location_name = EXCLUDED.location_name,
            city_name = EXCLUDED.city_name,
            county_name = EXCLUDED.county_name,
            district_name = EXCLUDED.district_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
        """
        cursor = self.get_cursor()
        try:
            processed_data = []
            for loc in locations_data:
                processed_data.append(loc)

            cursor.executemany(query, processed_data)
            self.connection.commit()
            logger.info(
                "Successfully inserted/updated %i location records", len(processed_data))

        except Exception as e:
            self.connection.rollback()
            logger.error("Error inserting location data: %s", e)
            raise
        finally:
            cursor.close()

# Indicators should be gotten from other repo
    def insert_indicator_data(self, data_list):
        """Insert indicator data in batch"""
        query = """
        INSERT INTO indicator_data (
            indicator_id, location_id, location_name, location_level,
            location_lat, location_lon, indicator_name, indicator_group_id,
            indicator_group_name, unit_name, area_wide_status, city_wide_status,
            country_wide_status, data_frequency, update_frequency, import_date, import_type
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id
        """
        cursor = self.get_cursor()
        try:
            for data in data_list:
                cursor.execute(query, (
                    data['indicator_id'],
                    data['location_id'],
                    data['location_name'],
                    data['location_level'],
                    data['location_lat'],
                    data['location_lon'],
                    data['indicator_name'],
                    data['indicator_group_id'],
                    data['indicator_group_name'],
                    data['unit_name'],
                    data['area_wide_status'],
                    data['city_wide_status'],
                    data['country_wide_status'],
                    data['data_frequency'],
                    data['update_frequency'],
                    data['import_date'],
                    data['import_type']
                ))
                row = cursor.fetchone()
                if row is None:
                    logger.warning(
                        "No ID returned for inserted indicator_data")
                    continue
                indicator_data_id = row['id']

                # Insert time series data if available
                if 'time_series' in data:
                    self.insert_time_series_data(
                        indicator_data_id, data['time_series'])

            self.connection.commit()
            logger.info(
                "Successfully inserted %i indicator records", len(data_list))
        except Exception as e:
            self.connection.rollback()
            logger.error("Error inserting indicator data: %s", e)
            raise
        finally:
            cursor.close()

    def insert_time_series_data(self, indicator_data_id, time_series_list):
        """Insert time series data"""
        query = """
        INSERT INTO time_series_data (
            indicator_data_id, date, value_aed_imp, value_aed_int,
            value_eur_imp, value_eur_int, value_try_imp, value_try_int,
            value_usd_imp, value_usd_int, value_text, differance_aed,
            differance_eur, differance_try, differance_usd, date_year,
            date_month, date_day, date_quarter, date_semi_annually
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        cursor = self.get_cursor()
        try:
            for ts_data in time_series_list:
                cursor.execute(query, (
                    indicator_data_id,
                    ts_data['date'],
                    ts_data['value_aed_imp'],
                    ts_data['value_aed_int'],
                    ts_data['value_eur_imp'],
                    ts_data['value_eur_int'],
                    ts_data['value_try_imp'],
                    ts_data['value_try_int'],
                    ts_data['value_usd_imp'],
                    ts_data['value_usd_int'],
                    ts_data['value_text'],
                    ts_data['differance_aed'],
                    ts_data['differance_eur'],
                    ts_data['differance_try'],
                    ts_data['differance_usd'],
                    ts_data['date_year'],
                    ts_data['date_month'],
                    ts_data['date_day'],
                    ts_data['date_quarter'],
                    ts_data['date_semi_annually']
                ))

            self.connection.commit()
            logger.info(
                "Successfully inserted %i time series records", len(time_series_list))
        except Exception as e:
            self.connection.rollback()
            logger.error("Error inserting time series data: %s", e)
            raise
        finally:
            cursor.close()

    def insert_location_hierarchy(self, locations, country_code):
        """Insert location hierarchy data"""
        query = """
        INSERT INTO location_hierarchy (
            location_id, location_name, level_id, level_name, parent_id, country_code
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id) DO NOTHING
        """
        cursor = self.get_cursor()
        try:
            for location in locations:
                cursor.execute(query, (
                    location['id'],
                    location['name'],
                    location['level_id'],
                    location['level_name'],
                    location.get('parent_id'),
                    country_code
                ))

            self.connection.commit()
            logger.info(
                "Successfully inserted %i location hierarchy records", len(locations))
        except Exception as e:
            self.connection.rollback()
            logger.error("Error inserting location hierarchy: %s", e)
            raise
        finally:
            cursor.close()

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
