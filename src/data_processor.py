import logging
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DataProcessor:
    """Class to process raw data from Reidin API into database-ready format"""

    @staticmethod
    def process_location_data(raw_data: List[Dict]) -> List[Dict]:
        """
        Process raw location data from API into database-ready format

        Args:
            raw_data: List of raw location data from API

        Returns:
            List of processed location data
        """
        processed_data = []
        required_keys = [
            'country_code', 'city_id', 'city_name', 'county_id', 'county_name',
            'district_id', 'district_name', 'location_id', 'location_name',
        ]

        for item in raw_data:
            geo_point = item.get('geo_point', {})
            try:
                processed_item = {k: item.get(k) for k in required_keys}
                processed_item['latitude'] = geo_point.get('lat')
                processed_item['longitude'] = geo_point.get('lon')
                processed_data.append(processed_item)

                processed_data.append(processed_item)

            except Exception as e:
                logger.error("Error processing location data: %s", e)
                continue

        logger.info("Processed %i location records", len(processed_data))
        return processed_data

    @staticmethod
    def process_indicator_data(raw_data: List[Dict]) -> List[Dict]:
        """
        Process raw indicator data from API into database-ready format

        Args:
            raw_data: List of raw indicator data from API

        Returns:
            List of processed indicator data
        """
        processed_data = []

        for item in raw_data:
            try:
                source = item.get('_source', {})

                # Extract location data
                location = source.get('location', {})
                locations = source.get('locations', [])

                # Extract indicator data
                indicator = source.get('indicator', {})
                indicator_groups = source.get('indicator_groups', [])

                # Extract last value data
                last_value = source.get('last_value', {})

                # Process time series data if available
                time_series = []
                if 'timepoints' in item:
                    time_series = DataProcessor._process_time_series(
                        item['timepoints'])

                processed_item = {
                    'indicator_id': indicator.get('id'),
                    'location_id': location.get('id'),
                    'location_name': location.get('name'),
                    'location_level': location.get('level_name'),
                    'location_lat': location.get('lat'),
                    'location_lon': location.get('lon'),
                    'indicator_name': indicator.get('name'),
                    'indicator_group_id': indicator.get('group_id'),
                    'indicator_group_name': indicator_groups[0].get('name') if indicator_groups else None,
                    'unit_name': source.get('unit', {}).get('name'),
                    'area_wide_status': source.get('area_wide_status'),
                    'city_wide_status': source.get('city_wide_status'),
                    'country_wide_status': source.get('country_wide_status'),
                    'data_frequency': source.get('data_frequency', {}).get('name'),
                    'update_frequency': source.get('update_frequency', {}).get('name'),
                    'import_date': DataProcessor._parse_datetime(source.get('import_date')),
                    'import_type': source.get('import_type'),
                    'time_series': time_series,
                    'locations': locations
                }

                processed_data.append(processed_item)

            except Exception as e:
                logger.error(f"Error processing indicator data: {e}")
                continue

        logger.info(f"Processed {len(processed_data)} indicator records")
        return processed_data

    @staticmethod
    def _process_time_series(timepoints: List[Dict]) -> List[Dict]:
        """
        Process time series data

        Args:
            timepoints: List of timepoint data

        Returns:
            List of processed time series data
        """
        processed_time_series = []

        for tp in timepoints:
            try:
                source = tp.get('_source', {})

                processed_ts = {
                    'date': DataProcessor._parse_date(source.get('date')),
                    'value_aed_imp': source.get('value_aed_imp'),
                    'value_aed_int': source.get('value_aed_int'),
                    'value_eur_imp': source.get('value_eur_imp'),
                    'value_eur_int': source.get('value_eur_int'),
                    'value_try_imp': source.get('value_try_imp'),
                    'value_try_int': source.get('value_try_int'),
                    'value_usd_imp': source.get('value_usd_imp'),
                    'value_usd_int': source.get('value_usd_int'),
                    'value_text': source.get('value_text'),
                    'differance_aed': source.get('differance_aed'),
                    'differance_eur': source.get('differance_eur'),
                    'differance_try': source.get('differance_try'),
                    'differance_usd': source.get('differance_usd'),
                    'date_year': source.get('date_year'),
                    'date_month': source.get('date_month'),
                    'date_day': source.get('date_day'),
                    'date_quarter': source.get('date_quarter'),
                    'date_semi_annually': source.get('date_semi_annually')
                }

                processed_time_series.append(processed_ts)

            except Exception as e:
                logger.error(f"Error processing time series data: {e}")
                continue

        return processed_time_series

    @staticmethod
    def _parse_datetime(date_string: str) -> datetime | None:
        """
        Parse datetime string to datetime object

        Args:
            date_string: ISO format datetime string

        Returns:
            datetime object
        """
        if not date_string:
            return None

        try:
            # Remove 'Z' suffix and parse
            if date_string.endswith('Z'):
                date_string = date_string[:-1]
            return datetime.fromisoformat(date_string)
        except Exception as e:
            logger.error(f"Error parsing datetime {date_string}: {e}")
            return None

    @staticmethod
    def _parse_date(date_string: str) -> str | None:
        """
        Parse date string to YYYY-MM-DD format

        Args:
            date_string: Date string

        Returns:
            Date in YYYY-MM-DD format
        """
        if not date_string:
            return None

        try:
            # If it's already in YYYY-MM-DD format, return as is
            if len(date_string) == 10 and date_string.count('-') == 2:
                return date_string

            # Parse and format
            dt = datetime.fromisoformat(date_string)
            return dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"Error parsing date {date_string}: {e}")
            return None

    @staticmethod
    def extract_location_hierarchy(processed_data: List[Dict]) -> List[Dict]:
        """
        Extract unique location hierarchy from processed data

        Args:
            processed_data: List of processed indicator data

        Returns:
            List of unique location hierarchy records
        """
        unique_locations = {}

        for item in processed_data:
            locations = item.get('locations', [])

            for location in locations:
                location_id = location.get('id')
                if location_id not in unique_locations:
                    unique_locations[location_id] = {
                        'id': location.get('id'),
                        'name': location.get('name'),
                        'level_id': location.get('level_id'),
                        'level_name': location.get('level_name'),
                        'parent_id': location.get('parent_id')
                    }

        return list(unique_locations.values())
