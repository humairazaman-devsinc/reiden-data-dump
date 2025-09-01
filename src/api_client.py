import requests
import logging
import time
from typing import Dict, List, Optional
from src.config import Config

logger = logging.getLogger(__name__)


class ReidinAPIClient:
    def __init__(self):
        self.base_url = Config.API_BASE_URL
        self.token = Config.API_TOKEN
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': self.token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

    def fetch_data_from_endpoint(self, endpoint: str, params: Dict) -> Dict:
        """
        Generic method to fetch data from a given API endpoint

        Args:
            endpoint: API endpoint (e.g., '/indicators/ae')
            params: Dictionary of query parameters

        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}{endpoint}"

        try:
            logger.info(f"Fetching data from: {url}")
            logger.info(f"Parameters: {params}")
            logger.info(f"Headers: {self.session.headers}")

            response = self.session.get(
                url, params=params, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()
            logger.info(
                f"Successfully fetched data. Status: {data.get('status')}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def fetch_bulk_data(self, endpoint: str, params: Dict, result_key: str = "results", hard_limit: Optional[int] = None) -> List[Dict]:
        """
        Fetch all data from an endpoint with pagination

        Args:
            endpoint: API endpoint
            params: Dictionary of query parameters
            result_key: Key in the JSON response where the data list is located
            hard_limit: Maximum number of records to fetch (None for no limit)

        Returns:
            List of all data records
        """
        all_data = []
        offset = 0
        limit = params.get('page_size', Config.BATCH_SIZE)

        while True:
            try:
                params.update({'page_size': limit})
                response = self.fetch_data_from_endpoint(endpoint, params)

                if response.get('status') != 'OK':
                    logger.error(
                        "API returned error status: %s", response.get('status'))
                    break

                result = response.get('result', {})
                data = result.get(result_key, [])

                if not data:
                    logger.info("No more data to fetch")
                    break

                all_data.extend(data)
                logger.info(
                    "Fetched %i records. Total: %i", len(data), len(all_data))

                # If we got fewer records than requested, we've reached the end
                if len(data) < limit or (hard_limit and len(all_data) >= hard_limit):
                    break

                offset = response.get('scroll_id', None)
                if offset is not None:
                    params['scroll_id'] = offset
                else:
                    break

                # Add a small delay to be respectful to the API
                time.sleep(0.1)

            except Exception as e:
                logger.error("Error in pagination loop: %s", e)
                break

        logger.info("Total records fetched: %i", len(all_data))
        return all_data

    def get_locations_data(self, country_code: str,
                           limit: int = 100, offset: int | None = None) -> List[Dict]:
        """
        Fetch all locations data from Reidin API

        Args:
            country_code: Country code (e.g., 'ae', 'tr')
            limit: Number of results to return
            offset: Number of results to skip

        Returns:
            API response as dictionary
        """

        endpoint = Config.LOCATIONS_ENDPOINT.format(country_code=country_code)

        params = {
            'page_size': limit,
            'scroll_id': offset
        }
        return self.fetch_bulk_data(endpoint, params, "results")

    def get_indicators_data(self, country_code: str, groups_id: int) -> List[Dict]:
        """
        Fetch all indicators data with pagination

        Args:
            country_code: Country code
            groups_id: Optional group ID filter
            limit: Number of records to fetch per request
            offset: Number of records to skip

        Returns:
            List of all indicator data
        """
        endpoint = Config.INDICATORS_ENDPOINT.format(country_code=country_code)
        params = {
            'groups_id': groups_id,
        }
        data = self.fetch_data_from_endpoint(endpoint, params)
        if data.get('status') != 'OK':
            logger.error(
                "API returned error status: %s", data.get('status'))
            return []
        return data.get('result', {})

    def close(self):
        """Close the session"""
        self.session.close()
