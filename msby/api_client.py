import logging
from typing import Dict, Any, List
from utils.http_client import get_http_session
from config import Config

logger = logging.getLogger(__name__)


class ReidinAPIClient:
    def __init__(self):
        self.session = get_http_session()
        self.base_url = Config.REIDIN_BASE_URL

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def _get(
        self, endpoint: str, params: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"Fetching data from {url} with params {params}")
        try:
            response = self.session.get(url, params=params, timeout=Config.TIMEOUT)
            response.raise_for_status()
            logger.info(f"API response: {response}")
            data = response.json()
            return data
        except Exception as e:
            # if code is 400, log as warning
            if hasattr(response, "status_code") and response.status_code == 400:
                logger.warning(f"Data not found: {e}")
                raise
            logger.error(f"API request failed: {e}")
            raise

    def get_locations(
        self, country_code: str, limit: int | None = None, offset: int | None = None
    ) -> Dict[str, Any]:
        """Get locations data from Reidin API"""
        endpoint = Config.LOCATIONS_ENDPOINT.format(country_code=country_code)
        if limit is None and offset is not None:
            limit = Config.BATCH_SIZE
        params = {"page_size": limit, "scroll_id": offset} if offset != None else None
        logger.info(f"Fetching locations data from {endpoint} with params {params}")
        return self._get(endpoint, params)

    def get_property_location(self, location_id: str) -> Dict[str, Any]:
        """Get property location data from Reidin API"""
        endpoint = Config.PROPERTY_LOCATION_ENDPOINT.format(location_id=location_id)
        return self._get(endpoint)

    def get_property_details(self, property_id: str) -> Dict[str, Any]:
        """Get property details data from Reidin API"""
        endpoint = Config.PROPERTY_DETAILS_ENDPOINT.format(property_id=property_id)
        return self._get(endpoint)

    def get_indicators_aliases(
        self, country_code: str, params: Dict[str, Any] = Config.DEFAULT_PARAMS
    ) -> Dict[str, Any]:
        """Get indicators aliases data from Reidin API"""
        endpoint = Config.INDICATORS_ALIASES_ENDPOINT.format(country_code=country_code)

        return self._get(endpoint, params)

    def get_indicators_area_aliases(
        self,
        country_code: str,
        params: Dict[str, Any] = Config.DEFAULT_PARAMS,
    ) -> Dict[str, Any]:
        """Get indicators area aliases data from Reidin API"""
        endpoint = Config.INDICATORS_AREA_ALIASES_ENDPOINT.format(
            country_code=country_code
        )

        return self._get(endpoint, params)

    # def get_indicators(self, country_code: str, groups_id: int | None = None, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    #     endpoint = Config.INDICATORS_ENDPOINT.format(country_code=country_code)
    #     params = {
    #         'limit': limit,
    #         'offset': offset
    #     }
    #     if groups_id:
    #         params['groups_id'] = groups_id
    #     return self._get(endpoint, params)
