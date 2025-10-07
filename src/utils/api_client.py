import logging
import time
from typing import Dict, Any, List, Optional
from utils.http_client import get_http_session
from .config import Config

logger = logging.getLogger(__name__)


class ReidinAPIClient:
    def __init__(self):
        self.session = get_http_session()
        self.base_url = Config.REIDIN_BASE_URL

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def get(
        self,
        endpoint: str,
        params: Dict[str, Any] | None = None,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        logger.info("Fetching data from %s with params %s", url, params)

        for attempt in range(max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=Config.TIMEOUT)
                response.raise_for_status()
                logger.info("API response: %s", response)
                data = response.json()
                return data
            except Exception as e:
                # Check if it's a 429 (rate limit) error
                if "response" in locals() and hasattr(response, "status_code"):
                    if response.status_code == 429:
                        if attempt < max_retries:
                            wait_time = retry_delay * (
                                2**attempt
                            )  # Exponential backoff
                            logger.warning(
                                "Rate limit hit (429), retrying in %.1f seconds (attempt %d/%d)",
                                wait_time,
                                attempt + 1,
                                max_retries,
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(
                                "Max retries reached for rate limit (429): %s", e
                            )
                            raise
                    elif response.status_code == 400:
                        logger.warning("Data not found: %s", e)
                        raise

                logger.error("API request failed: %s", e)
                raise

    def get_locations(
        self, country_code: str, limit: int | None = None, offset: int | None = None
    ) -> Dict[str, Any]:
        """Get locations data from Reidin API"""
        endpoint = Config.LOCATIONS_ENDPOINT.format(country_code=country_code)
        if limit is None and offset is not None:
            limit = Config.BATCH_SIZE
        params = {"page_size": limit, "scroll_id": offset} if offset != None else None
        logger.info("Fetching locations data from %s with params %s", endpoint, params)
        return self.get(endpoint, params)

    def get_property_location(self, location_id: str) -> Dict[str, Any]:
        """Get property location data from Reidin API"""
        endpoint = Config.PROPERTY_LOCATION_ENDPOINT.format(location_id=location_id)
        return self.get(endpoint)

    def get_property_details(self, property_id: str) -> Dict[str, Any]:
        """Get property details data from Reidin API"""
        endpoint = Config.PROPERTY_DETAILS_ENDPOINT.format(property_id=property_id)
        return self.get(endpoint)

    def get_indicators_aliases(
        self, country_code: str, params: Dict[str, Any] = Config.DEFAULT_PARAMS
    ) -> Dict[str, Any]:
        """Get indicators aliases data from Reidin API"""
        endpoint = Config.INDICATORS_ALIASES_ENDPOINT.format(country_code=country_code)

        return self.get(endpoint, params)

    def get_indicators_area_aliases(
        self,
        country_code: str,
        params: Dict[str, Any] = Config.DEFAULT_PARAMS,
    ) -> Dict[str, Any]:
        """Get indicators area aliases data from Reidin API"""
        endpoint = Config.INDICATORS_AREA_ALIASES_ENDPOINT.format(
            country_code=country_code
        )

        return self.get(endpoint, params)

    def fetch_cma_sales(
        self,
        country_code: str,
        property_id: str | int,
        alias: str = "last-five",
        currency: str = "aed",
        measurement: str = "imp",
        property_type: str = "Commercial",
        property_subtype: str = "Industrial-Warehouse",
        no_of_bedrooms: int | None = None,
        size: str | None = None,
        sales_activity_type: str | None = None,
        lat: str | None = None,
        lon: str | None = None,
    ) -> Dict[str, Any]:
        """Fetch CMA sales data from Reidin API"""
        endpoint = Config.CMA_SALES_ENDPOINT.format(country_code=country_code)

        params = {
            "alias": alias,
            "currency": currency,
            "measurement": measurement,
            "property_type": property_type,
            "property_subtype": property_subtype,
            "property_id": property_id,
        }

        # Add optional parameters if provided
        if no_of_bedrooms is not None:
            params["no_of_bedrooms"] = no_of_bedrooms
        if size:
            params["size"] = size
        if sales_activity_type:
            params["sales_activity_type"] = sales_activity_type
        if lat:
            params["lat"] = lat
        if lon:
            params["lon"] = lon

        logger.info("Fetching CMA sales data from %s with params %s", endpoint, params)
        return self.get(endpoint, params)

    def fetch_transactions_price(
        self,
        country_code: str,
        location_id: str | int,
        property_type: str,
        activity_type: str,
        property_id: int | None = None,
        property_sub_type: str | None = None,
        no_of_bedrooms: int | None = None,
    ) -> Dict[str, Any]:
        """Fetch transactions price data from Reidin API"""
        endpoint = Config.TRANSACTIONS_PRICE_ENDPOINT.format(country_code=country_code)

        params = {
            "location_id": location_id,
            "property_type": property_type,
            "activity_type": activity_type,
        }

        # Add optional parameters if provided
        if property_id is not None:
            params["property_id"] = property_id
        if property_sub_type:
            params["property_sub_type"] = property_sub_type
        if no_of_bedrooms is not None:
            params["no_of_bedrooms"] = no_of_bedrooms

        logger.info(
            "Fetching transactions price data from %s with params %s", endpoint, params
        )
        return self.get(endpoint, params)

    def fetch_transaction_raw_sales(
        self, country_code: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch transaction raw sales data from Reidin API"""
        endpoint = Config.TRANSACTION_RAW_SALES_ENDPOINT.format(
            country_code=country_code
        )
        logger.info(
            "Fetching transaction raw sales data from %s with params %s",
            endpoint,
            params,
        )
        return self.get(endpoint, params)

    def fetch_transaction_raw_rents(
        self, country_code: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch transaction raw rents data from Reidin API"""
        endpoint = Config.TRANSACTION_RAW_RENTS_ENDPOINT.format(
            country_code=country_code
        )
        logger.info(
            "Fetching transaction raw rents data from %s with params %s",
            endpoint,
            params,
        )
        return self.get(endpoint, params)

    def fetch_transaction_history(
        self, country_code: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch transaction history data from Reidin API"""
        endpoint = Config.TRANSACTION_HISTORY_ENDPOINT.format(country_code=country_code)
        logger.info(
            "Fetching transaction history data from %s with params %s", endpoint, params
        )
        return self.get(endpoint, params)

    def fetch_transactions_avg(
        self, country_code: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch transactions average data from Reidin API"""
        endpoint = Config.TRANSACTIONS_AVG_ENDPOINT.format(country_code=country_code)
        logger.info(
            "Fetching transactions average data from %s with params %s",
            endpoint,
            params,
        )
        return self.get(endpoint, params)

    def fetch_companies_from_properties(self, location_id: str) -> Dict[str, Any]:
        """Fetch companies data via property location from Reidin API"""
        logger.info("Fetching companies data for location %s", location_id)
        return self.get_property_location(location_id)

    def fetch_transactions_list(
        self,
        country_code: str,
        location_id: str | int,
        property_type: str,
        activity_type: str,
        currency: str = "aed",
        measurement: str = "imp",
    ) -> Dict[str, Any]:
        """Fetch transactions list data from Reidin API"""
        endpoint = Config.TRANSACTIONS_LIST_ENDPOINT.format(country_code=country_code)

        params = {
            "location_id": location_id,
            "property_type": property_type,
            "activity_type": activity_type,
            "currency": currency,
            "measurement": measurement,
        }

        logger.info(
            "Fetching transactions list data from %s with params %s", endpoint, params
        )
        return self.get(endpoint, params)

    def fetch_poi_cma(
        self,
        property_id: str | int,
        measurement: str,
        lang: str,
        lat: str | None = None,
        lon: str | None = None,
        max_retries: int = 5,
        retry_delay: float = 3.0,
    ) -> Dict[str, Any]:
        """Fetch POI CMA data from Reidin API with retry logic for rate limiting"""
        endpoint = Config.POI_CMA_ENDPOINT

        params = {
            "property_id": property_id,
            "measurement": measurement,
            "lang": lang,
        }

        # Add optional parameters if provided
        if lat:
            params["lat"] = lat
        if lon:
            params["lon"] = lon

        logger.info("Fetching POI CMA data from %s with params %s", endpoint, params)
        return self.get(
            endpoint, params, max_retries=max_retries, retry_delay=retry_delay
        )

    def fetch_cma2_rents(
        self,
        country_code: str,
        alias: str,
        currency: str,
        measurement: str,
        property_type: str,
        property_subtype: str,
        property_id: Optional[int] = None,
        max_retries: int = 5,
        retry_delay: float = 3.0,
    ) -> Dict[str, Any]:
        """Fetch CMA2-rents data from Reidin API with retry logic for rate limiting"""
        endpoint = Config.CMA2_RENTS_ENDPOINT.format(country_code=country_code)

        params = {
            "alias": alias,
            "currency": currency,
            "measurement": measurement,
            "property_type": property_type,
            "property_subtype": property_subtype,
        }

        # Add property_id if provided
        if property_id:
            params["property_id"] = property_id

        logger.info("Fetching CMA2-rents data from %s with params %s", endpoint, params)
        return self.get(
            endpoint, params, max_retries=max_retries, retry_delay=retry_delay
        )

    def fetch_cma_data(
        self, params: Dict[str, Any], max_retries: int = 3, retry_delay: float = 2.0
    ) -> Dict[str, Any]:
        """Fetch CMA data for a specific property with dynamic parameters"""
        country_code = params.get("country_code", "AE")
        endpoint = f"{country_code}/transactions/cma/"

        # Remove country_code from params as it's used in endpoint
        api_params = {k: v for k, v in params.items() if k != "country_code"}

        logger.info("Fetching CMA data from %s with params %s", endpoint, api_params)
        return self.get(
            endpoint, api_params, max_retries=max_retries, retry_delay=retry_delay
        )
