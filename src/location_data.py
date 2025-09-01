
import logging
from typing import Dict, List
from argparse import Namespace
from src.api_client import ReidinAPIClient
from src.data_processor import DataProcessor

logger = logging.getLogger(__name__)


def get_locations_data(args: Namespace, api_client: ReidinAPIClient) -> List[Dict] | None:
    """Fetch and process location data from Reidin API"""
    raw_data = api_client.get_locations_data(country_code=args.country_code)

    if not raw_data:
        logger.warning("No data received from API")
        return None

    logger.info("Fetched %i records from API", len(raw_data))

    # Process the data
    logger.info("Processing data...")
    processed_data = DataProcessor.process_location_data(raw_data)

    if not processed_data:
        logger.warning("No data processed")
        return None

    logger.info(f"Processed {len(processed_data)} records")
    return processed_data
