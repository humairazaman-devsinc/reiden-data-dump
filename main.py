#!/usr/bin/env python3
"""
Main script to fetch Reidin API data and store it in PostgreSQL
"""

import logging
import sys
import argparse

from src.api_client import ReidinAPIClient
from src.database import DatabaseManager
from src.config import Config
from src.location_data import get_locations_data

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reidin_data_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main function to orchestrate the data import process"""

    parser = argparse.ArgumentParser(
        description='Import Reidin API data to PostgreSQL')
    parser.add_argument('--country-code', '-c', default='ae',
                        help='Country code (default: ae)')
    parser.add_argument('--groups-id', '-g', default=8200, type=int,
                        help='Group ID filter (default: 8200)')
    parser.add_argument('--limit', '-l', type=int, default=100,
                        help='Number of records to fetch per request (default: 100)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Fetch data without storing to database')

    args = parser.parse_args()

    logger.info("Starting Reidin data import process")
    logger.info("Country code: %s", args.country_code)
    logger.info("Group ID: %i", args.groups_id)
    logger.info("Dry run: %s", args.dry_run)

    api_client = None
    db_manager = None

    try:
        # Initialize API client
        api_client = ReidinAPIClient()
        logger.info("API client initialized")

        # Initialize database manager (only if not dry run)
        if not args.dry_run:
            db_manager = DatabaseManager()
            logger.info("Database manager initialized")

        # Fetch data from API
        logger.info("Fetching data from Reidin API...")

        location_data = get_locations_data(args, api_client)

        if args.dry_run:
            logger.info("Dry run mode - data not stored to database")
            logger.info("Would store %i location records", len(location_data or []))
            if location_data:
                logger.info("Sample data: %s", location_data[:2])
            return

        if not db_manager:
            logger.error("Database manager is not initialized")
            return

        # Store data in database
        logger.info("Storing data in database...")

        if location_data:
            db_manager.insert_locations(location_data)
        else:
            logger.warning("No location data to store")

        logger.info("Data import completed successfully!")

    except Exception as e:
        logger.error("Error during data import: %s", e)
        raise

    finally:
        # Clean up resources
        if api_client:
            api_client.close()
        if db_manager:
            db_manager.close()
        logger.info("Resources cleaned up")


if __name__ == "__main__":
    main()
