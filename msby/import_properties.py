import argparse
import logging
import sys
from typing import List, Dict, Any, Optional
from api_client import ReidinAPIClient
from processors import DataProcessor
from database import DatabaseManager

logger = logging.getLogger(__name__)


class PropertyImporter:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()

    def get_properties_by_locations(
        self, country_code: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch properties for all locations in a country."""
        all_properties: List[Dict[str, Any]] = []

        # Get all locations for the country
        locations = self.db.get_locations(country_code, limit)
        logger.info("Found %d locations for country %s", len(locations), country_code)

        for location in locations:
            location_id = location.get("location_id")
            if not location_id:
                continue

            logger.info("Processing location %d for properties", location_id)

            try:
                with self.api_client as client:
                    raw = client.get_property_location(str(location_id))

                    if not isinstance(raw, dict) or "results" not in raw:
                        logger.warning(
                            "Unexpected API response for location %d: %r",
                            location_id,
                            raw,
                        )
                        continue

                    results = raw.get("results", [])
                    logger.info(
                        "Found %d properties for location %d", len(results), location_id
                    )

                    # Add location_id to each property for reference
                    for result in results:
                        if isinstance(result, dict):
                            result["_location_id"] = location_id

                    all_properties.extend(results)

            except Exception as e:
                logger.error(
                    "Failed to fetch properties for location %d: %s", location_id, e
                )
                continue

        logger.info("Total properties found: %d", len(all_properties))
        return all_properties

    def process_properties(
        self, country_code: str, limit: Optional[int] = None, dry_run: bool = False
    ) -> None:
        """Retrieve, process, and optionally store property data."""
        property_data = self.get_properties_by_locations(country_code, limit)

        if not property_data:
            logger.warning("No property data retrieved.")
            return

        logger.info("Total property records retrieved: %d", len(property_data))
        processed = DataProcessor.process_property_data(property_data)
        stats = DataProcessor.validate_data_quality(processed)
        logger.info("Validation stats: %s", stats)

        if not dry_run and processed:
            self.db.insert_property_data(processed)
        elif dry_run:
            logger.info("Dry run: no database write performed.")


def main():
    parser = argparse.ArgumentParser(
        description="Import property data from Reidin API."
    )
    parser.add_argument("--country-code", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("./reidin_property_import.log"),
        ],
    )

    try:
        logger.info("Starting property import for: %s", args.country_code)
        PropertyImporter().process_properties(
            args.country_code, args.limit, args.dry_run
        )
    except Exception:
        logger.exception("Property import failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
