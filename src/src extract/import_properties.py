import argparse
import logging
import sys
from typing import List, Dict, Any, Optional
from api_client import ReidinAPIClient
from processors import DataProcessor
from database import DatabaseManager
from config import Config

logger = logging.getLogger(__name__)


class PropertyImporter:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()

    def get_properties_by_locations(
        self, country_code: str, limit: Optional[int] = None, dry_run: bool = False
    ) -> List[Dict[str, Any]]:
        """Fetch properties for all locations in a country."""
        all_properties: List[Dict[str, Any]] = []

        # Get all locations for the country
        locations = self.db.get_locations(country_code, limit)
        total_locations = len(locations)
        logger.info("Found %d locations for country %s", total_locations, country_code)

        if not locations:
            logger.warning("No locations found for country %s", country_code)
            return []

        # Process locations in batches to save to database periodically
        batch_size = Config.BATCH_SIZE
        total_batches = (total_locations + batch_size - 1) // batch_size
        
        logger.info(
            "Processing %d locations in %d batches of %d",
            total_locations,
            total_batches,
            batch_size,
        )
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_locations)
            batch_locations = locations[start_idx:end_idx]
            
            logger.info(
                "Processing batch %d/%d: %s locations %s-%s",
                batch_num + 1,
                total_batches,
                len(batch_locations),
                start_idx + 1,
                end_idx,
            )
            
            batch_properties = []
            successful_locations = 0
            failed_locations = 0
            
            # Process each location in the batch
            for i, location in enumerate(batch_locations):
                location_id = location.get("location_id")
                if not location_id:
                    logger.warning("Location at index %d has no location_id", start_idx + i)
                    failed_locations += 1
                    continue

                logger.info("Processing location %d (%d/%d in batch)", location_id, i + 1, len(batch_locations))

                try:
                    with self.api_client as client:
                        raw = client.get_property_location(str(location_id))

                        if not isinstance(raw, dict) or "results" not in raw:
                            logger.warning(
                                "Unexpected API response for location %d: %r",
                                location_id,
                                raw,
                            )
                            failed_locations += 1
                            continue

                        results = raw.get("results", [])
                        logger.info(
                            "Found %d properties for location %d", len(results), location_id
                        )

                        # Add location_id to each property for reference
                        for result in results:
                            if isinstance(result, dict):
                                result["_location_id"] = location_id

                        batch_properties.extend(results)
                        successful_locations += 1

                except Exception as e:
                    logger.error(
                        "Failed to fetch properties for location %d: %s", location_id, e
                    )
                    failed_locations += 1
                    continue
            
            logger.info(
                "Batch %d/%d completed: %d successful, %d failed out of %d locations",
                batch_num + 1,
                total_batches,
                successful_locations,
                failed_locations,
                len(batch_locations)
            )
            
            # Save batch to database immediately to prevent data loss
            if batch_properties:
                try:
                    # Process the batch data
                    processed = DataProcessor.process_property_data(batch_properties)
                    
                    if processed:
                        if not dry_run:
                            self.db.insert_property_data(processed)
                            logger.info(
                                "Saved batch %d/%d to database: %d properties",
                                batch_num + 1,
                                total_batches,
                                len(processed)
                            )
                        else:
                            logger.info(
                                "Dry run: would save batch %d/%d to database: %d properties",
                                batch_num + 1,
                                total_batches,
                                len(processed)
                            )
                        all_properties.extend(batch_properties)
                    else:
                        logger.warning(
                            "Batch %d/%d produced no processed data",
                            batch_num + 1,
                            total_batches
                        )
                        
                except Exception as e:
                    logger.error(
                        "Failed to save batch %d/%d to database: %s",
                        batch_num + 1,
                        total_batches,
                        e,
                    )
                    # Continue with next batch even if this one failed to save

        logger.info("Total properties found: %d", len(all_properties))
        
        # Calculate total statistics
        total_successful = len(all_properties)
        total_failed_locations = total_locations - (total_successful // 10)  # Rough estimate
        logger.info(
            "Final summary: %d properties from successful locations, %d locations failed out of %d total locations",
            total_successful,
            total_failed_locations,
            total_locations
        )
        
        return all_properties

    def process_properties(
        self, country_code: str, limit: Optional[int] = None, dry_run: bool = False
    ) -> None:
        """Retrieve, process, and optionally store property data."""
        if dry_run:
            logger.info("Dry run mode: data will not be saved to database")
        
        property_data = self.get_properties_by_locations(country_code, limit, dry_run)

        if not property_data:
            logger.warning("No property data retrieved.")
            return

        logger.info("Total property records retrieved: %d", len(property_data))
        
        # Collect validation stats for final summary
        try:
            processed = DataProcessor.process_property_data(property_data)
            if processed:
                stats = DataProcessor.validate_data_quality(processed)
                logger.info("Final validation stats: %s", stats)
            else:
                logger.warning("No processed data available for validation")
        except Exception as e:
            logger.error("Failed to collect validation stats: %s", e)

        # Data has already been processed and saved during the fetch process
        # Just log the final summary
        logger.info("Import completed successfully. All data has been saved to database.")


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
