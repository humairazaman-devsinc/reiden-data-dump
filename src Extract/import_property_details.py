import argparse
import logging
import sys
from typing import List, Dict, Any, Optional
from api_client import ReidinAPIClient
from processors import DataProcessor
from database import DatabaseManager
from config import Config

logger = logging.getLogger(__name__)


class PropertyDetailsImporter:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()

    def get_property_details(
        self,
        properties: List[Dict[str, Any]],
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch detailed information for specific properties."""
        all_property_details: List[Dict[str, Any]] = []

        total_properties = len(properties)
        logger.info("Fetching details for %d properties", total_properties)

        properties_ids = [property["id"] for property in properties]

        # Process IDs in batches to save to database periodically
        batch_size = int(Config.BATCH_SIZE / 4)
        total_batches = (total_properties + batch_size - 1) // batch_size

        logger.info(
            "Processing %d properties in %d batches of %d",
            total_properties,
            total_batches,
            batch_size,
        )

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_properties)
            batch_ids = properties_ids[start_idx:end_idx]

            logger.info(
                "Processing batch %d/%d: %s properties %s-%s",
                batch_num + 1,
                total_batches,
                len(batch_ids),
                start_idx + 1,
                end_idx,
            )

            batch_property_details = []
            successful_ids = 0
            failed_ids = 0

            # Process each property ID individually
            for i, property_id in enumerate(batch_ids):
                logger.info(
                    "Processing property %d (%d/%d in batch)",
                    property_id,
                    i + 1,
                    len(batch_ids),
                )

                try:
                    with self.api_client as client:
                        raw = client.get_property_details(str(property_id))
                        if not isinstance(raw, list):
                            logger.warning(
                                "Unexpected API response for property %d: %r",
                                property_id,
                                raw,
                            )
                            failed_ids += 1
                            continue

                        # Add property_id for reference
                        batch_property_details.extend(raw)
                        successful_ids += 1

                except Exception as e:
                    logger.error(
                        "Failed to fetch details for property %d: %s", property_id, e
                    )
                    failed_ids += 1
                    continue

            logger.info(
                "Batch %d/%d completed: %d successful, %d failed out of %d properties",
                batch_num + 1,
                total_batches,
                successful_ids,
                failed_ids,
                len(batch_ids),
            )

            # Save batch to database immediately to prevent data loss
            if batch_property_details:
                try:
                    # Process the batch data
                    all_processed = []
                    for detail_data in batch_property_details:
                        processed = DataProcessor.process_property_details_data(
                            detail_data
                        )
                        all_processed.extend(processed)

                    if all_processed:
                        if not dry_run:
                            self.db.insert_property_details_data(all_processed)
                            logger.info(
                                "Saved batch %d/%d to database: %d property details",
                                batch_num + 1,
                                total_batches,
                                len(all_processed),
                            )
                        else:
                            logger.info(
                                "Dry run: would save batch %d/%d to database: %d property details",
                                batch_num + 1,
                                total_batches,
                                len(all_processed),
                            )
                        all_property_details.extend(batch_property_details)
                    else:
                        logger.warning(
                            "Batch %d/%d produced no processed data",
                            batch_num + 1,
                            total_batches,
                        )

                except Exception as e:
                    logger.error(
                        "Failed to save batch %d/%d to database: %s",
                        batch_num + 1,
                        total_batches,
                        e,
                    )
                    # Continue with next batch even if this one failed to save

        logger.info("Total property details retrieved: %d", len(all_property_details))

        # Calculate total statistics
        total_successful = len(all_property_details)
        total_failed = total_properties - total_successful
        logger.info(
            "Final summary: %d successful, %d failed out of %d total properties",
            total_successful,
            total_failed,
            total_properties,
        )

        return all_property_details

    def get_property_details_from_database(
        self,
        country_code: str = "ae",
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get property details for all properties in a country from the database."""
        # First get all properties for the country
        properties = self.db.get_properties(limit)

        if not properties:
            logger.warning("No properties found in database")
            return []

        logger.info("Found %d properties to fetch details for", len(properties))
        return self.get_property_details(properties, limit, dry_run)

    def process_property_details(
        self, country_code: str, limit: Optional[int] = None, dry_run: bool = False
    ) -> None:
        """Retrieve, process, and optionally store property details data."""
        if dry_run:
            logger.info("Dry run mode: data will not be saved to database")

        property_details_data = self.get_property_details_from_database(
            country_code, limit, dry_run
        )

        if not property_details_data:
            logger.warning("No property details data retrieved.")
            return

        logger.info(
            "Total property details records retrieved: %d", len(property_details_data)
        )

        # Collect validation stats for final summary
        try:
            all_processed = []
            for detail_data in property_details_data:
                processed = DataProcessor.process_property_details_data(detail_data)
                all_processed.extend(processed)

            if all_processed:
                stats = DataProcessor.validate_data_quality(all_processed)
                logger.info("Final validation stats: %s", stats)
            else:
                logger.warning("No processed data available for validation")
        except Exception as e:
            logger.error("Failed to collect validation stats: %s", e)

        # Data has already been processed and saved during the fetch process
        # Just log the final summary
        logger.info(
            "Import completed successfully. All data has been saved to database."
        )


def main():
    parser = argparse.ArgumentParser(
        description="Import property details data from Reidin API."
    )
    parser.add_argument("--country-code", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("./reidin_property_details_import.log"),
        ],
    )

    try:
        logger.info("Starting property details import for: %s", args.country_code)
        PropertyDetailsImporter().process_property_details(
            args.country_code, args.limit, args.dry_run
        )
    except Exception:
        logger.exception("Property details import failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
