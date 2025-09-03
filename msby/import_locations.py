import argparse
import logging
import sys
from typing import List, Dict, Any, Optional
from api_client import ReidinAPIClient
from processors import DataProcessor
from database import DatabaseManager

logger = logging.getLogger(__name__)


class LocationImporter:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()

    def get_locations(
        self, country_code: str, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all locations for a given country code."""
        all_locations: List[Dict[str, Any]] = []
        scroll_id: Optional[str] = None
        iteration = 0

        with self.api_client as client:
            while True:
                logger.info("Fetching locations (iteration %d)...", iteration)
                iteration += 1

                raw = client.get_locations(country_code, page_size, scroll_id)
                logger.info("scroll_new: %s", raw.get("scroll_id"))

                if not isinstance(raw, dict) or "results" not in raw:
                    logger.error("Unexpected API response type: %r", raw)
                    break

                results = raw.get("results", [])
                if not results:
                    logger.info("Reached the limit, breaking the loop")
                    break
                scroll_new = raw.get("scroll_id")

                all_locations.extend(results)
                logger.info(
                    "Fetched batch of %d, total %d", len(results), len(all_locations)
                )

                if scroll_id == scroll_new or not scroll_new:
                    logger.info("No new scroll_id, stopping.")
                    break

                scroll_id = scroll_new
                logger.info("scroll_id: %s", scroll_id)

        return all_locations

    def process_locations(
        self, country_code: str, limit: Optional[int] = None, dry_run: bool = False
    ) -> None:
        """Retrieve, process, and optionally store locations data."""
        location_data = self.get_locations(country_code, limit)
        if not location_data:
            logger.warning("No data retrieved from API.")
            return

        logger.info("Total records retrieved: %d", len(location_data))
        processed = DataProcessor.process_location_data(location_data)
        stats = DataProcessor.validate_data_quality(processed)
        logger.info("Validation stats: %s", stats)

        if not dry_run and processed:
            self.db.insert_location_data(processed)
        elif dry_run:
            logger.info("Dry run: no database write performed.")


def main():
    parser = argparse.ArgumentParser(
        description="Import locations data from Reidin API."
    )
    parser.add_argument("--country-code", default="ae")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("./reidin_location_import.log"),
        ],
    )

    try:
        logger.info("Starting location import for: %s", args.country_code)
        LocationImporter().process_locations(
            args.country_code, args.limit, args.dry_run
        )
    except Exception:
        logger.exception("Location import failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
