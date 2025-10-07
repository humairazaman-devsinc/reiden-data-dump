import argparse
import logging
import sys
from typing import List, Dict, Any, Optional
from api_client import ReidinAPIClient
from processors import DataProcessor
from database import DatabaseManager
from config import Config

logger = logging.getLogger(__name__)


class IndicatorAliasedImporter:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()

    def get_indicators_aliased(
        self,
        country_code: str,
        limit: Optional[int] = None,
        params: Dict[str, Any] = Config.DEFAULT_PARAMS,
        key: str = "location_id",
    ) -> List[Dict[str, Any]]:
        """Fetch property-level indicators for a country."""
        batch_indicators: List[Dict[str, Any]] = []
        all_indicators = 0

        # Get all properties for the country to fetch indicators for each
        ids = []
        if key == "property_id":
            ids = [property["id"] for property in self.db.get_properties(limit)]
        elif key == "location_id":
            ids = [
                location["location_id"]
                for location in self.db.get_locations(None, limit)
            ]

        total_ids = len(ids)
        logger.info("Total ids: %d", total_ids)
        logger.info(
            "Fetching indicators for %d %s in country %s",
            total_ids,
            key[:-3],
            country_code,
        )

        # Process IDs in batches to reduce API calls
        batch_size = Config.INDICATORS_BATCH_SIZE
        total_batches = (total_ids + batch_size - 1) // batch_size

        logger.info(
            "Processing %d IDs in %d batches of %d",
            total_ids,
            total_batches,
            batch_size,
        )

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_ids)
            batch_ids = ids[start_idx:end_idx]

            logger.info(
                "Processing batch %d/%d: %s IDs %s-%s",
                batch_num + 1,
                total_batches,
                len(batch_ids),
                start_idx + 1,
                end_idx,
            )

            try:
                with self.api_client as client:
                    params[key] = ",".join(map(str, batch_ids))
                    logger.info("Params: %s", params)

                    raw = client.get_indicators_aliases(country_code, params)

                if not isinstance(raw, dict):
                    logger.warning(
                        "Unexpected API response for batch %d: %r", batch_num + 1, raw
                    )
                    continue

                batch_indicators.extend(raw.get("results", []))
                if len(batch_indicators) >= Config.BATCH_SIZE:
                    self.db.insert_indicator_aliased_data(batch_indicators)
                    all_indicators += len(batch_indicators)
                    batch_indicators = []

            except Exception as e:
                logger.error(
                    "Failed to fetch indicators for batch %d (%s IDs %s-%s): %s",
                    batch_num + 1,
                    len(batch_ids),
                    start_idx + 1,
                    end_idx,
                    e,
                )
                continue

        logger.info("Total indicator responses retrieved: %d", all_indicators)
        return batch_indicators

    def process_indicators_aliased(
        self,
        country_code: str,
        limit: Optional[int] = None,
        dry_run: bool = False,
        params: Dict[str, Any] = Config.DEFAULT_PARAMS,
        key: str = "location_id",
    ) -> None:
        """Retrieve, process, and optionally store indicator aliased data."""
        indicators_data = self.get_indicators_aliased(country_code, limit, params, key)

        if not indicators_data:
            logger.warning("No indicators data retrieved.")
            return

        logger.info("Total indicator responses retrieved: %d", len(indicators_data))
        # logger.info(indicators_data)

        # Process each indicator response individually
        all_processed = DataProcessor.process_indicator_aliased_data(indicators_data)
        # logger.info("Processed indicators: %s", all_processed)
        logger.info("All processed indicators: %d", len(all_processed))

        stats = DataProcessor.validate_data_quality(all_processed)
        logger.info("Validation stats: %s", stats)

        if not dry_run and all_processed:
            logger.info("Inserting %d indicators into database", len(all_processed))
            self.db.insert_indicator_aliased_data(all_processed)
            logger.info("Added %d indicators to database", len(all_processed))
        elif dry_run:
            logger.info("Dry run: no database write performed.")


def main():
    parser = argparse.ArgumentParser(
        description="Import property-level indicators data from Reidin API."
    )
    parser.add_argument("--country-code", default="ae")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--currency", default="aed")
    parser.add_argument("--measurement", default="int")
    parser.add_argument("--alias", default="sales-price")
    parser.add_argument("--property-type", default="residential-general")
    parser.add_argument(
        "--key", type=str, default="location_id", choices=["property_id", "location_id"]
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("./reidin_indicators_aliased_import.log"),
        ],
    )
    params = {
        "currency": args.currency,
        "measurement": args.measurement,
        "alias": args.alias,
        "property_type": args.property_type,
    }
    try:
        logger.info("Starting indicators aliased import for: %s", args.country_code)
        IndicatorAliasedImporter().process_indicators_aliased(
            args.country_code, args.limit, args.dry_run, params, args.key
        )
    except Exception:
        logger.exception("Indicators aliased import failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
