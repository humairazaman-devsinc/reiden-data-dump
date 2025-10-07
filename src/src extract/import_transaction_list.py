#!/usr/bin/env python3
"""
Transaction List Data Importer with Batch Processing

This script imports transaction list data from the Reidin API following the established ETL pattern.
It processes locations and fetches transaction list data for each location with various parameter combinations.
Uses parallel processing for fast data dump across all possible parameter combinations.
"""

import argparse
import logging
import time
import sys
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from database import DatabaseManager
from api_client import ReidinAPIClient
from processors import DataProcessor
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transaction_list_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class _RateLimiter:
    """Simple global rate limiter to keep total RPS under a cap across threads."""
    def __init__(self, min_interval_sec: float) -> None:
        self._min_interval = max(min_interval_sec, 0.0)
        self._lock = threading.Lock()
        self._next_allowed_time = 0.0

    def acquire(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            if now < self._next_allowed_time:
                time.sleep(self._next_allowed_time - now)
                now = time.time()
            self._next_allowed_time = now + self._min_interval


class TransactionListImporter:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
        self.processor = DataProcessor()
        
        # Create the transaction_list table if it doesn't exist
        try:
            self.db.create_transaction_list_table()
        except Exception as e:
            logger.warning(f"Could not create transaction_list table: {e}")
        
        # Default parameters based on the user's example
        self.property_types = ["residential", "office"]
        self.activity_types = ["ready", "off-plan", "mortgage", "off-plan-mortgage"]
        self.currencies = ["aed"]
        self.measurements = ["imp"]
        
        # Concurrency and rate limiting settings
        self.max_workers = 8
        self.requests_per_second = 4.0
        self.min_interval_sec = 1.0 / self.requests_per_second

    def _to_list(self, x):
        """Convert single values to list format"""
        if isinstance(x, (list, tuple)):
            return list(x)
        return [x]

    def get_locations(self, country_code: str, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Get locations from the database"""
        try:
            # Prefer pulling ALL locations from DB (no country filter), then fallback to country-specific
            db_rows = self.db.get_locations(country_code=None, limit=limit, offset=offset)
            if not db_rows:
                db_rows = self.db.get_locations(country_code=country_code, limit=limit, offset=offset)
                if db_rows:
                    logger.info(f"No DB rows without country filter. Using {len(db_rows)} for COUNTRY_CODE='{country_code}'.")

            # Build unique location id list
            location_ids_raw: List[str] = [str(r.get("location_id")) for r in db_rows if r.get("location_id") is not None]
            seen_loc: set[str] = set()
            location_ids: List[str] = []
            for lid in location_ids_raw:
                if lid in seen_loc:
                    continue
                seen_loc.add(lid)
                location_ids.append(lid)
                if limit and len(location_ids) >= limit:
                    break
            
            logger.info(f"Using {len(location_ids)} unique location ids (sample: {location_ids[:5]})")
            return [{"location_id": lid} for lid in location_ids]
            
        except Exception as e:
            logger.error(f"Failed to get locations: {e}")
            return []

    def generate_parameter_combinations(self) -> List[Dict[str, Any]]:
        """Generate all possible parameter combinations"""
        param_sets: List[Dict[str, Any]] = []
        
        property_type_values = self._to_list(self.property_types)
        activity_type_values = self._to_list(self.activity_types)
        currency_values = self._to_list(self.currencies)
        measurement_values = self._to_list(self.measurements)
        
        for pt in property_type_values:
            for act in activity_type_values:
                for cur in currency_values:
                    for meas in measurement_values:
                        param_sets.append({
                            "property_type": pt,
                            "activity_type": act,
                            "currency": cur,
                            "measurement": meas,
                        })
        
        logger.info(f"Generated {len(param_sets)} parameter combinations")
        return param_sets


    def _fetch_and_process_one(
        self, 
        param_set: Dict[str, Any], 
        location_id: str, 
        limiter: _RateLimiter,
        country_code: str
    ) -> int:
        """Fetch one (param_set, location_id) result and process it directly to database. Returns number of records processed."""
        params = dict(param_set)
        params["location_id"] = location_id

        try:
            limiter.acquire()
            with ReidinAPIClient() as client:
                payload = client.fetch_transactions_list(
                    country_code=country_code,
                    location_id=location_id,
                    property_type=param_set["property_type"],
                    activity_type=param_set["activity_type"],
                    currency=param_set["currency"],
                    measurement=param_set["measurement"]
                )
        except Exception as e:
            logger.warning(f"Failed to fetch data for params={param_set} location_id={location_id}: {e}")
            return 0

        results = payload.get("results") if isinstance(payload, dict) else None

        # Print per-hit summary to terminal (best-effort)
        try:
            if isinstance(results, list):
                logger.info(f"params={param_set} location_id={location_id} results_count={len(results)}")
            else:
                logger.info(f"params={param_set} location_id={location_id} results_count=0")
        except Exception:
            logger.info(f"params={param_set} location_id={location_id} results_count={(len(results) if isinstance(results, list) else 0)}")

        if isinstance(results, list) and results:
            # Process the data
            processed_data = self.processor.process_transaction_list_data(
                raw_data=results,
                location_id=location_id,
                property_type=param_set["property_type"],
                activity_type=param_set["activity_type"],
                currency=param_set["currency"],
                measurement=param_set["measurement"]
            )
            
            if processed_data:
                # Insert into database
                self.db.insert_transaction_list_data(processed_data)
                logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['currency']}_{param_set['measurement']} - {len(processed_data)} records")
                return len(processed_data)
            else:
                logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['currency']}_{param_set['measurement']} - No data to process")
                return 0
        else:
            logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['currency']}_{param_set['measurement']} - No results found")
            return 0

    def process_transaction_list_batch(
        self,
        country_code: str = "AE",
        max_locations: int = 500,
        offset: int = 0,
        dry_run: bool = False
    ) -> None:
        """Main method to process transaction list data with batch processing and database storage"""
        
        logger.info("🚀 Starting Transaction List data import with batch processing")
        logger.info(f"Country: {country_code}")
        logger.info(f"Max locations: {max_locations}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Requests per second: {self.requests_per_second}")
        logger.info(f"Dry run: {dry_run}")
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be saved to database")
        
        # 1) Gather location IDs from the database
        locations = self.get_locations(country_code, max_locations, offset)
        if not locations:
            logger.error("No location ids found in DB; exiting.")
            return
        
        location_ids = [loc["location_id"] for loc in locations]
        
        # 2) Generate parameter combinations
        param_sets = self.generate_parameter_combinations()
        
        # 3) For each param set, iterate locations and call the endpoint (concurrently)
        total_records = 0
        successful_requests = 0
        failed_requests = 0
        limiter = _RateLimiter(self.min_interval_sec)

        for idx, param_set in enumerate(param_sets, start=1):
            logger.info(f"Running param set {idx}/{len(param_sets)}: {param_set}")
            
            if not dry_run:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [executor.submit(self._fetch_and_process_one, param_set, loc_id, limiter, country_code) for loc_id in location_ids]
                    for fut in as_completed(futures):
                        try:
                            records_count = fut.result()
                            total_records += records_count
                            successful_requests += 1
                        except Exception as e:
                            logger.warning(f"Failed to process location: {e}")
                            failed_requests += 1
            else:
                logger.info(f"🧪 DRY RUN: Would process {len(location_ids)} locations for param set {param_set}")
                successful_requests += len(location_ids)

        # Final summary
        logger.info("🎉 Transaction List import completed!")
        logger.info(f"Total parameter combinations: {len(param_sets)}")
        logger.info(f"Total locations processed: {len(location_ids)}")
        logger.info(f"Total API calls made: {len(param_sets) * len(location_ids)}")
        logger.info(f"Successful requests: {successful_requests}")
        logger.info(f"Failed requests: {failed_requests}")
        if not dry_run:
            logger.info(f"Total records inserted into database: {total_records}")

    def process_transaction_list_etl(
        self,
        country_code: str = "AE",
        max_locations: int = 500,
        dry_run: bool = False
    ) -> None:
        """ETL method to process transaction list data and store in database"""
        
        logger.info("🚀 Starting Transaction List ETL process")
        logger.info(f"Country: {country_code}")
        logger.info(f"Max locations: {max_locations}")
        logger.info(f"Dry run: {dry_run}")
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be inserted into database")
        
        # Get locations from database
        locations = self.get_locations(country_code, max_locations)
        if not locations:
            logger.error("No locations found in database")
            return
        
        # Generate parameter combinations
        param_sets = self.generate_parameter_combinations()
        
        total_records = 0
        successful_requests = 0
        failed_requests = 0
        
        for idx, param_set in enumerate(param_sets, start=1):
            logger.info(f"Processing param set {idx}/{len(param_sets)}: {param_set}")
            
            for location in locations:
                location_id = location["location_id"]
                
                try:
                    # Fetch data from API
                    response = self.api_client.fetch_transactions_list(
                        country_code=country_code,
                        location_id=location_id,
                        property_type=param_set["property_type"],
                        activity_type=param_set["activity_type"],
                        currency=param_set["currency"],
                        measurement=param_set["measurement"]
                    )
                    
                    # Process the response
                    results = response.get("results", [])
                    
                    if results:
                        # Process the data
                        processed_data = self.processor.process_transaction_list_data(
                            raw_data=results,
                            location_id=location_id,
                            property_type=param_set["property_type"],
                            activity_type=param_set["activity_type"],
                            currency=param_set["currency"],
                            measurement=param_set["measurement"]
                        )
                        
                        if processed_data and not dry_run:
                            # Insert into database
                            self.db.insert_transaction_list_data(processed_data)
                            total_records += len(processed_data)
                            logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['currency']}_{param_set['measurement']} - {len(processed_data)} records")
                        elif processed_data and dry_run:
                            logger.info(f"🧪 DRY RUN: Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['currency']}_{param_set['measurement']} - {len(processed_data)} records would be inserted")
                        
                        successful_requests += 1
                    else:
                        logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['currency']}_{param_set['measurement']} - No data found")
                        successful_requests += 1
                    
                    # Be gentle with the API
                    time.sleep(0.25)
                    
                except Exception as e:
                    logger.warning(f"Location {location_id}: Failed combination {param_set['property_type']}_{param_set['activity_type']}_{param_set['currency']}_{param_set['measurement']} - {e}")
                    failed_requests += 1
                    continue
        
        # Final summary
        logger.info("🎉 Transaction List ETL completed!")
        logger.info(f"Total parameter combinations: {len(param_sets)}")
        logger.info(f"Total locations processed: {len(locations)}")
        logger.info(f"Successful requests: {successful_requests}")
        logger.info(f"Failed requests: {failed_requests}")
        if not dry_run:
            logger.info(f"Total records inserted: {total_records}")


def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description="Import Transaction List data from Reidin API")
    
    parser.add_argument(
        "--country-code", 
        default="AE", 
        help="Country code (default: AE)"
    )
    parser.add_argument(
        "--max-locations", 
        type=int, 
        default=500, 
        help="Maximum number of locations to process (default: 500)"
    )
    parser.add_argument(
        "--offset", 
        type=int, 
        default=0, 
        help="Number of locations to skip from the beginning (default: 0)"
    )
    parser.add_argument(
        "--max-workers", 
        type=int, 
        default=8, 
        help="Maximum number of worker threads (default: 8)"
    )
    parser.add_argument(
        "--requests-per-second", 
        type=float, 
        default=4.0, 
        help="Maximum requests per second (default: 4.0)"
    )
    parser.add_argument(
        "--batch-mode", 
        action="store_true", 
        help="Run in batch processing mode (parallel processing for faster data dump)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Run in dry-run mode (no data insertion/saving)"
    )
    
    args = parser.parse_args()
    
    try:
        importer = TransactionListImporter()
        
        # Override settings from command line
        importer.max_workers = args.max_workers
        importer.requests_per_second = args.requests_per_second
        importer.min_interval_sec = 1.0 / args.requests_per_second
        
        if args.batch_mode:
            importer.process_transaction_list_batch(
                country_code=args.country_code,
                max_locations=args.max_locations,
                offset=args.offset,
                dry_run=args.dry_run
            )
        else:
            importer.process_transaction_list_etl(
                country_code=args.country_code,
                max_locations=args.max_locations,
                dry_run=args.dry_run
            )
        
    except Exception as e:
        logger.error(f"Transaction List import failed: {e}")
        raise


if __name__ == "__main__":
    main()
