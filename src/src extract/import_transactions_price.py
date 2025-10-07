#!/usr/bin/env python3
"""
Transactions Price Data Importer

This script imports transactions price data from the Reidin API following the established ETL pattern.
It processes locations and fetches price data for each location with various parameter combinations.
"""

import argparse
import logging
import time
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
        logging.FileHandler('transactions_price_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TransactionsPriceImporter:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
        self.processor = DataProcessor()
        
        # Parameter combinations to try based on the original script
        self.property_types = ["residential", "office"]
        self.activity_types = ["ready", "off-plan"]
        self.property_sub_types = ["apartment", "villa", "office"]
        
        # Batch processing settings
        self.max_workers = 8
        self.requests_per_second = 4.0
        self.min_interval_sec = 1.0 / self.requests_per_second

    def generate_parameter_combinations(self) -> List[Dict[str, Any]]:
        """Generate all possible parameter combinations"""
        combinations = []
        for property_type in self.property_types:
            for activity_type in self.activity_types:
                for property_sub_type in self.property_sub_types:
                    combinations.append({
                        "property_type": property_type,
                        "activity_type": activity_type,
                        "property_sub_type": property_sub_type
                    })
        return combinations

    def get_locations(self, country_code: str, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Get locations from the database"""
        try:
            locations = self.db.get_locations(country_code=country_code, limit=limit, offset=offset)
            logger.info(f"Retrieved {len(locations)} locations from database")
            return locations
        except Exception as e:
            logger.error(f"Failed to get locations: {e}")
            return []

    def process_transactions_price_for_location(
        self, 
        location_id: str | int,
        country_code: str = "AE",
        max_combinations: int = 20,
        request_delay: float = 0.25
    ) -> int:
        """Process transactions price data for a single location with multiple parameter combinations"""
        
        total_records = 0
        combinations_tried = 0
        successful_combinations = 0
        
        logger.info(f"Processing location {location_id}")
        
        # Try different parameter combinations
        for property_type in self.property_types:
            for activity_type in self.activity_types:
                for property_sub_type in self.property_sub_types:
                    if combinations_tried >= max_combinations:
                        break
                        
                    try:
                        # Fetch data from API
                        response = self.api_client.fetch_transactions_price(
                            country_code=country_code,
                            location_id=location_id,
                            property_type=property_type,
                            activity_type=activity_type,
                            property_sub_type=property_sub_type
                        )
                        
                        # Process the response
                        results = response.get("results", [])
                        
                        if results:
                            # Process the data
                            processed_data = self.processor.process_transactions_price_data(
                                raw_data=results,
                                location_id=location_id,
                                property_type=property_type,
                                activity_type=activity_type,
                                property_sub_type=property_sub_type
                            )
                            
                            if processed_data:
                                # Insert into database
                                self.db.insert_transactions_price_data(processed_data)
                                total_records += len(processed_data)
                                successful_combinations += 1
                                logger.info(f"  ✅ {property_type}_{activity_type}_{property_sub_type}: {len(processed_data)} records")
                        
                        # Be gentle with the API
                        time.sleep(request_delay)
                        combinations_tried += 1
                        
                    except Exception as e:
                        logger.warning(f"  ❌ {property_type}_{activity_type}_{property_sub_type}: {str(e)[:100]}")
                        combinations_tried += 1
                        continue
                
                if combinations_tried >= max_combinations:
                    break
            if combinations_tried >= max_combinations:
                break
        
        logger.info(f"Location {location_id}: {successful_combinations}/{combinations_tried} combinations successful, {total_records} total records")
        return total_records

    def _fetch_and_process_one(self, location_id: str, param_set: Dict[str, Any], country_code: str, dry_run: bool = False) -> int:
        """Fetch and process data for one location-parameter combination"""
        try:
            # Rate limiting
            time.sleep(self.min_interval_sec)
            
            # Fetch data from API
            response = self.api_client.fetch_transactions_price(
                country_code=country_code,
                location_id=location_id,
                property_type=param_set["property_type"],
                activity_type=param_set["activity_type"],
                property_sub_type=param_set["property_sub_type"]
            )
            
            # Process the response
            results = response.get("results", [])
            
            if results:
                # Process the data
                processed_data = self.processor.process_transactions_price_data(
                    raw_data=results,
                    location_id=location_id,
                    property_type=param_set["property_type"],
                    activity_type=param_set["activity_type"],
                    property_sub_type=param_set["property_sub_type"]
                )
                
                if processed_data and not dry_run:
                    # Insert into database
                    self.db.insert_transactions_price_data(processed_data)
                    logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['property_sub_type']} - {len(processed_data)} records")
                    return len(processed_data)
                elif processed_data and dry_run:
                    logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['property_sub_type']} - {len(processed_data)} records (DRY RUN)")
                    return len(processed_data)
                else:
                    logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['property_sub_type']} - No data to process")
                    return 0
            else:
                logger.info(f"Location {location_id}: {param_set['property_type']}_{param_set['activity_type']}_{param_set['property_sub_type']} - No results found")
                return 0
                
        except Exception as e:
            logger.warning(f"Failed to fetch data for location {location_id} with params {param_set}: {e}")
            return 0

    def process_transactions_price_batch(
        self,
        country_code: str = "AE",
        max_locations: int = 500,
        offset: int = 0,
        dry_run: bool = False
    ) -> None:
        """Main method to process transactions price data with batch processing and database storage"""
        
        logger.info("🚀 Starting Transactions Price data import with batch processing")
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
        param_combinations = self.generate_parameter_combinations()
        logger.info(f"Generated {len(param_combinations)} parameter combinations")
        
        # 3) Create all location-parameter pairs
        all_tasks = []
        for location_id in location_ids:
            for param_set in param_combinations:
                all_tasks.append((location_id, param_set))
        
        logger.info(f"Total tasks to process: {len(all_tasks)}")
        
        # 4) Process in parallel batches
        total_records = 0
        successful_requests = 0
        failed_requests = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._fetch_and_process_one, location_id, param_set, country_code, dry_run): (location_id, param_set)
                for location_id, param_set in all_tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                location_id, param_set = future_to_task[future]
                try:
                    records = future.result()
                    total_records += records
                    successful_requests += 1
                except Exception as e:
                    logger.error(f"Task failed for location {location_id} with params {param_set}: {e}")
                    failed_requests += 1
        
        # Final summary
        logger.info("🎉 Transactions Price import completed!")
        logger.info(f"Total parameter combinations: {len(param_combinations)}")
        logger.info(f"Total locations processed: {len(location_ids)}")
        logger.info(f"Total API calls made: {len(all_tasks)}")
        logger.info(f"Successful requests: {successful_requests}")
        logger.info(f"Failed requests: {failed_requests}")
        if not dry_run:
            logger.info(f"Total records inserted into database: {total_records}")

    def process_transactions_price(
        self, 
        country_code: str = "AE",
        max_locations: int = 50,
        max_combinations_per_location: int = 20,
        request_delay: float = 0.25,
        dry_run: bool = False
    ) -> None:
        """Main method to process transactions price data for multiple locations"""
        
        logger.info("🚀 Starting Transactions Price data import")
        logger.info(f"Country: {country_code}")
        logger.info(f"Max locations: {max_locations}")
        logger.info(f"Max combinations per location: {max_combinations_per_location}")
        logger.info(f"Request delay: {request_delay}s")
        logger.info(f"Dry run: {dry_run}")
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be inserted")
        
        # Get locations from database
        locations = self.get_locations(country_code=country_code, limit=max_locations)
        
        if not locations:
            logger.error("No locations found in database")
            return
        
        total_records = 0
        successful_locations = 0
        failed_locations = 0
        
        for i, location_data in enumerate(locations, 1):
            location_id = location_data.get("location_id")
            
            if not location_id:
                logger.warning(f"Location {i} has no ID, skipping")
                failed_locations += 1
                continue
            
            logger.info(f"Processing location {i}/{len(locations)}: {location_id}")
            
            try:
                if not dry_run:
                    records = self.process_transactions_price_for_location(
                        location_id=location_id,
                        country_code=country_code,
                        max_combinations=max_combinations_per_location,
                        request_delay=request_delay
                    )
                    total_records += records
                    
                    if records > 0:
                        successful_locations += 1
                        logger.info(f"✅ Location {location_id}: {records} records")
                    else:
                        logger.info(f"⚠️ Location {location_id}: No data found")
                else:
                    logger.info(f"🧪 DRY RUN: Would process location {location_id}")
                    successful_locations += 1
                
            except Exception as e:
                logger.error(f"❌ Location {location_id}: Failed - {e}")
                failed_locations += 1
                continue
        
        # Final summary
        logger.info("🎉 Transactions Price import completed!")
        logger.info(f"Total locations processed: {len(locations)}")
        logger.info(f"Successful locations: {successful_locations}")
        logger.info(f"Failed locations: {failed_locations}")
        logger.info(f"Total records inserted: {total_records}")


def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description="Import Transactions Price data from Reidin API")
    
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
        "--max-combinations", 
        type=int, 
        default=20, 
        help="Maximum parameter combinations to try per location (default: 20) - only for non-batch mode"
    )
    parser.add_argument(
        "--request-delay", 
        type=float, 
        default=0.25, 
        help="Delay between API requests in seconds (default: 0.25) - only for non-batch mode"
    )
    parser.add_argument(
        "--offset", 
        type=int, 
        default=0, 
        help="Number of locations to skip from the beginning (default: 0)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Run in dry-run mode (no data insertion)"
    )
    
    args = parser.parse_args()
    
    try:
        importer = TransactionsPriceImporter()
        
        # Override settings from command line
        importer.max_workers = args.max_workers
        importer.requests_per_second = args.requests_per_second
        importer.min_interval_sec = 1.0 / args.requests_per_second
        
        if args.batch_mode:
            importer.process_transactions_price_batch(
                country_code=args.country_code,
                max_locations=args.max_locations,
                offset=args.offset,
                dry_run=args.dry_run
            )
        else:
            importer.process_transactions_price(
                country_code=args.country_code,
                max_locations=args.max_locations,
                max_combinations_per_location=args.max_combinations,
                request_delay=args.request_delay,
                dry_run=args.dry_run
            )
        
    except Exception as e:
        logger.error(f"Transactions Price import failed: {e}")
        raise


if __name__ == "__main__":
    main()
