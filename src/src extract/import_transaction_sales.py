#!/usr/bin/env python3
"""
Transaction Sales Importer

This script imports transaction sales data from the Reidin API
and stores it in the database following the same ETL pattern as other importers.
"""

import argparse
import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from database import DatabaseManager
from api_client import ReidinAPIClient
from processors import DataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reidin_transaction_sales_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TransactionSalesImporter:
    """Importer for transaction sales data from Reidin API"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
        
        # Parameter combinations
        self.measurements = ["int", "imp"]
        self.transaction_types = ["Sales - Ready", "Sales - Off-Plan"]
        
        # Batch processing settings
        self.max_workers = 8
        self.requests_per_second = 4.0
        self.min_interval_sec = 1.0 / self.requests_per_second

    def generate_parameter_combinations(self) -> List[Dict[str, Any]]:
        """Generate all possible parameter combinations"""
        combinations = []
        for measurement in self.measurements:
            for transaction_type in self.transaction_types:
                combinations.append({
                    "measurement": measurement,
                    "transaction_type": transaction_type
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

    def _fetch_and_process_one(self, location_id: str, param_set: Dict[str, Any], country_code: str, max_pages: int = 5, page_size: int = 1000, dry_run: bool = False) -> int:
        """Fetch and process data for one location-parameter combination"""
        try:
            # Rate limiting
            time.sleep(self.min_interval_sec)
            
            total_records = 0
            current_scroll_id = None
            page = 1
            
            for _ in range(max_pages):
                params = self._build_params(
                    location_id, page, current_scroll_id, param_set["measurement"], param_set["transaction_type"],
                    "aed", None, None, None, None, None, page_size
                )
                
                try:
                    payload = self.api_client.fetch_transaction_raw_sales(country_code, params)
                except Exception as e:
                    logger.warning(f"API call failed for location {location_id} with params {param_set}: {e}")
                    break
                
                results = payload.get("results") if isinstance(payload, dict) else None
                
                if isinstance(results, list) and results:
                    total_records += len(results)
                    
                    if not dry_run:
                        # Process the data
                        processed = DataProcessor.process_transaction_sales_data(
                            results, int(location_id), "aed", param_set["measurement"], param_set["transaction_type"],
                            None, None, None, None
                        )
                        
                        # Filter valid data and insert
                        valid_data = [record for record in processed if record.get('price') is not None]
                        if valid_data:
                            self.db.insert_transaction_sales_data(valid_data)
                            logger.info(f"Location {location_id}: {param_set['measurement']}_{param_set['transaction_type']} - {len(valid_data)} records")
                else:
                    break
                
                # Check for next page
                current_scroll_id = payload.get("scroll_id") if isinstance(payload, dict) else None
                page += 1
                if not current_scroll_id:
                    break
            
            return total_records
                
        except Exception as e:
            logger.warning(f"Failed to fetch data for location {location_id} with params {param_set}: {e}")
            return 0

    def process_transaction_sales_batch(
        self,
        country_code: str = "AE",
        max_locations: int = 500,
        offset: int = 0,
        max_pages: int = 5,
        page_size: int = 1000,
        dry_run: bool = False
    ) -> None:
        """Main method to process transaction sales data with batch processing and database storage"""
        
        logger.info("🚀 Starting Transaction Sales data import with batch processing")
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
                executor.submit(self._fetch_and_process_one, location_id, param_set, country_code, max_pages, page_size, dry_run): (location_id, param_set)
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
        logger.info("🎉 Transaction Sales import completed!")
        logger.info(f"Total parameter combinations: {len(param_combinations)}")
        logger.info(f"Total locations processed: {len(location_ids)}")
        logger.info(f"Total API calls made: {len(all_tasks)}")
        logger.info(f"Successful requests: {successful_requests}")
        logger.info(f"Failed requests: {failed_requests}")
        if not dry_run:
            logger.info(f"Total records inserted into database: {total_records}")

    def process_transaction_sales(
        self,
        country_code: str,
        measurement: str = "imp",
        transaction_type: str = "Sales - Ready",
        currency: str = "aed",
        property_type: Optional[str] = None,
        bedroom: Optional[int] = None,
        size_range: Optional[str] = None,
        price_range: Optional[str] = None,
        transaction_date: Optional[str] = None,
        max_locations: int = 500,
        max_pages: int = 5,
        page_size: int = 1000,
        request_delay: float = 0.25,
        dry_run: bool = False
    ) -> None:
        """
        Process transaction sales data import
        
        Args:
            country_code: Country code (e.g., 'AE')
            measurement: Measurement unit (default: 'imp')
            transaction_type: Type of transaction (default: 'Sales - Ready')
            currency: Currency (default: 'aed')
            property_type: Property type filter
            bedroom: Number of bedrooms filter
            size_range: Size range filter
            price_range: Price range filter
            transaction_date: Transaction date filter
            max_locations: Maximum number of locations to process
            max_pages: Maximum pages per location
            page_size: Records per page
            request_delay: Delay between API calls
            dry_run: If True, don't write to database
        """
        logger.info(f"Starting transaction sales import for: {country_code}")
        logger.info(f"Parameters: max_locations={max_locations}, max_pages={max_pages}")
        
        try:
            # Get locations from database
            db_rows = self.db.get_locations(country_code=country_code, limit=max_locations)
            if not db_rows:
                db_rows = self.db.get_locations(country_code=None, limit=max_locations)
                if db_rows:
                    logger.info(f"No DB rows for COUNTRY_CODE='{country_code}'. Using {len(db_rows)} locations without country filter.")
            
            location_ids = [str(r.get("location_id")) for r in db_rows if r.get("location_id") is not None]
            
            if not location_ids:
                logger.error("No location ids found in DB; exiting.")
                return
            
            logger.info(f"Processing {len(location_ids)} locations")
            
            all_processed_data = []
            locations_with_data = set()
            
            with self.api_client as client:
                for loc_id in location_ids:
                    total_for_loc = 0
                    current_scroll_id = None
                    page = 1
                    
                    for _ in range(max_pages):
                        params = self._build_params(
                            loc_id, page, current_scroll_id, measurement, transaction_type,
                            currency, property_type, bedroom, size_range, price_range,
                            transaction_date, page_size
                        )
                        
                        try:
                            payload = client.fetch_transaction_raw_sales(country_code, params)
                        except Exception as e:
                            logger.error(f"API call failed for location {loc_id}: {e}")
                            break
                        
                        time.sleep(request_delay)
                        
                        results = payload.get("results") if isinstance(payload, dict) else None
                        
                        # Log progress
                        if isinstance(results, list):
                            preview = str(results[:1])[:200] if results else "[]"
                            logger.info(f"location={loc_id} page={page} results_count={len(results)} preview={preview}")
                        else:
                            preview = str(payload)[:200] if payload else "None"
                            logger.info(f"location={loc_id} page={page} results_count=0 preview={preview}")
                        
                        if isinstance(results, list) and results:
                            locations_with_data.add(loc_id)
                            total_for_loc += len(results)
                            
                            # Process the data
                            processed = DataProcessor.process_transaction_sales_data(
                                results, int(loc_id), currency, measurement, transaction_type,
                                bedroom, size_range, price_range, transaction_date
                            )
                            all_processed_data.extend(processed)
                        else:
                            break
                        
                        # Check for next page
                        current_scroll_id = payload.get("scroll_id") if isinstance(payload, dict) else None
                        page += 1
                        if not current_scroll_id:
                            break
                    
                    logger.info(f"location={loc_id} total_results_accumulated={total_for_loc}")
            
            logger.info(f"Processing {len(all_processed_data)} total rows")
            
            # Filter out records with no actual data
            valid_data = [record for record in all_processed_data if record.get('price') is not None]
            logger.info(f"Found {len(valid_data)} rows with actual data")
            
            if not dry_run and valid_data:
                # Save to database
                self.db.insert_transaction_sales_data(valid_data)
                logger.info("Transaction sales import completed")
            else:
                logger.info("Dry run: no database write performed.")
            
            logger.info(f"Locations with data (non-empty results): {len(locations_with_data)}")
            
        except Exception as e:
            logger.error(f"Transaction sales import failed: {e}")
            raise
    
    def _build_params(
        self,
        location_id: str,
        page_number: int,
        scroll_id: Optional[str],
        measurement: str,
        transaction_type: str,
        currency: str,
        property_type: Optional[str],
        bedroom: Optional[int],
        size_range: Optional[str],
        price_range: Optional[str],
        transaction_date: Optional[str],
        page_size: int
    ) -> Dict[str, Any]:
        """Build API parameters"""
        params = {
            "measurement": measurement,
            "transaction_type": transaction_type,
            "page_size": page_size,
            "location": location_id,
            "page_number": page_number
        }
        
        if currency:
            params["currency"] = currency
        if property_type:
            params["property_type"] = property_type
        if bedroom is not None:
            params["bedroom"] = bedroom
        if size_range:
            params["size_range"] = size_range
        if price_range:
            params["price_range"] = price_range
        if transaction_date:
            params["transaction_date"] = transaction_date
        if scroll_id:
            params["scroll_id"] = scroll_id
        
        return params


def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(description="Import transaction sales data from Reidin API")
    parser.add_argument("--country-code", default="AE", help="Country code (default: AE)")
    parser.add_argument("--max-locations", type=int, default=500, help="Maximum locations to process (default: 500)")
    parser.add_argument("--max-workers", type=int, default=8, help="Maximum number of worker threads (default: 8)")
    parser.add_argument("--requests-per-second", type=float, default=4.0, help="Maximum requests per second (default: 4.0)")
    parser.add_argument("--batch-mode", action="store_true", help="Run in batch processing mode (parallel processing for faster data dump)")
    parser.add_argument("--offset", type=int, default=0, help="Number of locations to skip from the beginning (default: 0)")
    parser.add_argument("--max-pages", type=int, default=5, help="Maximum pages per location (default: 5)")
    parser.add_argument("--page-size", type=int, default=1000, help="Records per page (default: 1000)")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to database")
    
    args = parser.parse_args()
    
    importer = TransactionSalesImporter()
    
    # Override settings from command line
    importer.max_workers = args.max_workers
    importer.requests_per_second = args.requests_per_second
    importer.min_interval_sec = 1.0 / args.requests_per_second
    
    if args.batch_mode:
        importer.process_transaction_sales_batch(
            country_code=args.country_code,
            max_locations=args.max_locations,
            offset=args.offset,
            max_pages=args.max_pages,
            page_size=args.page_size,
            dry_run=args.dry_run
        )
    else:
        importer.process_transaction_sales(
            country_code=args.country_code,
            max_locations=args.max_locations,
            max_pages=args.max_pages,
            page_size=args.page_size,
            dry_run=args.dry_run
        )


if __name__ == "__main__":
    main()
