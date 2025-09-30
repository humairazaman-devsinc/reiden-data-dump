import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from queue import Queue, Empty
from threading import Thread, Semaphore
from typing import Dict, List, Any, Optional

# Ensure the parent directory is on sys.path when running this file directly
PARENT_DIR = Path(__file__).resolve().parents[1]
if str(PARENT_DIR) not in sys.path:
    sys.path.append(str(PARENT_DIR))

from api_client import ReidinAPIClient
from database import DatabaseManager
from processors import DataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('transactions_avg_import.log')
    ]
)
logger = logging.getLogger(__name__)


class TransactionsAvgImporter:
    def __init__(self, batch_size: int = 5000, max_workers: int = 8, request_delay: float = 1.0):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
        self.processor = DataProcessor()
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.request_delay = request_delay
        
        # Asynchronous queue for decoupling API fetching from DB inserts (with backpressure)
        self.data_queue = Queue(maxsize=200000)  # Increased for better throughput - prevents backpressure
        
        # Flusher threads control (multiple flushers for better DB throughput)
        self.flusher_threads = []
        self.num_flushers = 6  # Increased for M3 Mac performance (6-8 flushers)
        
        # Bounded semaphore for controlled task submission
        self.task_semaphore = Semaphore(max_workers * 50)  # Balanced limit for sliding window
        
        # Global rate limiter for API calls (prevents 429 errors)
        self.rate_limiter = Semaphore(1)  # Global rate limiting
        self.last_request_time = 0
        
        # Statistics
        self.total_records_inserted = 0
        self.total_batches_flushed = 0
        
        # Parameter combinations for transactions average endpoint
        self.property_types = ["Apartment", "Villa", "Office", "Serviced/Hotel Apartment"]
        self.activity_types = ["sales", "off-plan", "mortgage", "off-plan-mortgage"]
        self.currencies = ["aed"]
        self.measurements = ["int"]

    def start_flusher_threads(self):
        """Start multiple dedicated flusher threads for better DB throughput"""
        for i in range(self.num_flushers):
            flusher = Thread(target=self._flusher_worker, daemon=True, name=f"Flusher-{i+1}")
            flusher.start()
            self.flusher_threads.append(flusher)
        logger.info(f"🔄 Started {self.num_flushers} dedicated flusher threads")

    def stop_flusher_threads(self):
        """Stop all flusher threads gracefully"""
        logger.info("🔄 Stopping flusher threads...")
        for _ in self.flusher_threads:
            self.data_queue.put(None)  # Sentinel value to stop flusher
        
        for flusher in self.flusher_threads:
            flusher.join(timeout=30)  # Wait up to 30 seconds
        logger.info("✅ All flusher threads stopped")

    def _flusher_worker(self):
        """Worker thread that continuously flushes data to database"""
        batch = []
        batch_start_time = time.time()
        
        while True:
            try:
                # Get data from queue with timeout
                item = self.data_queue.get(timeout=1.0)
                
                # Check for sentinel value (shutdown signal)
                if item is None:
                    # Flush any remaining data before stopping
                    if batch:
                        self._flush_batch_to_db(batch)
                    logger.info("✅ Flusher worker completed")
                    break
                
                batch.append(item)
                
                # Flush batch when it reaches batch_size or after timeout
                if len(batch) >= self.batch_size or (time.time() - batch_start_time) > 5.0:
                    self._flush_batch_to_db(batch)
                    batch = []
                    batch_start_time = time.time()
                
                self.data_queue.task_done()
                
            except Empty:
                # Timeout - flush any accumulated data
                if batch:
                    self._flush_batch_to_db(batch)
                    batch = []
                    batch_start_time = time.time()
            except Exception as e:
                logger.error(f"Flusher worker error: {e}")
                if batch:
                    self._flush_batch_to_db(batch)
                    batch = []

    def _flush_batch_to_db(self, batch: List[Dict[str, Any]]):
        """Flush a batch of records to the database"""
        if not batch:
            return
        
        try:
            start_time = time.time()
            self.db.insert_transactions_avg_data(batch)
            
            self.total_records_inserted += len(batch)
            self.total_batches_flushed += 1
            
            elapsed = time.time() - start_time
            rate = len(batch) / elapsed if elapsed > 0 else 0
            
            logger.info(f"✅ Successfully inserted {len(batch)} records in {elapsed:.2f}s ({rate:.1f} rec/s) (Total: {self.total_records_inserted} records, {self.total_batches_flushed} batches)")
            
        except Exception as e:
            logger.error(f"Failed to flush batch to database: {e}")

    def _rate_limited_request(self, func, *args, **kwargs):
        """Make a rate-limited API request"""
        with self.rate_limiter:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.request_delay:
                sleep_time = self.request_delay - time_since_last
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
            return func(*args, **kwargs)

    def get_locations(self, country_code: str, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Get locations from the database with optional offset for batch processing"""
        try:
            locations = self.db.get_locations(country_code, limit, offset)
            logger.info(f"Retrieved {len(locations)} locations from database (offset: {offset})")
            return locations
        except Exception as e:
            logger.error(f"Failed to get locations: {e}")
            return []

    def get_processed_location_combinations(self) -> set:
        """Get set of location/parameter combinations that have already been processed (for resume capability)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check for location/parameter combinations that have been processed
                    # Note: no_of_bedrooms is NULL in our current implementation
                    cur.execute('''
                        SELECT DISTINCT location_id, property_type, activity_type, currency, measurement, no_of_bedrooms
                        FROM transactions_avg 
                        WHERE location_id IS NOT NULL
                    ''')
                    processed_combinations = {tuple(row) for row in cur.fetchall()}
                    logger.info(f"Found {len(processed_combinations)} already processed location/parameter combinations")
                    return processed_combinations
        except Exception as e:
            logger.error(f"Failed to get processed location combinations: {e}")
            return set()

    def process_single_location(self, location_data: Dict[str, Any], max_combinations_per_location: int = 10, processed_combinations: set = None) -> int:
        """Process a single location with all parameter combinations"""
        location_id = location_data['location_id']
        records_processed = 0
        combinations_tried = 0
        
        if processed_combinations is None:
            processed_combinations = set()
        
        # Generate all combinations of parameters
        for property_type in self.property_types:
            for activity_type in self.activity_types:
                for currency in self.currencies:
                    for measurement in self.measurements:
                        if combinations_tried >= max_combinations_per_location:
                            break
                        
                        # Check if this specific combination has already been processed
                        combination_key = (location_id, property_type, activity_type, currency, measurement, None)  # None for no_of_bedrooms
                        if combination_key in processed_combinations:
                            logger.debug(f"Skipping already processed combination: {location_id}/{property_type}/{activity_type}/{currency}/{measurement}")
                            combinations_tried += 1
                            continue
                        
                        try:
                            # Build parameters for the API call
                            params = {
                                "property_type": property_type,
                                "activity_type": activity_type,
                                "currency": currency,
                                "measurement": measurement,
                                "location_id": str(location_id),
                            }

                            # Make rate-limited API call
                            response_data = self._rate_limited_request(
                                self.api_client.fetch_transactions_avg,
                                "AE",  # country_code
                                params
                            )
                            
                            if response_data and isinstance(response_data, dict) and "results" in response_data:
                                # Process the response data
                                processed_records = self.processor.process_transactions_avg_data(
                                    response_data.get("results", []),
                                    location_id=location_id,
                                    property_type=property_type,
                                    activity_type=activity_type,
                                    currency=currency,
                                    measurement=measurement
                                )
                                
                                # Add processed records to queue
                                for record in processed_records:
                                    self.data_queue.put(record)
                                
                                records_processed += len(processed_records)
                            
                            combinations_tried += 1
                            
                        except Exception as e:
                            logger.warning(f"Failed to process location {location_id} with combination {property_type}/{activity_type}/{currency}/{measurement}: {e}")
                            combinations_tried += 1
                            continue
                if combinations_tried >= max_combinations_per_location:
                    break
            if combinations_tried >= max_combinations_per_location:
                break
        
        return records_processed

    def process_transactions_avg(
        self, 
        country_code: str = "AE",
        max_locations: int = 500,
        max_combinations_per_location: int = 10,
        dry_run: bool = False,
        offset: int = 0
    ) -> None:
        """Main method to process transactions average data"""
        logger.info("🚀 Starting Transactions Average data import with new architecture")
        logger.info(f"Country: {country_code}")
        logger.info(f"Max locations: {max_locations}")
        logger.info(f"Offset: {offset}")
        logger.info(f"Max combinations per location: {max_combinations_per_location}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Request delay: {self.request_delay}s")
        logger.info(f"Dry run: {dry_run}")
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be inserted")
            return
        
        # Get locations from database
        locations = self.get_locations(country_code, limit=max_locations, offset=offset)
        if not locations:
            logger.error("No locations found")
            return
        
        # Get already processed location combinations for resume capability
        processed_combinations = self.get_processed_location_combinations()
        
        # Filter out already processed location/parameter combinations
        remaining_locations = []
        total_combinations = len(self.property_types) * len(self.activity_types) * len(self.currencies) * len(self.measurements)
        
        for location in locations:
            location_id = location['location_id']
            # Check how many combinations have been processed for this location
            location_combinations = [combo for combo in processed_combinations if combo[0] == location_id]
            
            if len(location_combinations) < total_combinations:
                # This location still has unprocessed combinations
                remaining_locations.append(location)
        
        if not remaining_locations:
            logger.info("✅ All locations have already been processed with all parameter combinations")
            return

        total_processed_combinations = sum(len([combo for combo in processed_combinations if combo[0] == loc['location_id']]) for loc in locations)
        total_possible_combinations = len(locations) * total_combinations
        
        logger.info(f"🔄 Resume mode: {total_processed_combinations}/{total_possible_combinations} combinations already processed")
        logger.info(f"🔄 Processing {len(remaining_locations)} locations with remaining combinations")
        
        # Start flusher threads
        self.start_flusher_threads()
        
        try:
            # Process locations in parallel
            logger.info(f"🔄 Starting parallel API processing with {self.max_workers} workers...")
            
            total_records = 0
            successful_tasks = 0
            failed_tasks = 0
            
            # Process in batches to control memory usage
            batch_size = min(40, len(remaining_locations))  # Max 40 futures per batch
            
            for i in range(0, len(remaining_locations), batch_size):
                batch = remaining_locations[i:i + batch_size]
                logger.info(f"📦 Processing batch {i//batch_size + 1}/{(len(remaining_locations) + batch_size - 1)//batch_size} ({len(batch)} locations)")
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit tasks with semaphore control
                    futures = []
                    for location_data in batch:
                        with self.task_semaphore:
                            future = executor.submit(
                                self.process_single_location, 
                                location_data, 
                                max_combinations_per_location,
                                processed_combinations
                            )
                            futures.append(future)
                    
                    # Process completed tasks
                    for future in as_completed(futures):
                        try:
                            records = future.result()
                            total_records += records
                            successful_tasks += 1
                        except Exception as e:
                            logger.error(f"Task failed: {e}")
                            failed_tasks += 1
                    
                    # Log progress
                processed_so_far = min(i + batch_size, len(remaining_locations))
                logger.info(f"📊 Progress: {processed_so_far}/{len(remaining_locations)} locations processed")
                logger.info(f"📊 Queue size: {self.data_queue.qsize()}")
            
            # Wait for all data to be flushed
            logger.info("🔄 Stopping flusher thread and waiting for final flush...")
            self.stop_flusher_threads()
            
            # Final statistics
            logger.info("🎉 Transactions Average import completed!")
            logger.info(f"Total API tasks: {successful_tasks + failed_tasks}")
            logger.info(f"Successful tasks: {successful_tasks}")
            logger.info(f"Failed tasks: {failed_tasks}")
            logger.info(f"Total records processed: {total_records}")
            logger.info(f"Total records inserted: {self.total_records_inserted}")
            logger.info(f"Total batches flushed: {self.total_batches_flushed}")
            logger.info(f"Average records per batch: {self.total_records_inserted / max(1, self.total_batches_flushed):.1f}")
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            self.stop_flusher_threads()
            raise


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Transactions Average ETL with modern architecture")
    
    # Processing options
    parser.add_argument(
        "--max-locations", 
        type=int, 
        default=500, 
        help="Maximum locations to process (default: 500)"
    )
    parser.add_argument(
        "--max-combinations", 
        type=int, 
        default=10, 
        help="Maximum combinations per location (default: 10)"
    )
    parser.add_argument(
        "--request-delay", 
        type=float, 
        default=0.25, 
        help="Delay between API requests in seconds (default: 0.25)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=5000, 
        help="Database batch size (default: 5000)"
    )
    parser.add_argument(
        "--max-workers", 
        type=int, 
        default=8, 
        help="Maximum number of worker threads (default: 8)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Run in dry-run mode (no data insertion)"
    )
    parser.add_argument(
        "--offset", 
        type=int, 
        default=0, 
        help="Offset for location processing (for batch processing)"
    )
    parser.add_argument(
        "--country-code", 
        type=str, 
        default="AE", 
        help="Country code (default: AE)"
    )
    
    args = parser.parse_args()

    # Create importer and run
    importer = TransactionsAvgImporter(
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        request_delay=args.request_delay
    )

    try:
        importer.process_transactions_avg(
            country_code=args.country_code,
            max_locations=args.max_locations,
            max_combinations_per_location=args.max_combinations,
            dry_run=args.dry_run,
            offset=args.offset
        )
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()