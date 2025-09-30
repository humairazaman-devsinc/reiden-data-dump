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
        logging.FileHandler('cma2_rents_import.log')
    ]
)
logger = logging.getLogger(__name__)


class CMA2RentsImporter:
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
        
        self.aliases = ["last-fifteen"] 
        self.currencies = ["aed"]  
        self.measurements = ["int"] 
        self.property_types = ["Office"]
        self.property_subtypes = [
            "Medical Office", "Office", "Whole Building" ] 

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
            self.db.insert_cma2_rents_data(batch)
            
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

    def get_properties(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Get properties from the database with optional offset for batch processing"""
        try:
            properties = self.db.get_properties(limit=limit, offset=offset)
            logger.info(f"Retrieved {len(properties)} properties from database (offset: {offset})")
            return properties
        except Exception as e:
            logger.error(f"Failed to get properties: {e}")
            return []

    def get_processed_property_ids(self) -> set:
        """Get set of property IDs that have already been processed with current parameter combinations (for resume capability)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check for properties that have been processed with current parameter combinations
                    cur.execute('''
                        SELECT DISTINCT property_id 
                        FROM cma2_rents 
                        WHERE alias = %s 
                        AND currency = %s 
                        AND measurement = %s 
                        AND property_type = %s 
                        AND property_subtype = ANY(%s)
                    ''', (
                        self.aliases[0],  # last-fifteen
                        self.currencies[0],  # aed
                        self.measurements[0],  # int
                        self.property_types[0],  # Office
                        self.property_subtypes  # ['Medical Office', 'Office']
                    ))
                    processed_ids = {row[0] for row in cur.fetchall()}
                    logger.info(f"Found {len(processed_ids)} already processed property IDs with current combinations")
                    return processed_ids
        except Exception as e:
            logger.error(f"Failed to get processed property IDs: {e}")
            return set()

    def generate_parameter_combinations(self) -> List[Dict[str, Any]]:
        """Generate all parameter combinations for CMA2-rents API calls"""
        combinations = []
        
        for alias in self.aliases:
            for currency in self.currencies:
                for measurement in self.measurements:
                    for property_type in self.property_types:
                        for property_subtype in self.property_subtypes:
                            combinations.append({
                                'alias': alias,
                                'currency': currency,
                                'measurement': measurement,
                                'property_type': property_type,
                                'property_subtype': property_subtype
                            })
        
        logger.info(f"Generated {len(combinations)} parameter combinations")
        return combinations

    def process_single_property(self, property_data: Dict[str, Any], max_combinations_per_property: int = 10) -> int:
        """Process a single property with all parameter combinations"""
        property_id = property_data['id']
        records_processed = 0
        combinations_tried = 0
        
        # Generate all combinations of REQUIRED parameters only
        # These are the 5 mandatory parameters that MUST be provided
        for alias in self.aliases:
            for currency in self.currencies:
                for measurement in self.measurements:
                    for property_type in self.property_types:
                        for property_subtype in self.property_subtypes:
                            if combinations_tried >= max_combinations_per_property:
                                break
                            
                            # Only REQUIRED parameters - no optional parameters
                            try:
                                # Make rate-limited API call
                                response_data = self._rate_limited_request(
                                    self.api_client.fetch_cma2_rents,
                                    country_code="AE",
                                    property_id=property_id,
                                    alias=alias,
                                    currency=currency,
                                    measurement=measurement,
                                    property_type=property_type,
                                    property_subtype=property_subtype
                                )
                                
                                if response_data:
                                    # Process the response data (already JSON, not response object)
                                    processed_records = self.processor.process_cma2_rents_response(
                                        response_data,
                                        property_id=property_id,
                                        alias=alias,
                                        currency=currency,
                                        measurement=measurement,
                                        property_type=property_type,
                                        property_subtype=property_subtype
                                    )
                                    
                                    # Add processed records to queue
                                    for record in processed_records:
                                        self.data_queue.put(record)
                                    
                                    records_processed += len(processed_records)
                                
                                combinations_tried += 1
                
        except Exception as e:
                                logger.warning(f"Failed to process property {property_id} with combination {alias}/{currency}/{measurement}/{property_type}/{property_subtype}: {e}")
                                combinations_tried += 1
                                continue
                        
                        if combinations_tried >= max_combinations_per_property:
                            break
                    if combinations_tried >= max_combinations_per_property:
                        break
                if combinations_tried >= max_combinations_per_property:
                    break
            if combinations_tried >= max_combinations_per_property:
                break
        
        return records_processed

    def process_cma2_rents(
        self, 
        country_code: str = "AE",
        max_properties: int = 500,
        max_combinations_per_property: int = 10,
        request_delay: float = 0.25,
        dry_run: bool = False,
        offset: int = 0
    ) -> None:
        """Main method to process CMA2-rents data"""
        logger.info("🚀 Starting CMA2-Rents data import with new architecture")
        logger.info(f"Country: {country_code}")
        logger.info(f"Max properties: {max_properties}")
        logger.info(f"Offset: {offset}")
        logger.info(f"Max combinations per property: {max_combinations_per_property}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Request delay: {request_delay}s")
        logger.info(f"Dry run: {dry_run}")
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be inserted")
            return
        
        # Get properties from database
        properties = self.get_properties(limit=max_properties, offset=offset)
        if not properties:
            logger.error("No properties found")
            return
        
        # Get already processed property IDs for resume capability
        processed_property_ids = self.get_processed_property_ids()
        
        # Filter out already processed properties
        remaining_properties = [p for p in properties if p['id'] not in processed_property_ids]
        
        if not remaining_properties:
            logger.info("✅ All properties have already been processed with current combinations")
            return
        
        logger.info(f"🔄 Resume mode: Skipped {len(properties) - len(remaining_properties)} already processed tasks, {len(remaining_properties)} remaining")
        
        # Start flusher threads
        self.start_flusher_threads()
        
        try:
            # Process properties in parallel
            logger.info(f"🔄 Starting parallel API processing with {self.max_workers} workers...")
            
            total_records = 0
            successful_tasks = 0
            failed_tasks = 0
            
            # Process in batches to control memory usage
            batch_size = min(40, len(remaining_properties))  # Max 40 futures per batch
            
            for i in range(0, len(remaining_properties), batch_size):
                batch = remaining_properties[i:i + batch_size]
                logger.info(f"📦 Processing batch {i//batch_size + 1}/{(len(remaining_properties) + batch_size - 1)//batch_size} ({len(batch)} properties)")
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit tasks with semaphore control
                    futures = []
                    for property_data in batch:
                        with self.task_semaphore:
                            future = executor.submit(
                                self.process_single_property, 
                                property_data, 
                                max_combinations_per_property
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
                processed_so_far = min(i + batch_size, len(remaining_properties))
                logger.info(f"📊 Progress: {processed_so_far}/{len(remaining_properties)} properties processed")
                logger.info(f"📊 Queue size: {self.data_queue.qsize()}")
            
            # Wait for all data to be flushed
            logger.info("🔄 Stopping flusher thread and waiting for final flush...")
            self.stop_flusher_threads()
            
            # Final statistics
            logger.info("🎉 CMA2-Rents import completed!")
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
    parser = argparse.ArgumentParser(description="CMA2-Rents ETL with modern architecture")
    
    # Processing options
    parser.add_argument(
        "--max-properties", 
        type=int, 
        default=500, 
        help="Maximum properties to process (default: 500)"
    )
    parser.add_argument(
        "--max-combinations", 
        type=int, 
        default=10, 
        help="Maximum combinations per property (default: 10)"
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
        help="Offset for property processing (for batch processing)"
    )
    
    args = parser.parse_args()
    
    # Create importer and run
    importer = CMA2RentsImporter(
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        request_delay=args.request_delay
    )
    
    try:
        importer.process_cma2_rents(
            country_code="AE",
            max_properties=args.max_properties,
            max_combinations_per_property=args.max_combinations,
            request_delay=args.request_delay,
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
