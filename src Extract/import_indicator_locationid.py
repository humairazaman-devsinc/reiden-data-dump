import argparse
import logging
import sys
import time
from pathlib import Path
from queue import Queue, Empty
from threading import Thread, Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        logging.FileHandler('indicator_location_id_import.log')
    ]
)
logger = logging.getLogger(__name__)


class IndicatorLocationIDImporter:
    def __init__(self, batch_size: int = 10000, max_workers: int = 10, request_delay: float = 0.1):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
        self.processor = DataProcessor()
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.request_delay = request_delay
        
        # Asynchronous queue for decoupling API fetching from DB inserts
        self.data_queue = Queue(maxsize=100000)  # Increased queue size for better throughput
        
        # Flusher threads control - increased for better DB throughput
        self.flusher_threads = []
        self.num_flushers = 8  # Increased for better DB performance
        
        # Global rate limiter for API calls (prevents 429 errors)
        self.rate_limiter = Semaphore(max_workers)  # Allow multiple concurrent requests
        self.last_request_time = 0
        
        # Statistics
        self.total_records_inserted = 0
        self.total_batches_flushed = 0
        self.total_api_errors = 0
        
        # Default parameters for indicator location ID API - optimized for speed
        self.currency = "aed"
        self.measurement = "int"
        self.page_size = 200  # Increased page size for fewer API calls
        self.sort = "desc"
        self.start_date = None
        self.end_date = None
        self.alias = None

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
        """Worker thread that continuously flushes data to database - optimized for speed"""
        batch = []
        batch_start_time = time.time()
        
        while True:
            try:
                # Get data from queue with shorter timeout for faster flushing
                item = self.data_queue.get(timeout=0.5)
                
                # Check for sentinel value (shutdown signal)
                if item is None:
                    # Flush any remaining data before stopping
                    if batch:
                        self._flush_batch_to_db(batch)
                    break
                
                batch.append(item)
                
                # Flush batch when it reaches batch_size or after shorter timeout
                if len(batch) >= self.batch_size or (time.time() - batch_start_time) > 2.0:
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
            self.db.insert_indicator_location_id_data(batch)
            
            self.total_records_inserted += len(batch)
            self.total_batches_flushed += 1
            
            elapsed = time.time() - start_time
            rate = len(batch) / elapsed if elapsed > 0 else 0
            
            # Only log every 10th batch to reduce logging overhead
            if self.total_batches_flushed % 10 == 0:
                logger.info(f"✅ Flushed {len(batch)} records in {elapsed:.2f}s ({rate:.1f} rec/s) (Total: {self.total_records_inserted} records, {self.total_batches_flushed} batches)")
            
        except Exception as e:
            logger.error(f"Failed to flush batch to database: {e}")


    def _rate_limited_request(self, func, *args, **kwargs):
        """Make a rate-limited API request - only process 200 responses, skip others"""
        try:
            with self.rate_limiter:
                # Minimal delay for rate limiting
                if self.request_delay > 0:
                    time.sleep(self.request_delay)
                
                # Make the API call
                result = func(*args, **kwargs)
                
                # Only return result if it's a successful response
                if result is not None:
                    return result
                else:
                    return None
                    
        except Exception as e:
            self.total_api_errors += 1
            # Log error but continue processing
            logger.debug(f"API call failed: {e} - skipping")
            return None

    def get_locations(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Get locations from the database with optional offset for batch processing"""
        try:
            locations = self.db.get_locations(limit=limit, offset=offset)
            logger.info(f"Retrieved {len(locations)} locations from database (offset: {offset})")
            return locations
        except Exception as e:
            logger.error(f"Failed to get locations: {e}")
            return []

    def get_processed_location_ids(self) -> set:
        """Get set of location IDs that have already been processed with current parameter combinations (for resume capability)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check for locations that have been processed with current parameter combinations
                    cur.execute('''
                        SELECT DISTINCT location_id 
                        FROM indicator_location 
                        WHERE currency = %s 
                        AND measurement = %s
                    ''', (
                        self.currency,
                        self.measurement
                    ))
                    processed_ids = {row[0] for row in cur.fetchall()}
                    logger.info(f"Found {len(processed_ids)} already processed location IDs with current combinations")
                    return processed_ids
        except Exception as e:
            logger.error(f"Failed to get processed location IDs: {e}")
            return set()

    def process_single_location(self, location_data: Dict[str, Any], max_pages_per_location: int = 10) -> int:
        """Process a single location with pagination support - skip failed requests and continue"""
        location_id = location_data['location_id']
        records_processed = 0
        pages_processed = 0
        
        # Process with pagination - start from page 1
        for page_number in range(1, max_pages_per_location + 1):
            try:
                # Make rate-limited API call
                response_data = self._rate_limited_request(
                    self.api_client.fetch_indicator_location_id,
                    country_code="AE",
                    location_id=location_id,
                    currency=self.currency,
                    measurement=self.measurement,
                    page_number=page_number,
                    page_size=self.page_size,
                    sort=self.sort,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    alias=self.alias
                )
                
                # Skip if API call failed (None response)
                if response_data is None:
                    logger.warning(f"⚠️ Skipping location {location_id} page {page_number} - API call failed")
                    continue  # Skip this page but continue with next page
                
                # Only process if we have valid data - check both 'results' and 'data' fields
                if response_data and (response_data.get('results') or response_data.get('data')):
                    try:
                        # Process the response data
                        processed_records = self.processor.process_indicator_location_id_response(
                            response_data,
                            location_id=location_id,
                            currency=self.currency,
                            measurement=self.measurement,
                            page_number=page_number
                        )
                        
                        # Add processed records to queue
                        for record in processed_records:
                            self.data_queue.put(record)
                        
                        records_processed += len(processed_records)
                        pages_processed += 1
                        
                        logger.debug(f"✅ Location {location_id} page {page_number}: {len(processed_records)} records processed")
                        
                        # If no more results, break pagination loop
                        results_data = response_data.get('results', []) or response_data.get('data', [])
                        if len(results_data) == 0:
                            break
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to process data for location {location_id} page {page_number}: {e} - skipping")
                        continue  # Skip this page but continue with next page
                else:
                    # No more data, break pagination loop
                    break
            
            except Exception as e:
                logger.warning(f"⚠️ Error processing location {location_id} page {page_number}: {e} - skipping page")
                continue  # Skip this page but continue with next page
        
        logger.info(f"✅ Location {location_id}: {pages_processed} pages, {records_processed} records processed")
        return records_processed

    def process_indicator_location_id(
        self, 
        country_code: str = "AE",
        max_locations: int = 500,
        max_pages_per_location: int = 10,
        request_delay: float = 0.25,
        dry_run: bool = False,
        offset: int = 0
    ) -> None:
        """Main method to process indicator location ID data"""
        logger.info("🚀 Starting Indicator Location ID data import with new architecture")
        logger.info(f"Country: {country_code}")
        logger.info(f"Max locations: {max_locations}")
        logger.info(f"Offset: {offset}")
        logger.info(f"Max pages per location: {max_pages_per_location}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Request delay: {request_delay}s")
        logger.info(f"Dry run: {dry_run}")
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be inserted")
            return
        
        # Get locations from database
        locations = self.get_locations(limit=max_locations, offset=offset)
        if not locations:
            logger.error("No locations found")
            return
        
        # Get already processed location IDs for resume capability
        processed_location_ids = self.get_processed_location_ids()
        
        # Filter out already processed locations
        remaining_locations = [l for l in locations if l['location_id'] not in processed_location_ids]
        
        if not remaining_locations:
            logger.info("✅ All locations have already been processed with current combinations")
            return
        
        logger.info(f"🔄 Resume mode: Skipped {len(locations) - len(remaining_locations)} already processed tasks, {len(remaining_locations)} remaining")
        
        # Start flusher threads
        self.start_flusher_threads()
        
        try:
            # Process locations in parallel for maximum speed
            logger.info(f"🚀 Starting parallel API processing with {self.max_workers} workers...")
            
            total_records = 0
            successful_tasks = 0
            
            # Process locations in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all location processing tasks
                future_to_location = {
                    executor.submit(self.process_single_location, location_data, max_pages_per_location): location_data
                    for location_data in remaining_locations
                }
                
                # Process completed tasks as they finish
                completed_count = 0
                for future in as_completed(future_to_location):
                    location_data = future_to_location[future]
                    try:
                        records = future.result()
                        total_records += records
                        successful_tasks += 1
                        completed_count += 1
                        
                        # Log progress every 10 locations for better performance
                        if completed_count % 10 == 0 or completed_count == len(remaining_locations):
                            logger.info(f"📊 Progress: {completed_count}/{len(remaining_locations)} locations processed")
                            logger.info(f"📊 Queue size: {self.data_queue.qsize()}")
                            logger.info(f"📊 API errors: {self.total_api_errors}")
                            logger.info(f"📊 Records processed so far: {total_records}")
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Location {location_data['location_id']} failed: {e}")
                        completed_count += 1
            
            # Wait for all data to be flushed
            logger.info("🔄 Stopping flusher thread and waiting for final flush...")
            self.stop_flusher_threads()
            
            # Final statistics
            logger.info("🎉 Indicator Location ID import completed!")
            logger.info(f"Total locations processed: {successful_tasks}")
            logger.info(f"Total API errors encountered: {self.total_api_errors}")
            logger.info(f"Total records processed: {total_records}")
            logger.info(f"Total records inserted: {self.total_records_inserted}")
            logger.info(f"Total batches flushed: {self.total_batches_flushed}")
            logger.info(f"Average records per batch: {self.total_records_inserted / max(1, self.total_batches_flushed):.1f}")
            
            # Performance metrics
            if successful_tasks > 0:
                avg_records_per_location = total_records / successful_tasks
                logger.info(f"Average records per location: {avg_records_per_location:.1f}")
            
            if self.total_api_errors > 0:
                error_rate = (self.total_api_errors / (successful_tasks + self.total_api_errors)) * 100
                logger.info(f"API error rate: {error_rate:.1f}%")
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            self.stop_flusher_threads()
            raise


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Indicator Location ID ETL with modern architecture")
    
    # Processing options
    parser.add_argument(
        "--max-locations", 
        type=int, 
        default=500, 
        help="Maximum locations to process (default: 500)"
    )
    parser.add_argument(
        "--max-pages", 
        type=int, 
        default=10, 
        help="Maximum pages per location (default: 10)"
    )
    parser.add_argument(
        "--request-delay", 
        type=float, 
        default=0.1, 
        help="Delay between API requests in seconds (default: 0.1)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=10000, 
        help="Database batch size (default: 10000)"
    )
    parser.add_argument(
        "--max-workers", 
        type=int, 
        default=10, 
        help="Maximum number of worker threads (default: 10)"
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
        "--currency", 
        type=str, 
        default="aed", 
        help="Currency for API requests (default: aed)"
    )
    parser.add_argument(
        "--measurement", 
        type=str, 
        default="int", 
        help="Measurement unit for API requests (default: int)"
    )
    parser.add_argument(
        "--page-size", 
        type=int, 
        default=200, 
        help="Number of records per API page (default: 200)"
    )
    
    args = parser.parse_args()
    
    # Create importer and run
    importer = IndicatorLocationIDImporter(
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        request_delay=args.request_delay
    )
    
    # Update currency, measurement, and page size
    importer.currency = args.currency
    importer.measurement = args.measurement
    importer.page_size = args.page_size
    
    try:
        importer.process_indicator_location_id(
            country_code="ae",
            max_locations=args.max_locations,
            max_pages_per_location=args.max_pages,
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
