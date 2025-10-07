#!/usr/bin/env python3
"""
CMA Data Importer

This script imports CMA data from the Reidin API with dynamic schema handling.
It processes properties and fetches CMA data for each property with various parameter combinations.
Features dynamic column creation and nested field flattening.
"""

import argparse
import logging
import time
import json
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread, Semaphore
from queue import Queue, Empty, Full
from utils.database import DatabaseManager
from utils.api_client import ReidinAPIClient
from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("cma_import.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class CMAImporter:
    def __init__(
        self, batch_size: int = 5000, max_workers: int = 8, request_delay: float = 1.0
    ):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.request_delay = request_delay

        # Track discovered columns for dynamic schema
        self.discovered_columns = set()
        self.column_mapping = {}

        # Asynchronous queue for decoupling API fetching from DB inserts
        self.data_queue = Queue(maxsize=200000)

        # Flusher threads control
        self.flusher_threads = []
        self.num_flushers = 8

        # Bounded semaphore for controlled task submission
        self.task_semaphore = Semaphore(max_workers * 50)

        # Global rate limiter for API calls
        self.last_request_time = 0

        # Statistics tracking
        self.total_records_processed = 0
        self.total_records_inserted = 0
        self.total_batches_flushed = 0

        # Parameter combinations
        self.aliases = ["last-fifteen"]
        self.currencies = ["aed"]
        self.measurements = ["int"]
        self.property_types = [
            "Apartment",
            "Villa",
            "Office",
            "Serviced/Hotel Apartment",
        ]
        self.activity_types = ["sales", "off-plan", "mortgage", "off-plan-mortgage"]

    def _rate_limit(self):
        """Apply rate limiting to prevent API throttling"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()

    def _discover_schema(self, data: Dict[str, Any], prefix: str = "") -> Set[str]:
        """Recursively discover all fields in the response data"""
        columns = set()

        for key, value in data.items():
            column_name = f"{prefix}_{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively process nested objects
                nested_columns = self._discover_schema(value, column_name)
                columns.update(nested_columns)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Handle arrays of objects
                for item in value:
                    nested_columns = self._discover_schema(item, f"{column_name}_item")
                    columns.update(nested_columns)
            else:
                # Simple field
                columns.add(column_name)

        return columns

    def _flatten_data(self, data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Flatten nested data structure"""
        flattened = {}

        for key, value in data.items():
            column_name = f"{prefix}_{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively flatten nested objects
                nested_data = self._flatten_data(value, column_name)
                flattened.update(nested_data)
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    # Handle arrays of objects - flatten each item
                    for i, item in enumerate(value):
                        item_data = self._flatten_data(item, f"{column_name}_{i}")
                        flattened.update(item_data)
                else:
                    # Simple array - store as JSON string
                    flattened[column_name] = json.dumps(value) if value else None
            else:
                # Simple field
                flattened[column_name] = value

        return flattened

    def _fetch_cma_data(
        self,
        property_id: int,
        alias: str,
        currency: str,
        measurement: str,
        property_type: str,
        activity_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch CMA data for a specific property and parameters"""
        try:
            self._rate_limit()

            params = {
                "alias": alias,
                "currency": currency,
                "measurement": measurement,
                "property_type": property_type,
                "activity_type": activity_type,
                "property_id": property_id,
            }

            logger.info(
                f"Fetching CMA data for property {property_id} with params {params}"
            )

            response = self.api_client.fetch_cma_data(params)

            if response and response.get("data"):
                # Add metadata
                data = response["data"]
                data["property_id"] = property_id
                data["alias"] = alias
                data["currency"] = currency
                data["measurement"] = measurement
                data["property_type"] = property_type
                data["activity_type"] = activity_type
                data["raw_data"] = json.dumps(response)  # Store raw response

                # Discover new columns
                new_columns = self._discover_schema(data)
                self.discovered_columns.update(new_columns)

                return data
            else:
                logger.warning(
                    f"No data returned for property {property_id} with params {params}"
                )
                return None

        except Exception as e:
            logger.error(f"Error fetching CMA data for property {property_id}: {e}")
            return None

    def _process_cma_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and flatten CMA data"""
        if not data:
            return None

        try:
            # Flatten the data structure
            flattened = self._flatten_data(data)

            # Add processing metadata
            flattened["processed_at"] = time.time()
            flattened["import_batch_id"] = int(time.time())

            return flattened

        except Exception as e:
            logger.error(f"Error processing CMA data: {e}")
            return None

    def _api_worker(self, task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Worker function for API calls"""
        try:
            property_id = task["property_id"]
            alias = task["alias"]
            currency = task["currency"]
            measurement = task["measurement"]
            property_type = task["property_type"]
            activity_type = task["activity_type"]

            # Fetch data
            raw_data = self._fetch_cma_data(
                property_id, alias, currency, measurement, property_type, activity_type
            )

            if raw_data:
                # Process the data
                processed_data = self._process_cma_data(raw_data)
                return processed_data
            else:
                return None

        except Exception as e:
            logger.error(f"Error in API worker: {e}")
            return None
        finally:
            # Release semaphore
            self.task_semaphore.release()

    def _flusher_worker(self):
        """Dedicated worker thread that handles database inserts"""
        buffer = []
        last_flush_time = time.time()

        while True:
            try:
                # Get data from queue with timeout
                data = self.data_queue.get(timeout=1.0)

                # Check for poison pill
                if data is None:
                    break

                if isinstance(data, list):
                    buffer.extend(data)
                else:
                    buffer.append(data)

                # Flush if buffer is full
                if len(buffer) >= self.batch_size:
                    self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush_time = time.time()

            except Empty:
                # Timeout - check if we should flush based on time
                if buffer and (time.time() - last_flush_time) > 300:  # 5 minute timeout
                    self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush_time = time.time()
                continue

        # Flush any remaining data
        if buffer:
            self._flush_buffer(buffer)
            buffer.clear()
        logger.info("✅ Flusher worker completed")

    def _flush_buffer(self, buffer: List[Dict[str, Any]]):
        """Flush a buffer of records to the database"""
        if not buffer:
            return

        try:
            logger.info(f"🔄 Flushing batch of {len(buffer)} records to database...")
            start_time = time.time()

            # Insert data with dynamic schema
            self.db.insert_cma_data_dynamic(buffer, self.discovered_columns)

            end_time = time.time()
            duration = end_time - start_time

            self.total_records_inserted += len(buffer)
            self.total_batches_flushed += 1

            speed = len(buffer) / duration if duration > 0 else 0
            logger.info(
                f"✅ Successfully inserted {len(buffer)} records in {duration:.2f}s ({speed:.1f} rec/s) (Total: {self.total_records_inserted} records, {self.total_batches_flushed} batches)"
            )

        except Exception as e:
            logger.error(f"❌ Failed to flush batch of {len(buffer)} records: {e}")

    def _start_flusher_threads(self):
        """Start dedicated flusher threads"""
        self.flusher_threads = []
        for i in range(self.num_flushers):
            flusher_thread = Thread(target=self._flusher_worker, name=f"Flusher-{i}")
            flusher_thread.daemon = True
            flusher_thread.start()
            self.flusher_threads.append(flusher_thread)
        logger.info(f"🔄 Started {self.num_flushers} dedicated flusher threads")

    def _stop_flusher_threads(self):
        """Stop flusher threads gracefully"""
        logger.info("🔄 Stopping flusher threads...")

        # Send poison pills
        for _ in range(self.num_flushers):
            try:
                self.data_queue.put(None, timeout=5)
            except Full:
                logger.warning("Queue is full, waiting to send poison pill...")
                time.sleep(1)

        # Wait for all flusher threads to complete
        for flusher_thread in self.flusher_threads:
            flusher_thread.join(timeout=30)
        logger.info("✅ All flusher threads stopped")

    def process_cma(
        self,
        country_code: str = "AE",
        max_properties: int = 100,
        max_combinations: int = 16,
        offset: int = 0,
        dry_run: bool = False,
    ):
        """Main method to process CMA data"""

        logger.info("🚀 Starting CMA data import with dynamic schema")
        logger.info(f"Country: {country_code}")
        logger.info(f"Max properties: {max_properties}")
        logger.info(f"Offset: {offset}")
        logger.info(f"Max combinations per property: {max_combinations}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Request delay: {self.request_delay}s")
        logger.info(f"Dry run: {dry_run}")

        try:
            # Get properties from database
            properties = self.db.get_properties(limit=max_properties, offset=offset)
            logger.info(
                f"Retrieved {len(properties)} properties from database (offset: {offset})"
            )

            if not properties:
                logger.warning("No properties found in database")
                return

            # Generate API tasks
            api_tasks = []
            for property_data in properties:
                property_id = property_data["id"]

                # Generate combinations
                combinations = 0
                for alias in self.aliases:
                    for currency in self.currencies:
                        for measurement in self.measurements:
                            for property_type in self.property_types:
                                for activity_type in self.activity_types:
                                    if combinations >= max_combinations:
                                        break

                                    api_tasks.append(
                                        {
                                            "property_id": property_id,
                                            "alias": alias,
                                            "currency": currency,
                                            "measurement": measurement,
                                            "property_type": property_type,
                                            "activity_type": activity_type,
                                        }
                                    )
                                    combinations += 1

                                if combinations >= max_combinations:
                                    break
                            if combinations >= max_combinations:
                                break
                        if combinations >= max_combinations:
                            break
                    if combinations >= max_combinations:
                        break

            logger.info(
                f"Generated {len(api_tasks)} API tasks for {len(properties)} properties"
            )
            logger.info(
                f"Average {len(api_tasks) / len(properties):.1f} combinations per property"
            )

            if dry_run:
                logger.info("🔍 Dry run mode - no data will be inserted")
                return

            # Start flusher threads
            self._start_flusher_threads()

            # Process API tasks
            logger.info(
                f"🔄 Starting parallel API processing with {self.max_workers} workers..."
            )

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                futures = []
                for task in api_tasks:
                    # Acquire semaphore
                    self.task_semaphore.acquire()

                    future = executor.submit(self._api_worker, task)
                    futures.append(future)

                # Process results
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            self.data_queue.put(result)
                            self.total_records_processed += 1
                    except Exception as e:
                        logger.error(f"Error processing future: {e}")

            # Stop flusher threads
            self._stop_flusher_threads()

            logger.info("🎉 CMA import completed!")
            logger.info(f"Total records processed: {self.total_records_processed}")
            logger.info(f"Total records inserted: {self.total_records_inserted}")
            logger.info(f"Total batches flushed: {self.total_batches_flushed}")
            logger.info(f"Discovered columns: {len(self.discovered_columns)}")

        except Exception as e:
            logger.error(f"❌ Error in CMA import: {e}")
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Import CMA data from Reidin API with dynamic schema"
    )

    parser.add_argument(
        "--country-code", type=str, default="AE", help="Country code (default: AE)"
    )
    parser.add_argument(
        "--max-properties",
        type=int,
        default=100,
        help="Maximum number of properties to process (default: 100)",
    )
    parser.add_argument(
        "--max-combinations",
        type=int,
        default=16,
        help="Maximum parameter combinations to try per property (default: 16)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=1.0,
        help="Delay between API requests in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Batch size for database inserts (default: 5000)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Maximum number of parallel workers (default: 8)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Run in dry-run mode (no data insertion)"
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Offset for property processing (for batch processing)",
    )

    args = parser.parse_args()

    try:
        importer = CMAImporter(
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            request_delay=args.request_delay,
        )

        importer.process_cma(
            country_code=args.country_code,
            max_properties=args.max_properties,
            max_combinations=args.max_combinations,
            offset=args.offset,
            dry_run=args.dry_run,
        )

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
