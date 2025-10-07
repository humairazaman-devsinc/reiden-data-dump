#!/usr/bin/env python3
"""
CMA Sales Data Importer

This script imports CMA sales data from the Reidin API following the established ETL pattern.
It processes properties and fetches CMA sales data for each property with various parameter combinations.
"""

import argparse
import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread, Semaphore
from queue import Queue, Empty, Full
from datetime import datetime
from utils.database import DatabaseManager
from utils.api_client import ReidinAPIClient
from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("cma_sales_import.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

def to_float(value):
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None

def to_int(value):
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None

def parse_iso_datetime(value):
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid datetime '{value}': {e}")
        return None

class CMASalesImporter:
    def __init__(
        self, batch_size: int = 5000, max_workers: int = 8, request_delay: float = 1.0
    ):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.request_delay = request_delay

        # Asynchronous queue for decoupling API fetching from DB inserts (with backpressure)
        self.data_queue = Queue(
            maxsize=200000
        )  # Increased for better throughput - prevents backpressure

        # Flusher threads control (multiple flushers for better DB throughput)
        self.flusher_threads = []
        self.num_flushers = 6  # Increased for M3 Mac performance (6-8 flushers)
        self.processing_complete = False  # Signal to indicate processing is done

        # Bounded semaphore for controlled task submission
        self.task_semaphore = Semaphore(
            max_workers * 50
        )  # Balanced limit for sliding window

        # Global rate limiter for API calls (prevents 429 errors)
        self.rate_limiter = Semaphore(1)  # Global rate limiting
        self.last_request_time = 0

        # Statistics
        self.total_records_inserted = 0
        self.total_batches_flushed = 0

        self.aliases = ["last-fifteen"]
        self.currencies = ["aed"]
        self.measurements = ["int"]
        # self.property_types = ["Commercial", "Land", "Residential", "Office"]
        self.property_types = ["Commercial", "Land", "Residential", "Office"]

        # Optimized property type to subtype mapping for faster processing
        self.property_type_subtypes = {
            "Commercial": [
                "Industrial-Warehouse",
                "Other",
                "Shop",
                "Showroom",
                "Sized Partition",
                "Sport & Entertainment",
                "Villa",
                "Whole Building",
                "Workshop",
            ],
            "Land": [
                "Commercial Land",
                "Land for Agriculture",
                "Land for Airport",
                "Land for Education",
                "Land for Government Housing",
                "Land for Government Organizations",
                "Land for Industrial",
                "Land for Labor Camp",
                "Land for Shopping Mall",
                "Land for Sports",
                "Land for Warehouse",
                "Land - General",
                "Other Land",
                "Residential Land",
            ],
            "Residential": [
                "Apartment",
                "Serviced/Hotel Apartment",
                "Villa",
                "Villa Plot",
            ],
            "Office": ["Medical Office", "Office", "Sized Partition", "Whole Building"],
        }
        # self.property_subtypes = [
        #     "Sized Partition", "Whole Building" ]   # REQUIRED
        # self.property_subtypes = [
        #     "Medical Office", "Office", "Sized Partition", "Whole Building" ]  # REQUIRED

    def start_flusher_threads(self):
        """Start multiple dedicated flusher threads for better DB throughput"""
        if not self.flusher_threads:
            for i in range(self.num_flushers):
                flusher_thread = Thread(
                    target=self._flusher_worker, daemon=True, name=f"Flusher-{i+1}"
                )
                flusher_thread.start()
                self.flusher_threads.append(flusher_thread)
            logger.info(f"🔄 Started {self.num_flushers} dedicated flusher threads")

    def stop_flusher_threads(self):
        """Stop all flusher threads and flush any remaining data"""
        logger.info("🔄 Stopping flusher threads...")
        # Send poison pills to stop all flushers with retry logic
        for _ in range(self.num_flushers):
            while True:
                try:
                    self.data_queue.put(None, timeout=5.0)
                    break
                except Full:
                    logger.warning("Queue is full, waiting to send poison pill...")
                    time.sleep(1)

        # Wait for all flusher threads to complete
        for flusher_thread in self.flusher_threads:
            flusher_thread.join(timeout=30)
        logger.info("✅ All flusher threads stopped")

    def _flusher_worker(self):
        """Dedicated worker thread that handles database inserts"""
        buffer = []
        last_flush_time = time.time()

        while True:
            try:
                # Try to get data from queue with timeout
                try:
                    data = self.data_queue.get(timeout=1.0)
                    if data is None:  # Poison pill - stop processing
                        break

                    # Handle both individual records and chunks
                    if isinstance(data, list):
                        buffer.extend(data)  # Add chunk of records
                    else:
                        buffer.append(data)  # Add single record
                except Empty:
                    # Timeout - check if we should flush based on time or completion
                    if buffer and (
                        (time.time() - last_flush_time) > 30 or self.processing_complete
                    ):  # 30 second timeout or processing complete
                        self._flush_buffer(buffer)
                        buffer.clear()
                        last_flush_time = time.time()
                    if self.processing_complete:
                        break
                    continue

                # Flush if buffer is full
                if len(buffer) >= self.batch_size:
                    self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush_time = time.time()

            except Exception as e:
                logger.exception("❌ Error in flusher worker")
                time.sleep(2)  # Prevent tight retry loop
                # Continue processing - don't re-raise to keep flusher alive

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

            self.db.insert_cma_sales_data(buffer)

            end_time = time.time()
            duration = end_time - start_time

            self.total_records_inserted += len(buffer)
            self.total_batches_flushed += 1

            # Calculate and log speed
            speed = len(buffer) / duration if duration > 0 else 0
            logger.info(
                f"✅ Successfully inserted {len(buffer)} records in {duration:.2f}s ({speed:.1f} rec/s) (Total: {self.total_records_inserted} records, {self.total_batches_flushed} batches)"
            )

            # Log queue size every 10 batches to monitor bottlenecks
            if self.total_batches_flushed % 10 == 0:
                queue_size = self.data_queue.qsize()
                logger.info(f"📊 Queue size: {queue_size}")

                # Warning if queue is getting too full
                if queue_size > self.data_queue.maxsize * 0.8:
                    logger.warning(
                        f"⚠️ Queue is {queue_size}/{self.data_queue.maxsize} (~{queue_size/self.data_queue.maxsize*100:.1f}% full) - DB may be falling behind"
                    )

                # Monitor semaphore usage
                available_slots = self.task_semaphore._value
                total_slots = self.max_workers * 50
                if available_slots < total_slots * 0.1:  # Less than 10% available
                    logger.warning(
                        f"⚠️ Task semaphore is {total_slots - available_slots}/{total_slots} slots used - may need tuning"
                    )

        except Exception as e:
            logger.error(f"❌ Failed to flush batch of {len(buffer)} records: {e}")
            # Re-queue the data for retry in smaller chunks to avoid clogging the queue
            # Note: ON CONFLICT DO NOTHING will skip duplicates, so re-queuing is safe
            chunk_size = min(100, len(buffer))  # Re-queue in smaller chunks
            for i in range(0, len(buffer), chunk_size):
                chunk = buffer[i : i + chunk_size]
                try:
                    # Add chunk as a list to be processed by the flusher
                    self.data_queue.put(chunk, timeout=1.0)
                except Full:
                    logger.error(
                        f"❌ Queue is full, cannot re-queue chunk of {len(chunk)} records for retry"
                    )
                except Exception as retry_e:
                    logger.error(f"❌ Failed to re-queue chunk for retry: {retry_e}")

    def get_properties(
        self, limit: Optional[int] = None, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get properties from the database with optional offset for batch processing"""
        try:
            properties = self.db.get_properties(limit=limit, offset=offset)
            logger.info(
                f"Retrieved {len(properties)} properties from database (offset: {offset})"
            )
            return properties
        except Exception as e:
            logger.error(f"Failed to get properties: {e}")
            return []

    def get_processed_property_ids(self) -> set:
        """Get set of property IDs that have already been processed with current parameter combinations (for resume capability)"""
        try:
            query = """
                        SELECT DISTINCT property_id 
                        FROM cma_sales 
                        WHERE alias = %s 
                        AND currency = %s 
                        AND measurement = %s 
                        AND property_type = %s 
                    """
            # AND property_subtype IN (%s, %s)
            columns = (
                self.aliases[0],
                self.currencies[0],
                self.measurements[0],
                self.property_types[0],
                # self.property_subtypes[0],
                # self.property_subtypes[1],
            )
            properties = self.db.run_query(query, columns)
            processed_ids = {row[0] for row in properties}
            logger.info(
                f"Found {len(processed_ids)} already processed property IDs with current combinations"
            )
            return processed_ids
        except Exception as e:
            logger.error(f"Failed to get processed property IDs: {e}")
            return set()

    def add_to_queue(self, data: List[Dict[str, Any]]) -> None:
        """Add processed data to the queue for the flusher thread"""
        for record in data:
            try:
                self.data_queue.put(record, timeout=5.0)
            except Full:
                logger.error(
                    f"❌ Queue is full, cannot add record. Queue size: {self.data_queue.qsize()}"
                )
            except Exception as e:
                logger.error(f"❌ Failed to add record to queue: {e}")

    def _process_cma_sales_data(
        self,
        raw_data: List[Dict[str, Any]],
        property_id: str | int,
        alias: str,
        currency: str,
        measurement: str,
        property_type: str,
        property_subtype: str,
        no_of_bedrooms: int | None = None,
        size: str | None = None,
        sales_activity_type: str | None = None,
        lat: str | None = None,
        lon: str | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Process CMA sales data from API response based on the sample structure:
        {
            "size": 5746.25,
            "price": 3100000,
            "location": {
                "city_id": 5,
                "city_name": "Dubai",
                "county_id": 600,
                "county_name": "Dubai Investment Park",
                "district_id": 23437,
                "location_id": 23437,
                "district_name": "Dubai Investment Park First",
                "location_name": "Dubai Investment Park First",
                "municipal_area": "Dubai Investment Park First"
            },
            "property_id": 119327580,
            "activity_type": "Sales - Ready",
            "property_name": "8Wd",
            "property_type": "Commercial",
            "no_of_bedrooms": null,
            "number_of_unit": "W-3",
            "price_per_size": 539.48,
            "parent_property": {},
            "property_nature": "Building",
            "number_of_floors": "G",
            "property_subtype": "Industrial-Warehouse",
            "transaction_date": "2025-06-18T15:03:46Z"
        }
        """
        processed_records = []
        seen_keys = set()  # For deduplication

        for item in raw_data:
            try:
                if not isinstance(item, dict):
                    continue

                # The CMA sales API returns actual property transaction records
                # Each record represents a comparable property transaction
                comparable_property_id = item.get("property_id")
                if not comparable_property_id:
                    logger.warning(f"No property_id found in CMA sales record: {item}")
                    continue

                # Create unique key for deduplication based on the comparable property
                unique_key = (
                    int(property_id),
                    alias,
                    currency,
                    measurement,
                    property_type,
                    property_subtype,
                    int(comparable_property_id),
                )

                if unique_key in seen_keys:
                    continue  # Skip duplicate

                seen_keys.add(unique_key)

                # Extract and convert numeric values with proper type handling
                size_value = to_float(item.get("size"))
                price_value = to_float(item.get("price"))
                price_per_size_value = to_float(item.get("price_per_size"))

                # Parse transaction date
                transaction_date = item.get("transaction_date")

                # Extract location information (flatten nested location object)
                location = item.get("location", {})

                # Extract parent property information
                parent_property = item.get("parent_property", {})

                # Extract no_of_bedrooms from the item (not from parameters)
                item_no_of_bedrooms = to_int(item.get("no_of_bedrooms"))

                processed_record = {
                    # Property information (the property we're getting CMA for)
                    "property_id": int(property_id),
                    # CMA parameters used in the query
                    "alias": alias,
                    "currency": currency,
                    "measurement": measurement,
                    "property_type": property_type,
                    "property_subtype": property_subtype,
                    # Comparable property information (from the API response)
                    "comparable_property_id": int(comparable_property_id),
                    "comparable_property_name": item.get("property_name"),
                    # Transaction details from comparable property
                    "size": size_value,
                    "price": price_value,
                    "price_per_size": price_per_size_value,
                    "transaction_date": transaction_date,
                    # Location information (flattened from nested location object)
                    "city_id": location.get("city_id"),
                    "city_name": location.get("city_name"),
                    "county_id": location.get("county_id"),
                    "county_name": location.get("county_name"),
                    "district_id": location.get("district_id"),
                    "district_name": location.get("district_name"),
                    "location_id": location.get("location_id"),
                    "location_name": location.get("location_name"),
                    "municipal_area": location.get("municipal_area"),
                    # Property attributes
                    "activity_type": item.get("activity_type"),
                    "no_of_bedrooms": item_no_of_bedrooms,
                    "number_of_unit": item.get("number_of_unit"),
                    "property_nature": item.get("property_nature"),
                    "number_of_floors": item.get("number_of_floors"),
                    # Parent property information (if applicable)
                    "parent_property": parent_property,
                    # Raw data for reference (complete API response)
                    "raw_data": item,
                }

                processed_records.append(processed_record)

            except Exception as e:
                logger.error(f"Failed to process CMA sales record: {e}")
                logger.debug(f"Problematic item: {item}")
                continue

        logger.info(
            f"Processed {len(processed_records)} CMA sales records from {len(raw_data)} raw records (deduplicated from {len(raw_data)})"
        )
        return processed_records

    def _process_with_semaphore(self, task: Dict[str, Any], country_code: str) -> int:
        """Process a single API call with semaphore control"""
        try:
            return self.process_single_api_call(
                task["property_id"],
                task["alias"],
                task["currency"],
                task["measurement"],
                task["property_type"],
                task["property_subtype"],
                task["no_of_bedrooms"],
                task["size"],
                task["sales_activity_type"],
                task["lat"],
                task["lon"],
                country_code,
            )
        finally:
            self.task_semaphore.release()  # Always release semaphore

    def process_single_api_call(
        self,
        property_id: str | int,
        alias: str,
        currency: str,
        measurement: str,
        property_type: str,
        property_subtype: str,
        no_of_bedrooms: Optional[int] = None,
        size: Optional[str] = None,
        sales_activity_type: Optional[str] = None,
        lat: Optional[str] = None,
        lon: Optional[str] = None,
        country_code: str = "AE",
    ) -> int:
        """Process a single API call with all parameters (REQUIRED + optional)"""
        try:
            # Global rate limiting to prevent 429 errors
            with self.rate_limiter:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < self.request_delay:
                    time.sleep(self.request_delay - time_since_last)
                self.last_request_time = time.time()

            # Fetch data from API with all parameters
            response = self.api_client.fetch_cma_sales(
                country_code=country_code,
                property_id=property_id,
                alias=alias,
                currency=currency,
                measurement=measurement,
                property_type=property_type,
                property_subtype=property_subtype,
                no_of_bedrooms=no_of_bedrooms,
                size=size,
                sales_activity_type=sales_activity_type,
                lat=lat,
                lon=lon,
            )

            # Process the response
            results = response.get("results", [])

            if results:
                # Process the data with all parameters
                processed_data = self._process_cma_sales_data(
                    raw_data=results,
                    property_id=property_id,
                    alias=alias,
                    currency=currency,
                    measurement=measurement,
                    property_type=property_type,
                    property_subtype=property_subtype,
                    no_of_bedrooms=no_of_bedrooms,
                    size=size,
                    sales_activity_type=sales_activity_type,
                    lat=lat,
                    lon=lon,
                )

                if processed_data:
                    # Add to queue for the flusher thread
                    self.add_to_queue(processed_data)
                    return len(processed_data)

            return 0

        except Exception as e:
            # Create a more descriptive error message
            param_str = (
                f"{alias}_{currency}_{measurement}_{property_type}_{property_subtype}"
            )
            if no_of_bedrooms is not None:
                param_str += f"_bed{no_of_bedrooms}"
            if size:
                param_str += f"_size{size}"
            if sales_activity_type:
                param_str += f"_{sales_activity_type.replace(' ', '_')}"

            logger.warning(
                f"Property {property_id}: Failed combination {param_str} - {e}"
            )
            return 0

    def generate_api_tasks(
        self, properties: List[Dict[str, Any]], max_combinations_per_property: int = 20
    ) -> List[Dict[str, Any]]:
        """Generate API tasks using simple approach - all subtypes for all properties"""
        tasks = []

        for property_data in properties:
            property_id = property_data.get("id")
            if not property_id:
                continue

            # Optimized approach: use only relevant subtypes for each property type
            for alias in self.aliases:
                for currency in self.currencies:
                    for measurement in self.measurements:
                        for property_type in self.property_types:
                            # Get only relevant subtypes for this property type
                            relevant_subtypes = self.property_type_subtypes.get(
                                property_type, []
                            )
                            for property_subtype in relevant_subtypes:
                                # Only REQUIRED parameters - no optional parameters
                                tasks.append(
                                    {
                                        "property_id": property_id,
                                        "alias": alias,
                                        "currency": currency,
                                        "measurement": measurement,
                                        "property_type": property_type,
                                        "property_subtype": property_subtype,
                                        "no_of_bedrooms": None,
                                        "size": None,
                                        "sales_activity_type": None,
                                        "lat": None,
                                        "lon": None,
                                    }
                                )

        logger.info(
            f"Generated {len(tasks)} API tasks for {len(properties)} properties"
        )
        logger.info(
            f"Average {len(tasks) / len(properties):.1f} combinations per property"
        )
        return tasks

    def process_cma_sales(
        self,
        country_code: str = "AE",
        max_properties: int = 500,
        max_combinations_per_property: int = 10,
        request_delay: float = 0.25,
        dry_run: bool = False,
        offset: int = 0,
    ) -> None:
        """Main method to process CMA sales data with parallel API fetching and serialized DB inserts"""

        logger.info("🚀 Starting CMA Sales data import with new architecture")
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

        # Get properties from database with offset
        properties = self.get_properties(limit=max_properties, offset=offset)

        if not properties:
            logger.error("No properties found in database")
            return

        # Generate all API tasks
        logger.info("📋 Generating API tasks...")
        api_tasks = self.generate_api_tasks(properties, max_combinations_per_property)
        logger.info(f"Generated {len(api_tasks)} API tasks")

        # Filter out already processed property IDs for resume capability
        processed_property_ids = self.get_processed_property_ids()
        if processed_property_ids:
            original_count = len(api_tasks)
            api_tasks = [
                task
                for task in api_tasks
                if task["property_id"] not in processed_property_ids
            ]
            skipped_count = original_count - len(api_tasks)
            logger.info(
                f"🔄 Resume mode: Skipped {skipped_count} already processed tasks, {len(api_tasks)} remaining"
            )

        if not api_tasks:
            logger.warning("No API tasks generated")
            return

        # Start the dedicated flusher thread
        self.start_flusher_threads()

        try:
            # Start parallel API processing with chunked submission
            logger.info(
                f"🔄 Starting parallel API processing with {self.max_workers} workers..."
            )

            successful_tasks = 0
            failed_tasks = 0
            total_records_processed = 0

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Use batch processing approach: submit tasks in batches to control memory
                max_futures_in_memory = (
                    self.max_workers * 20
                )  # Keep more futures for better parallelism
                logger.info(
                    f"📦 Processing {len(api_tasks)} tasks in batches (max {max_futures_in_memory} futures per batch)"
                )

                # Submit all tasks in batches to control memory
                batch_size = max_futures_in_memory
                for i in range(0, len(api_tasks), batch_size):
                    batch_tasks = api_tasks[i : i + batch_size]
                    logger.info(
                        f"📦 Submitting batch {i//batch_size + 1}/{(len(api_tasks) + batch_size - 1)//batch_size} ({len(batch_tasks)} tasks)"
                    )

                    # Submit batch
                    batch_futures = []
                    for task in batch_tasks:
                        self.task_semaphore.acquire()
                        future = executor.submit(
                            self._process_with_semaphore, task, country_code
                        )
                        batch_futures.append((future, task))

                    # Process completed futures in this batch
                    for future, task in batch_futures:
                        try:
                            records_processed = future.result(
                                timeout=30
                            )  # 30 second timeout
                            successful_tasks += 1
                            total_records_processed += records_processed

                            # Log progress every 1000 tasks
                            if (successful_tasks + failed_tasks) % 1000 == 0:
                                logger.info(
                                    f"📊 Progress: {successful_tasks + failed_tasks}/{len(api_tasks)} tasks completed, {total_records_processed} records processed"
                                )

                        except TimeoutError:
                            failed_tasks += 1
                            logger.error(
                                f"❌ Task timed out after 30 seconds for property {task['property_id']}"
                            )
                        except Exception as e:
                            failed_tasks += 1
                            logger.error(
                                f"❌ Task failed for property {task['property_id']}: {e}"
                            )

                    # Log memory usage every batch
                    try:
                        import psutil

                        memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                        logger.info(
                            f"✅ Batch completed: {len(batch_tasks)} tasks, Memory: {memory_mb:.1f}MB"
                        )
                    except ImportError:
                        logger.info(f"✅ Batch completed: {len(batch_tasks)} tasks")

                    # Clear batch futures to free memory
                    batch_futures.clear()

            # Stop the flusher thread and wait for final flush
            logger.info(
                "🔄 Signaling processing completion and stopping flusher threads..."
            )
            self.processing_complete = True
            self.stop_flusher_threads()

            # Final summary
            logger.info("🎉 CMA Sales import completed!")
            logger.info(f"Total API tasks: {len(api_tasks)}")
            logger.info(f"Successful tasks: {successful_tasks}")
            logger.info(f"Failed tasks: {failed_tasks}")
            logger.info(f"Total records processed: {total_records_processed}")
            logger.info(f"Total records inserted: {self.total_records_inserted}")
            logger.info(f"Total batches flushed: {self.total_batches_flushed}")
            logger.info(
                f"Average records per batch: {self.total_records_inserted / max(self.total_batches_flushed, 1):.1f}"
            )

        except Exception as e:
            logger.error(f"❌ Error during processing: {e}")
            self.stop_flusher_threads()
            raise


def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description="Import CMA Sales data from Reidin API with parallel batch processing"
    )

    parser.add_argument(
        "--country-code", default="AE", help="Country code (default: AE)"
    )
    parser.add_argument(
        "--max-properties",
        type=int,
        default=500,
        help="Maximum number of properties to process (default: 500)",
    )
    parser.add_argument(
        "--max-combinations",
        type=int,
        default=10,
        help="Maximum parameter combinations to try per property (default: 10)",
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
        default=20000,
        help="Batch size for database inserts (default: 5000)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Maximum number of parallel workers (default: 6)",
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
        importer = CMASalesImporter(
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            request_delay=args.request_delay,
        )

        importer.process_cma_sales(
            country_code=args.country_code,
            max_properties=args.max_properties,
            max_combinations_per_property=args.max_combinations,
            request_delay=args.request_delay,
            dry_run=args.dry_run,
            offset=args.offset,
        )

    except Exception as e:
        logger.error(f"CMA Sales import failed: {e}")
        raise


if __name__ == "__main__":
    main()
