#!/usr/bin/env python3
"""
Optimized Transaction Rent Importer

This script imports transaction rent data from the Reidin API with optimized batch processing:
1. Generate all possible parameter combinations upfront
2. Create request templates for each parameter combination
3. Batch locations by parameter template
4. Process multiple locations in parallel for each template
5. Accumulate data across all templates before database insertion
"""

import argparse
import logging
import time
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from contextlib import contextmanager
from collections import deque
import threading

# Ensure the parent directory is on sys.path when running this file directly
PARENT_DIR = Path(__file__).resolve().parents[1]
if str(PARENT_DIR) not in sys.path:
    sys.path.append(str(PARENT_DIR))

from config import Config
from api_client import ReidinAPIClient
from database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reidin_transaction_rent_import_optimized.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that automatically adjusts request rate based on network conditions
    and error patterns to prevent timeouts and optimize performance.
    """
    
    def __init__(self, initial_rate: float = 2.0, min_rate: float = 0.5, max_rate: float = 5.0):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.last_adjustment = time.time()
        self.adjustment_interval = 30  # Adjust every 30 seconds
        
        # Error tracking
        self.timeout_errors = deque(maxlen=100)  # Last 100 errors
        self.success_requests = deque(maxlen=100)  # Last 100 requests
        self.lock = threading.Lock()
        
        # Performance metrics
        self.total_requests = 0
        self.total_timeouts = 0
        self.total_429_errors = 0
        
    def record_request(self, success: bool, error_type: str = None):
        """Record a request result for adaptive adjustment"""
        with self.lock:
            self.total_requests += 1
            current_time = time.time()
            
            if success:
                self.success_requests.append(current_time)
            else:
                if error_type == 'timeout':
                    self.timeout_errors.append(current_time)
                    self.total_timeouts += 1
                elif error_type == '429':
                    self.total_429_errors += 1
    
    def get_current_rate(self) -> float:
        """Get the current rate limit"""
        return self.current_rate
    
    def should_adjust(self) -> bool:
        """Check if it's time to adjust the rate"""
        return time.time() - self.last_adjustment > self.adjustment_interval
    
    def adjust_rate(self):
        """Adjust the rate based on recent performance"""
        if not self.should_adjust():
            return
            
        with self.lock:
            current_time = time.time()
            self.last_adjustment = current_time
            
            # Calculate recent timeout rate (last 2 minutes)
            recent_timeouts = sum(1 for t in self.timeout_errors 
                                if current_time - t < 120)
            recent_requests = len(self.success_requests) + recent_timeouts
            
            timeout_rate = recent_timeouts / max(recent_requests, 1)
            
            # Calculate recent 429 error rate
            recent_429s = self.total_429_errors
            error_rate = recent_429s / max(self.total_requests, 1)
            
            # Adjust rate based on error patterns
            if timeout_rate > 0.1:  # More than 10% timeouts
                # Reduce rate significantly
                self.current_rate = max(self.min_rate, self.current_rate * 0.7)
                logger.warning(f"🔻 High timeout rate ({timeout_rate:.1%}), reducing rate to {self.current_rate:.1f} req/sec")
                
            elif timeout_rate > 0.05:  # More than 5% timeouts
                # Reduce rate moderately
                self.current_rate = max(self.min_rate, self.current_rate * 0.85)
                logger.info(f"🔻 Moderate timeout rate ({timeout_rate:.1%}), reducing rate to {self.current_rate:.1f} req/sec")
                
            elif timeout_rate < 0.01 and error_rate < 0.02:  # Very low error rates
                # Increase rate cautiously
                self.current_rate = min(self.max_rate, self.current_rate * 1.1)
                logger.info(f"🔺 Low error rates, increasing rate to {self.current_rate:.1f} req/sec")
                
            else:
                logger.debug(f"📊 Rate stable at {self.current_rate:.1f} req/sec (timeout: {timeout_rate:.1%}, 429: {error_rate:.1%})")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        with self.lock:
            return {
                'current_rate': self.current_rate,
                'total_requests': self.total_requests,
                'total_timeouts': self.total_timeouts,
                'total_429_errors': self.total_429_errors,
                'timeout_rate': self.total_timeouts / max(self.total_requests, 1),
                'error_rate': self.total_429_errors / max(self.total_requests, 1)
            }


@dataclass
class OptimizedProcessingConfig:
    """Configuration for optimized transaction rent processing"""
    country_code: str = "AE"
    max_workers: int = 16  # Increased for better parallelization
    requests_per_second: float = 8.0  # Increased rate for batch processing
    max_pages: int = 20  # Increased to capture more data per location
    dry_run: bool = False
    resume_scroll_id: Optional[str] = None
    # New optimization parameters
    locations_per_batch: int = 2001  # Process all locations in single batch per template
    max_accumulated_records: int = 50000  # Insert when we have 50k records
    template_parallel_workers: int = 4  # Workers per template
    adaptive_rate_limiting: bool = True  # Enable adaptive rate limiting
    
    def __post_init__(self):
        # Initialize adaptive rate limiter if enabled
        if self.adaptive_rate_limiting:
            self.rate_limiter = AdaptiveRateLimiter(
                initial_rate=self.requests_per_second,
                min_rate=0.5,
                max_rate=5.0
            )
        else:
            self.rate_limiter = None


@dataclass
class RequestTemplate:
    """Template for API requests with specific parameters"""
    measurement: str
    template_id: str  # Unique identifier for this template


@dataclass
class BatchRequest:
    """Batch request containing multiple locations for a template"""
    template: RequestTemplate
    location_ids: List[str]
    batch_id: str


@dataclass
class ProcessingResult:
    """Result of processing operation"""
    total_records: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    failed_combinations: List[Tuple[str, Dict[str, Any]]] = None
    templates_processed: int = 0
    batches_processed: int = 0
    
    def __post_init__(self):
        if self.failed_combinations is None:
            self.failed_combinations = []


class ResponseFlattener:
    """Handles flattening of API responses into database-ready format for COPY optimization"""
    
    def flatten_response(self, item: Dict[str, Any], location_id: str, measurement: str) -> Dict[str, Any]:
        """Flatten nested API response into database-ready format matching new schema"""
        processed = {
            "raw_data": item,
            "currency": "aed",  # Default currency since it's not in API response but required by DB
            "measurement": measurement
        }
        
        # Handle direct fields
        for key in ["date", "size", "price", "end_date", "start_date", "size_land_int", 
                   "price_per_size", "transaction_type", "transaction_version"]:
            if key in item:
                processed[key] = item[key]
        
        # Handle location object
        if "location" in item and isinstance(item["location"], dict):
            loc = item["location"]
            processed.update({
                "loc_city_id": loc.get("city_id"),
                "loc_city_name": loc.get("city_name"),
                "loc_county_id": loc.get("county_id"),
                "loc_county_name": loc.get("county_name"),
                "loc_district_id": loc.get("district_id"),
                "loc_location_id": loc.get("location_id"),
                "loc_district_name": loc.get("district_name"),
                "loc_location_name": loc.get("location_name"),
                "loc_municipal_area": loc.get("municipal_area")
            })
        
        # Handle property object
        if "property" in item and isinstance(item["property"], dict):
            prop = item["property"]
            processed.update({
                "prop_property_id": prop.get("property_id"),
                "prop_property_name": prop.get("property_name")
            })
        
        # Handle attributes object
        if "attributes" in item and isinstance(item["attributes"], dict):
            attr = item["attributes"]
            processed.update({
                "attr_unit": attr.get("unit"),
                "attr_floor": attr.get("floor"),
                "attr_parking": attr.get("parking"),
                "attr_land_number": attr.get("land_number"),
                "attr_no_of_rooms": attr.get("no_of_rooms"),
                "attr_balcony_area": attr.get("balcony_area"),
                "attr_building_name": attr.get("building_name"),
                "attr_building_number": attr.get("building_number")
            })
        
        # Handle property_type object
        if "property_type" in item and isinstance(item["property_type"], dict):
            prop_type = item["property_type"]
            processed.update({
                "prop_type_name": prop_type.get("type_name"),
                "prop_subtype_name": prop_type.get("subtype_name")
            })
                
        return processed


class OptimizedTransactionRentImporter:
    """Optimized importer for transaction rent data from Reidin API"""
    
    def __init__(self, config: OptimizedProcessingConfig):
        self.config = config
        self.measurements = ["int", "imp"]
        self.min_interval_sec = 1.0 / config.requests_per_second
        self.flattener = ResponseFlattener()
        
    @contextmanager
    def _get_resources(self):
        """Context manager for database and API client resources"""
        db = DatabaseManager()
        api_client = ReidinAPIClient()
        try:
            # Table already exists in Azure database
            yield db, api_client
        finally:
            # Cleanup if needed
            pass

    def generate_all_parameter_combinations(self) -> List[RequestTemplate]:
        """
        Generate all possible parameter combinations as request templates
        
        Returns:
            List of RequestTemplate objects representing all possible parameter combinations
        """
        templates = []
        
        # Generate templates for each measurement
        for measurement in self.measurements:
            template = RequestTemplate(
                measurement=measurement,
                template_id=f"{measurement}"
            )
            templates.append(template)
        
        logger.info(f"Generated {len(templates)} request templates")
        for template in templates:
            logger.info(f"  - Template {template.template_id}: measurement={template.measurement}")
        
        return templates

    def create_location_batches(self, location_ids: List[str], templates: List[RequestTemplate]) -> List[BatchRequest]:
        """
        Create batches of locations for each template
        
        Args:
            location_ids: List of all location IDs to process
            templates: List of request templates
            
        Returns:
            List of BatchRequest objects
        """
        batch_requests = []
        
        for template in templates:
            # Split locations into batches for this template
            for i in range(0, len(location_ids), self.config.locations_per_batch):
                batch_location_ids = location_ids[i:i + self.config.locations_per_batch]
                batch_id = f"{template.template_id}_batch_{i // self.config.locations_per_batch + 1}"
                
                batch_request = BatchRequest(
                    template=template,
                    location_ids=batch_location_ids,
                    batch_id=batch_id
                )
                batch_requests.append(batch_request)
        
        logger.info(f"Created {len(batch_requests)} batch requests")
        logger.info(f"  - {len(templates)} templates")
        logger.info(f"  - {len(location_ids)} locations")
        logger.info(f"  - {self.config.locations_per_batch} locations per batch")
        
        return batch_requests

    def _safe_api_call(self, api_client, location_id: str, template: RequestTemplate, params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], bool]:
        """Safe API call with automatic rate limiting and error handling"""
        try:
            payload = api_client.fetch_transaction_raw_rents(self.config.country_code, params)
            self._record_rate(success=True)
            return payload, True
        except Exception as e:
            error_type = self._detect_error_type(e)
            self._record_rate(success=False, error_type=error_type)
            logger.warning(f"API call failed for location {location_id} with template {template.template_id}: {e}")
            return None, False

    def _record_rate(self, success: bool, error_type: str = None):
        """Record request result for rate limiting"""
        if self.config.rate_limiter:
            self.config.rate_limiter.record_request(success, error_type)

    def _detect_error_type(self, error: Exception) -> str:
        """Detect error type from exception"""
        error_str = str(error).lower()
        if "timeout" in error_str or "ConnectTimeoutError" in error_str:
            return "timeout"
        elif "429" in error_str or "Too Many Requests" in error_str:
            return "429"
        return None

    def _get_locations(self, db: DatabaseManager, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Get locations from the database"""
        try:
            locations = db.get_locations(country_code=self.config.country_code, limit=limit, offset=offset)
            logger.info(f"Retrieved {len(locations)} locations from database")
            return locations
        except Exception as e:
            logger.error(f"Failed to get locations: {e}")
            return []

    def _flatten_api_response(self, item: Dict[str, Any], location_id: str, measurement: str) -> Dict[str, Any]:
        """Flatten nested API response into database-ready format"""
        return self.flattener.flatten_response(item, location_id, measurement)

    def _build_api_params(self, location_id: str, template: RequestTemplate, page_number: int = 1, scroll_id: Optional[str] = None) -> Dict[str, Any]:
        """Build API parameters for transaction rent request using template"""
        params = {
            "measurement": template.measurement,
            "location": location_id,
            "page_number": page_number
        }
        if scroll_id:
            params["scroll_id"] = scroll_id
        return params
    
    def _process_api_response(self, results: List[Dict[str, Any]], location_id: str, measurement: str) -> List[Dict[str, Any]]:
        """Process API response data into database-ready format"""
        processed_data = []
        for item in results:
            if isinstance(item, dict):
                processed_item = self._flatten_api_response(item, location_id, measurement)
                processed_data.append(processed_item)
        return processed_data

    def _fetch_location_data(self, api_client: ReidinAPIClient, location_id: str, template: RequestTemplate) -> Tuple[int, List[Dict[str, Any]], bool]:
        """
        Fetch all data for a single location using a template
        
        Returns:
            Tuple of (total_records, processed_data, success_flag)
        """
        try:
            total_records = 0
            current_scroll_id = self.config.resume_scroll_id
            page = 1
            accumulated_data = []
            
            for _ in range(self.config.max_pages):
                params = self._build_api_params(location_id, template, page, current_scroll_id)
                
                # Use safe API call helper
                payload, success = self._safe_api_call(api_client, location_id, template, params)
                if not success:
                    return total_records, accumulated_data, False

                results = payload.get("results") if isinstance(payload, dict) else None
                
                if isinstance(results, list) and results:
                    total_records += len(results)
                    
                    if not self.config.dry_run:
                        processed_data = self._process_api_response(results, location_id, template.measurement)
                        
                        # Filter valid data and accumulate
                        valid_data = [record for record in processed_data if record.get('price') is not None]
                        if valid_data:
                            accumulated_data.extend(valid_data)
                        else:
                            break

                # Check for next page
                current_scroll_id = payload.get("scroll_id") if isinstance(payload, dict) else None
                page += 1
                if not current_scroll_id:
                    break

            return total_records, accumulated_data, True

        except Exception as e:
            logger.warning(f"Failed to fetch data for location {location_id} with template {template.template_id}: {e}")
            return total_records, accumulated_data, False

    def _process_batch_request(self, api_client: ReidinAPIClient, batch_request: BatchRequest) -> Tuple[int, List[Dict[str, Any]], int, int]:
        """
        Process a batch of locations for a specific template
        
        Returns:
            Tuple of (total_records, all_processed_data, successful_locations, failed_locations)
        """
        total_records = 0
        all_processed_data = []
        successful_locations = 0
        failed_locations = 0
        
        logger.info(f"Processing batch {batch_request.batch_id} with {len(batch_request.location_ids)} locations")
        
        # Process locations in parallel within this batch
        with ThreadPoolExecutor(max_workers=self.config.template_parallel_workers) as executor:
            # Submit all location tasks for this batch
            future_to_location = {
                executor.submit(self._fetch_location_data, api_client, location_id, batch_request.template): location_id
                for location_id in batch_request.location_ids
            }
            
            # Process completed location tasks
            for future in as_completed(future_to_location):
                location_id = future_to_location[future]
                try:
                    records, processed_data, success = future.result()
                    total_records += records
                    
                    if success:
                        successful_locations += 1
                        all_processed_data.extend(processed_data)
                    else:
                        failed_locations += 1
                        
                    # Adaptive rate limiting between location requests
                    if self.config.rate_limiter:
                        # Adjust rate based on recent performance
                        self.config.rate_limiter.adjust_rate()
                        current_rate = self.config.rate_limiter.get_current_rate()
                        time.sleep(1.0 / current_rate)
                    else:
                        time.sleep(self.min_interval_sec)
                    
                except Exception as e:
                    logger.error(f"Task failed for location {location_id} in batch {batch_request.batch_id}: {e}")
                    failed_locations += 1
        
        logger.info(f"Batch {batch_request.batch_id} completed: {successful_locations} successful, {failed_locations} failed, {len(all_processed_data)} records")
        return total_records, all_processed_data, successful_locations, failed_locations

    def process_optimized_transaction_rent_batch(self, max_locations: int = 500, offset: int = 0) -> ProcessingResult:
        """
        Main method to process transaction rent data with optimized batch processing
        
        Args:
            max_locations: Maximum number of locations to process
            offset: Number of locations to skip from the beginning
            
        Returns:
            ProcessingResult with statistics about the operation
        """
        result = ProcessingResult()
        
        logger.info("🚀 Starting OPTIMIZED Transaction Rent data import")
        logger.info(f"Country: {self.config.country_code}")
        logger.info(f"Max locations: {max_locations}")
        logger.info(f"Max workers: {self.config.max_workers}")
        logger.info(f"Requests per second: {self.config.requests_per_second}")
        logger.info(f"Locations per batch: {self.config.locations_per_batch}")
        logger.info(f"Max accumulated records: {self.config.max_accumulated_records}")
        logger.info(f"Dry run: {self.config.dry_run}")
        
        if self.config.dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be saved to database")
        
        with self._get_resources() as (db, api_client):
            # 1) Gather location IDs from the database
            locations = self._get_locations(db, max_locations, offset)
            if not locations:
                logger.error("No location ids found in DB; exiting.")
                return result
            
            location_ids = [loc["location_id"] for loc in locations]
            
            # 2) Generate all parameter combinations as templates
            templates = self.generate_all_parameter_combinations()
            result.templates_processed = len(templates)
            
            # 3) Create location batches for each template
            batch_requests = self.create_location_batches(location_ids, templates)
            result.batches_processed = len(batch_requests)
            
            # 4) Process batches in parallel with accumulated data insertion
            all_accumulated_data = []
            total_successful_requests = 0
            total_failed_requests = 0
            
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                # Submit all batch requests
                future_to_batch = {
                    executor.submit(self._process_batch_request, api_client, batch_request): batch_request
                    for batch_request in batch_requests
                }
                
                # Process completed batch requests
                for future in as_completed(future_to_batch):
                    batch_request = future_to_batch[future]
                    try:
                        records, processed_data, successful_locations, failed_locations = future.result()
                        result.total_records += records
                        total_successful_requests += successful_locations
                        total_failed_requests += failed_locations
                        
                        # Accumulate data for batch insertion
                        all_accumulated_data.extend(processed_data)
                        
                        # Log adaptive rate limiter stats periodically
                        if self.config.rate_limiter and total_successful_requests % 50 == 0:
                            stats = self.config.rate_limiter.get_stats()
                            logger.info(f"📊 Rate Limiter Stats: Rate={stats['current_rate']:.1f} req/sec, "
                                      f"Timeouts={stats['total_timeouts']}, 429s={stats['total_429_errors']}, "
                                      f"Timeout Rate={stats['timeout_rate']:.1%}")
                        
                        # Insert data when we reach the threshold
                        if len(all_accumulated_data) >= self.config.max_accumulated_records and not self.config.dry_run:
                            logger.info(f"Inserting {len(all_accumulated_data)} accumulated records to database")
                            db.insert_transaction_raw_rent_data(all_accumulated_data)
                            all_accumulated_data = []  # Reset accumulator
                            
                    except Exception as e:
                        logger.error(f"Batch failed for {batch_request.batch_id}: {e}")
                        total_failed_requests += len(batch_request.location_ids)
            
            # Insert any remaining accumulated data
            if all_accumulated_data and not self.config.dry_run:
                logger.info(f"Inserting final {len(all_accumulated_data)} accumulated records to database")
                db.insert_transaction_raw_rent_data(all_accumulated_data)
        
        # Final summary
        result.successful_requests = total_successful_requests
        result.failed_requests = total_failed_requests
        
        logger.info("🎉 OPTIMIZED Transaction Rent import completed!")
        logger.info(f"Templates processed: {result.templates_processed}")
        logger.info(f"Batches processed: {result.batches_processed}")
        logger.info(f"Total locations processed: {len(location_ids)}")
        logger.info(f"Successful requests: {result.successful_requests}")
        logger.info(f"Failed requests: {result.failed_requests}")
        if not self.config.dry_run:
            logger.info(f"Total records inserted into database: {result.total_records}")
        
        return result


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Import transaction rent data from Reidin API (Optimized)")
    
    # Required arguments
    parser.add_argument("--country-code", default="AE", help="Country code (default: AE)")
    
    # Optional arguments
    parser.add_argument("--max-locations", type=int, default=500, help="Maximum locations to process (default: 500)")
    parser.add_argument("--max-workers", type=int, default=16, help="Maximum number of worker threads (default: 16)")
    parser.add_argument("--requests-per-second", type=float, default=8.0, help="Maximum requests per second (default: 8.0)")
    parser.add_argument("--offset", type=int, default=0, help="Number of locations to skip from the beginning (default: 0)")
    parser.add_argument("--max-pages", type=int, default=20, help="Maximum pages per location (default: 20)")
    parser.add_argument("--locations-per-batch", type=int, default=2001, help="Locations per batch (default: 2001)")
    parser.add_argument("--max-accumulated-records", type=int, default=50000, help="Max records before DB insert (default: 50000)")
    parser.add_argument("--template-parallel-workers", type=int, default=4, help="Workers per template batch (default: 4)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument("--resume-scroll-id", type=str, help="Resume from specific scroll_id")
    parser.add_argument("--disable-adaptive-rate-limiting", action="store_true", help="Disable adaptive rate limiting (default: enabled)")
    
    args = parser.parse_args()
    
    # Handle adaptive rate limiting argument
    adaptive_rate_limiting = not args.disable_adaptive_rate_limiting
    
    # Create configuration
    config = OptimizedProcessingConfig(
        country_code=args.country_code,
        max_workers=args.max_workers,
        requests_per_second=args.requests_per_second,
        max_pages=args.max_pages,
        locations_per_batch=args.locations_per_batch,
        max_accumulated_records=args.max_accumulated_records,
        template_parallel_workers=args.template_parallel_workers,
        dry_run=args.dry_run,
        resume_scroll_id=args.resume_scroll_id,
        adaptive_rate_limiting=adaptive_rate_limiting
    )
    
    # Create importer and run
    importer = OptimizedTransactionRentImporter(config)
    
    try:
        result = importer.process_optimized_transaction_rent_batch(
            max_locations=args.max_locations,
            offset=args.offset
        )
        
        # Print final summary
        if result.failed_combinations:
            logger.warning(f"Failed combinations: {len(result.failed_combinations)}")
            for location_id, params in result.failed_combinations[:5]:  # Show first 5
                logger.warning(f"  - Location {location_id} with params {params}")
            if len(result.failed_combinations) > 5:
                logger.warning(f"  ... and {len(result.failed_combinations) - 5} more")
                
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise


if __name__ == "__main__":
    main()
