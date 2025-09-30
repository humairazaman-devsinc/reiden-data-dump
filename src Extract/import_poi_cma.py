#!/usr/bin/env python3
"""
POI CMA Importer

This script imports POI CMA data from the Reidin API for all properties.
It processes the /poi/poi_cma endpoint with required parameters:
- measurement: int/imp
- lang: tr/en
- property_id: from all properties in database

The script follows the same pattern as transaction/raw_rent ETL with:
1. Batch processing of properties
2. Parallel API calls with rate limiting
3. Data flattening and normalization
4. Bulk database insertion
"""

import argparse
import logging
import sys
import time
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from contextlib import contextmanager

# Ensure the parent directory is on sys.path when running this file directly
from pathlib import Path
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
        logging.FileHandler('reidin_poi_cma_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class POICMAProcessingConfig:
    """Configuration for POI CMA processing"""
    max_workers: int = 8
    requests_per_second: float = 2.0
    batch_size: int = 100
    dry_run: bool = False
    max_properties: Optional[int] = None
    offset: int = 0
    # New configuration for template-based processing
    workers_per_template: int = 2  # Workers dedicated to each template
    generate_csv_files: bool = True  # Generate separate CSV files for each combination
    csv_output_dir: str = "poi_cma_csv_output"  # Directory for CSV files
    csv_only_mode: bool = False  # If True, only generate CSV files, don't load to database


class POICMAResponseFlattener:
    """Handles flattening of POI CMA API responses into database-ready format"""
    
    def flatten_response(self, item: Dict[str, Any], property_id: str, measurement: str, lang: str, lat: str = None, lon: str = None) -> Dict[str, Any]:
        """Flatten nested API response into database-ready format based on actual API structure"""
        processed = {
            "property_id": property_id,
            "measurement": measurement,
            "lang": lang,
            "lat": lat,
            "lon": lon,
            "raw_data": item
        }
        
        # Handle direct POI fields from actual API response
        # API response structure: subtype_name, type_name, name, review, rating, distance
        for key in ["subtype_name", "type_name", "name", "review", "rating", "distance"]:
            if key in item:
                # Map API field names to database column names
                db_key = f"poi_{key}"
                processed[db_key] = item[key]
        
        return processed


@dataclass
class ParameterTemplate:
    """Template for API requests with specific parameters"""
    measurement: str
    lang: str
    template_id: str  # Unique identifier for this template
    
    def __post_init__(self):
        if not self.template_id:
            self.template_id = f"{self.measurement}_{self.lang}"


class POICMAImporter:
    """Importer for POI CMA data from Reidin API"""
    
    def __init__(self, config: POICMAProcessingConfig):
        self.config = config
        self.measurements = ["int", "imp"]
        self.languages = ["en"]  # Skip Turkish templates - they return 0 records
        self.min_interval_sec = 1.0 / config.requests_per_second
        self.flattener = POICMAResponseFlattener()
        
        # Create CSV output directory if needed
        if self.config.generate_csv_files:
            import os
            os.makedirs(self.config.csv_output_dir, exist_ok=True)
        
    @contextmanager
    def _get_resources(self):
        """Context manager for database and API client resources"""
        db = DatabaseManager()
        api_client = ReidinAPIClient()
        try:
            yield db, api_client
        finally:
            # Cleanup if needed
            pass

    def get_properties(self, db: DatabaseManager, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """Get properties from the database with proper offset support"""
        try:
            properties = db.get_properties(limit=limit, offset=offset)
            logger.info(f"Retrieved {len(properties)} properties from database (offset: {offset}, limit: {limit})")
            return properties
        except Exception as e:
            logger.error(f"Failed to get properties: {e}")
            return []

    def generate_parameter_templates(self) -> List[ParameterTemplate]:
        """Generate separate templates for each parameter combination"""
        templates = []
        for measurement in self.measurements:
            for lang in self.languages:
                template = ParameterTemplate(
                    measurement=measurement,
                    lang=lang,
                    template_id=f"{measurement}_{lang}"
                )
                templates.append(template)
        return templates

    def _safe_api_call(self, api_client: ReidinAPIClient, property_id: str, params: Dict[str, str]) -> Tuple[Optional[Dict[str, Any]], bool]:
        """Safe API call with error handling"""
        try:
            payload = api_client.fetch_poi_cma(
                property_id=property_id,
                measurement=params["measurement"],
                lang=params["lang"]
            )
            return payload, True
        except Exception as e:
            logger.warning(f"API call failed for property {property_id} with params {params}: {e}")
            return None, False

    def _process_api_response(self, results: List[Dict[str, Any]], property_id: str, measurement: str, lang: str, lat: str = None, lon: str = None) -> List[Dict[str, Any]]:
        """Process API response data into database-ready format"""
        processed_data = []
        for item in results:
            if isinstance(item, dict):
                processed_item = self.flattener.flatten_response(item, property_id, measurement, lang, lat, lon)
                processed_data.append(processed_item)
        return processed_data

    def _write_to_csv(self, data: List[Dict[str, Any]], template: ParameterTemplate) -> str:
        """Write data to CSV file for a specific template"""
        if not data:
            return None
            
        import csv
        import os
        
        filename = f"poi_cma_{template.template_id}.csv"
        filepath = os.path.join(self.config.csv_output_dir, filename)
        
        # Define CSV columns in the same order as database schema
        fieldnames = [
            'property_id', 'measurement', 'lang', 'lat', 'lon',
            'poi_subtype_name', 'poi_type_name', 'poi_name', 'poi_review', 
            'poi_rating', 'poi_distance', 'raw_data'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for record in data:
                # Convert raw_data to JSON string for CSV
                if 'raw_data' in record and record['raw_data']:
                    import json
                    record['raw_data'] = json.dumps(record['raw_data'])
                writer.writerow(record)
        
        logger.info(f"✅ Written {len(data)} records to CSV: {filepath}")
        return filepath

    def _fetch_property_poi_data(self, api_client: ReidinAPIClient, property_id: str, params: Dict[str, str]) -> Tuple[int, List[Dict[str, Any]], bool]:
        """
        Fetch POI data for a single property with specific parameters
        
        Returns:
            Tuple of (total_records, processed_data, success_flag)
        """
        try:
            # Use safe API call helper
            payload, success = self._safe_api_call(api_client, property_id, params)
            if not success:
                return 0, [], False

            results = payload.get("results") if isinstance(payload, dict) else None
            
            if isinstance(results, list) and results:
                if not self.config.dry_run:
                    processed_data = self._process_api_response(results, property_id, params["measurement"], params["lang"])
                    return len(results), processed_data, True
                else:
                    return len(results), [], True
            else:
                return 0, [], True

        except Exception as e:
            logger.warning(f"Failed to fetch POI data for property {property_id} with params {params}: {e}")
            return 0, [], False

    def _process_template_parallel(self, api_client: ReidinAPIClient, template: ParameterTemplate, property_ids: List[str]) -> Tuple[int, List[Dict[str, Any]], int, int]:
        """
        Process all properties for a specific template in parallel
        
        Returns:
            Tuple of (total_records, all_processed_data, successful_properties, failed_properties)
        """
        total_records = 0
        all_processed_data = []
        successful_properties = 0
        failed_properties = 0
        
        logger.info(f"🔄 Processing template {template.template_id} with {len(property_ids)} properties using {self.config.workers_per_template} workers")
        
        # Process properties in parallel for this template
        with ThreadPoolExecutor(max_workers=self.config.workers_per_template) as executor:
            # Submit all property tasks for this template
            future_to_property = {
                executor.submit(self._fetch_property_poi_data, api_client, property_id, {
                    "measurement": template.measurement,
                    "lang": template.lang
                }): property_id
                for property_id in property_ids
            }
            
            # Process completed property tasks
            for future in as_completed(future_to_property):
                property_id = future_to_property[future]
                try:
                    records, processed_data, success = future.result()
                    total_records += records
                    
                    if success:
                        successful_properties += 1
                        all_processed_data.extend(processed_data)
                    else:
                        failed_properties += 1
                        
                    # Rate limiting between property requests
                    time.sleep(self.min_interval_sec)
                    
                except Exception as e:
                    logger.error(f"Task failed for property {property_id} in template {template.template_id}: {e}")
                    failed_properties += 1
        
        logger.info(f"✅ Template {template.template_id} completed: {successful_properties} successful, {failed_properties} failed, {len(all_processed_data)} records")
        return total_records, all_processed_data, successful_properties, failed_properties

    def process_poi_cma_batch(self, max_properties: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Main method to process POI CMA data for all properties
        
        Args:
            max_properties: Maximum number of properties to process
            offset: Number of properties to skip from the beginning
            
        Returns:
            Dict with statistics about the operation
        """
        result = {
            "total_records": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "properties_processed": 0,
            "parameter_combinations": 0
        }
        
        logger.info("🚀 Starting POI CMA data import")
        logger.info(f"Max properties: {max_properties}")
        logger.info(f"Max workers: {self.config.max_workers}")
        logger.info(f"Requests per second: {self.config.requests_per_second}")
        logger.info(f"Batch size: {self.config.batch_size}")
        logger.info(f"Dry run: {self.config.dry_run}")
        
        if self.config.dry_run:
            logger.info("🧪 DRY RUN MODE - No data will be saved to database")
        
        with self._get_resources() as (db, api_client):
            # 1) Get properties from the database
            properties = self.get_properties(db, max_properties, offset)
            if not properties:
                logger.error("No properties found in DB; exiting.")
                return result
            
            property_ids = [prop["id"] for prop in properties]
            result["properties_processed"] = len(property_ids)
            
            # 2) Generate separate templates for each parameter combination
            templates = self.generate_parameter_templates()
            result["parameter_combinations"] = len(templates)
            
            logger.info(f"Processing {len(property_ids)} properties with {len(templates)} parameter templates")
            logger.info(f"Total API calls to make: {len(property_ids) * len(templates)}")
            logger.info(f"Workers per template: {self.config.workers_per_template}")
            
            # 3) Process each template in parallel with dedicated workers
            all_accumulated_data = []
            total_successful_requests = 0
            total_failed_requests = 0
            csv_files_created = []
            
            # Process templates sequentially (one at a time) to avoid API overload
            for template in templates:
                try:
                    records, processed_data, successful_props, failed_props = self._process_template_parallel(api_client, template, property_ids)
                    result["total_records"] += records
                    total_successful_requests += successful_props
                    total_failed_requests += failed_props
                    
                    # Save to database if not dry run and not in CSV-only mode
                    if processed_data and not self.config.dry_run and not self.config.csv_only_mode:
                        try:
                            db.insert_poi_cma_data(processed_data)
                            logger.info(f"✅ Saved template {template.template_id} to database: {len(processed_data)} POI records")
                            all_accumulated_data.extend(processed_data)
                        except Exception as e:
                            logger.error(f"Failed to save template {template.template_id} to database: {e}")
                    elif processed_data and (self.config.dry_run or self.config.csv_only_mode):
                        if self.config.csv_only_mode:
                            logger.info(f"CSV-only mode: skipping database save for template {template.template_id}: {len(processed_data)} POI records")
                        else:
                            logger.info(f"Dry run: would save template {template.template_id} to database: {len(processed_data)} POI records")
                        all_accumulated_data.extend(processed_data)
                    
                    # Generate CSV file for this template
                    if processed_data and self.config.generate_csv_files:
                        try:
                            csv_file = self._write_to_csv(processed_data, template)
                            if csv_file:
                                csv_files_created.append(csv_file)
                        except Exception as e:
                            logger.error(f"Failed to create CSV for template {template.template_id}: {e}")
                    
                except Exception as e:
                    logger.error(f"Template processing failed for {template.template_id}: {e}")
                    total_failed_requests += len(property_ids)
        
        # Final summary
        result["successful_requests"] = total_successful_requests
        result["failed_requests"] = total_failed_requests
        result["csv_files_created"] = len(csv_files_created)
        
        logger.info("🎉 POI CMA import completed!")
        logger.info(f"Properties processed: {result['properties_processed']}")
        logger.info(f"Parameter templates: {result['parameter_combinations']}")
        logger.info(f"Successful requests: {result['successful_requests']}")
        logger.info(f"Failed requests: {result['failed_requests']}")
        if not self.config.dry_run and not self.config.csv_only_mode:
            logger.info(f"Total records inserted into database: {result['total_records']}")
        elif self.config.csv_only_mode:
            logger.info(f"CSV-only mode: {result['total_records']} records processed (not saved to database)")
        if self.config.generate_csv_files and csv_files_created:
            logger.info(f"CSV files created: {len(csv_files_created)}")
            for csv_file in csv_files_created:
                logger.info(f"  📄 {csv_file}")
        
        return result


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Import POI CMA data from Reidin API")
    
    # Optional arguments
    parser.add_argument("--max-properties", type=int, default=100, help="Maximum properties to process (default: 100)")
    parser.add_argument("--max-workers", type=int, default=8, help="Maximum number of worker threads (default: 8)")
    parser.add_argument("--requests-per-second", type=float, default=2.0, help="Maximum requests per second (default: 2.0)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    parser.add_argument("--offset", type=int, default=0, help="Number of properties to skip from the beginning (default: 0)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument("--workers-per-template", type=int, default=2, help="Workers dedicated to each template (default: 2)")
    parser.add_argument("--generate-csv", action="store_true", help="Generate separate CSV files for each template")
    parser.add_argument("--csv-output-dir", type=str, default="poi_cma_csv_output", help="Directory for CSV files (default: poi_cma_csv_output)")
    parser.add_argument("--csv-only", action="store_true", help="Only generate CSV files, don't load to database")
    
    args = parser.parse_args()
    
    # Create configuration
    config = POICMAProcessingConfig(
        max_workers=args.max_workers,
        requests_per_second=args.requests_per_second,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        max_properties=args.max_properties,
        offset=args.offset,
        workers_per_template=args.workers_per_template,
        generate_csv_files=args.generate_csv,
        csv_output_dir=args.csv_output_dir,
        csv_only_mode=args.csv_only
    )
    
    # Create importer and run
    importer = POICMAImporter(config)
    
    try:
        result = importer.process_poi_cma_batch(
            max_properties=args.max_properties,
            offset=args.offset
        )
        
        # Print final summary
        logger.info("Final Results:")
        logger.info(f"  Properties processed: {result['properties_processed']}")
        logger.info(f"  Parameter templates: {result['parameter_combinations']}")
        logger.info(f"  Successful requests: {result['successful_requests']}")
        logger.info(f"  Failed requests: {result['failed_requests']}")
        logger.info(f"  Total records: {result['total_records']}")
        if 'csv_files_created' in result:
            logger.info(f"  CSV files created: {result['csv_files_created']}")
                
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise


if __name__ == "__main__":
    main()
