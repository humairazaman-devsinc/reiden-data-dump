#!/usr/bin/env python3
"""
Transaction History Importer

This script imports transaction history data from the Reidin API
and stores it in the database following the same ETL pattern as other importers.
"""

import argparse
import logging
import time
from typing import List, Dict, Any, Optional

from database import DatabaseManager
from api_client import ReidinAPIClient
from processors import DataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reidin_transaction_history_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TransactionHistoryImporter:
    """Importer for transaction history data from Reidin API"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = ReidinAPIClient()
    
    def process_transaction_history(
        self,
        country_code: str,
        municipal_property_type: str,
        municipal_areas: List[str],
        land_numbers: List[str],
        currency: str = "aed",
        measurement: str = "imp",
        transaction_type: Optional[str] = None,
        building_number: Optional[str] = None,
        building_name: Optional[str] = None,
        unit: Optional[str] = None,
        floor: Optional[str] = None,
        unit_min_size: Optional[float] = None,
        unit_max_size: Optional[float] = None,
        max_pages: int = 10,
        request_delay: float = 0.25,
        dry_run: bool = False
    ) -> None:
        """
        Process transaction history data for given municipal areas and land numbers
        
        Args:
            country_code: Country code (e.g., "AE")
            municipal_property_type: Property type (Unit, Building, Land)
            municipal_areas: List of municipal areas to query
            land_numbers: List of land numbers to query
            currency: Currency code (default: "aed")
            measurement: Measurement unit (default: "imp")
            transaction_type: Optional transaction type filter
            building_number: Optional building number filter
            building_name: Optional building name filter
            unit: Optional unit filter
            floor: Optional floor filter
            unit_min_size: Optional minimum unit size filter
            unit_max_size: Optional maximum unit size filter
            max_pages: Maximum pages to fetch per area/land combination
            request_delay: Delay between API requests in seconds
            dry_run: If True, don't save to database
        """
        logger.info("Starting transaction history import for: %s", country_code)
        logger.info("Parameters: municipal_property_type=%s, currency=%s, measurement=%s, max_pages=%d", 
                   municipal_property_type, currency, measurement, max_pages)
        
        if not municipal_areas or not land_numbers:
            logger.error("Please provide at least one municipal area and one land number")
            return
        
        transaction_history_data = []
        successful_combinations = 0
        failed_combinations = 0
        
        try:
            with self.api_client as client:
                for area in municipal_areas:
                    for land in land_numbers:
                        logger.info("Processing area=%s, land=%s", area, land)
                        combination_data = []
                        
                        for page in range(1, max_pages + 1):
                            try:
                                # Build parameters for the API call
                                params = {
                                    "municipal_property_type": municipal_property_type,
                                    "municipal_area": area,
                                    "land_number": land,
                                    "currency": currency,
                                    "measurement": measurement,
                                    "page_number": page,
                                }
                                
                                # Add optional filters
                                if transaction_type:
                                    params["transaction_type"] = transaction_type
                                if building_number:
                                    params["building_number"] = building_number
                                if building_name:
                                    params["building_name"] = building_name
                                if unit:
                                    params["unit"] = unit
                                if floor:
                                    params["floor"] = floor
                                if unit_min_size:
                                    params["unit_min_size"] = unit_min_size
                                if unit_max_size:
                                    params["unit_max_size"] = unit_max_size

                                raw = client.fetch_transaction_history(country_code, params)

                                if not isinstance(raw, dict) or "results" not in raw:
                                    logger.warning(
                                        "Unexpected API response for area=%s, land=%s, page=%d: %r",
                                        area, land, page, raw,
                                    )
                                    break

                                results = raw.get("results", [])
                                logger.info(
                                    "Found %d transaction history records for area=%s, land=%s, page=%d", 
                                    len(results), area, land, page
                                )

                                if results:
                                    combination_data.extend(results)
                                else:
                                    # Stop paging on first empty page
                                    break
                                
                                # Rate limiting
                                time.sleep(request_delay)
                                
                            except Exception as e:
                                logger.error("Failed to fetch data for area=%s, land=%s, page=%d: %s", 
                                           area, land, page, e)
                                break
                        
                        if combination_data:
                            # Process the data for this combination
                            processed = DataProcessor.process_transaction_history_data(
                                combination_data, 
                                area, 
                                land,
                                transaction_type,
                                building_number,
                                building_name,
                                unit,
                                floor,
                                unit_min_size,
                                unit_max_size
                            )
                            
                            if processed:
                                if not dry_run:
                                    self.db.insert_transaction_history_data(processed)
                                    logger.info(
                                        "Saved %d transaction history records for area=%s, land=%s",
                                        len(processed), area, land
                                    )
                                else:
                                    logger.info(
                                        "DRY RUN: Would save %d transaction history records for area=%s, land=%s",
                                        len(processed), area, land
                                    )
                                
                                transaction_history_data.extend(processed)
                                successful_combinations += 1
                            else:
                                logger.warning("No processed data for area=%s, land=%s", area, land)
                                failed_combinations += 1
                        else:
                            logger.warning("No data found for area=%s, land=%s", area, land)
                            failed_combinations += 1
                        
                        logger.info("Completed area=%s, land=%s: %d records", area, land, len(combination_data))
        
        except Exception as e:
            logger.error("Transaction history import failed: %s", e)
            raise
        
        # Final summary
        logger.info("Transaction history import completed")
        logger.info("Total records processed: %d", len(transaction_history_data))
        logger.info("Successful combinations: %d, Failed combinations: %d", 
                   successful_combinations, failed_combinations)
        
        if not transaction_history_data:
            logger.warning("No transaction history data retrieved.")
            return

        # Collect validation stats for final summary
        try:
            if transaction_history_data:
                stats = {"total": len(transaction_history_data), "processed": "during_import"}
                logger.info("Final validation stats: %s", stats)
            else:
                logger.warning("No processed data available for validation")
        except Exception as e:
            logger.error("Failed to collect validation stats: %s", e)

        logger.info("Import completed successfully. All data has been saved to database.")


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Import transaction history data from Reidin API")
    
    # Required arguments
    parser.add_argument("--country-code", required=True, help="Country code (e.g., AE)")
    parser.add_argument("--municipal-property-type", required=True, 
                       choices=["Unit", "Building", "Land"], 
                       help="Municipal property type")
    parser.add_argument("--municipal-areas", required=True, nargs="+", 
                       help="Municipal areas to query (space-separated)")
    parser.add_argument("--land-numbers", required=True, nargs="+", 
                       help="Land numbers to query (space-separated)")
    
    # Optional arguments
    parser.add_argument("--currency", default="aed", help="Currency code (default: aed)")
    parser.add_argument("--measurement", default="imp", help="Measurement unit (default: imp)")
    parser.add_argument("--transaction-type", help="Transaction type filter")
    parser.add_argument("--building-number", help="Building number filter")
    parser.add_argument("--building-name", help="Building name filter")
    parser.add_argument("--unit", help="Unit filter")
    parser.add_argument("--floor", help="Floor filter")
    parser.add_argument("--unit-min-size", type=float, help="Minimum unit size filter")
    parser.add_argument("--unit-max-size", type=float, help="Maximum unit size filter")
    parser.add_argument("--max-pages", type=int, default=10, help="Maximum pages per combination (default: 10)")
    parser.add_argument("--request-delay", type=float, default=0.25, help="Delay between requests in seconds (default: 0.25)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    
    args = parser.parse_args()
    
    # Create importer and run
    importer = TransactionHistoryImporter()
    importer.process_transaction_history(
        country_code=args.country_code,
        municipal_property_type=args.municipal_property_type,
        municipal_areas=args.municipal_areas,
        land_numbers=args.land_numbers,
        currency=args.currency,
        measurement=args.measurement,
        transaction_type=args.transaction_type,
        building_number=args.building_number,
        building_name=args.building_name,
        unit=args.unit,
        floor=args.floor,
        unit_min_size=args.unit_min_size,
        unit_max_size=args.unit_max_size,
        max_pages=args.max_pages,
        request_delay=args.request_delay,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
