import argparse
import logging
import sys
from typing import List, Dict, Any, Optional
from import_locations import LocationImporter
from import_properties import PropertyImporter
from import_property_details import PropertyDetailsImporter
from import_indicators_aliased import IndicatorAliasedImporter
from import_transactions_avg import TransactionsAvgImporter
from import_transaction_history import TransactionHistoryImporter
from import_transaction_sales import TransactionSalesImporter
from import_cma_sales import CMASalesImporter
from import_transactions_price import TransactionsPriceImporter
from import_transaction_list import TransactionListImporter
from import_transaction_rent import OptimizedTransactionRentImporter

logger = logging.getLogger(__name__)


class MasterImporter:
    def __init__(self):
        self.location_importer = LocationImporter()
        self.property_importer = PropertyImporter()
        self.property_details_importer = PropertyDetailsImporter()
        self.indicator_aliased_importer = IndicatorAliasedImporter()
        self.transactions_avg_importer = TransactionsAvgImporter()
        self.transaction_history_importer = TransactionHistoryImporter()
        self.transaction_sales_importer = TransactionSalesImporter()
        self.cma_sales_importer = CMASalesImporter()
        self.transactions_price_importer = TransactionsPriceImporter()
        self.transaction_list_importer = TransactionListImporter()
        self.transaction_rent_importer = OptimizedTransactionRentImporter()

    def import_all(
        self,
        country_code: str,
        limit: Optional[int] = None,
        dry_run: bool = False,
        skip_locations: bool = False,
        skip_properties: bool = False,
        skip_property_details: bool = False,
        skip_indicators: bool = False,
        skip_transactions_avg: bool = False,
        skip_transaction_history: bool = False,
        skip_transaction_rent: bool = False,
        skip_transaction_sales: bool = False,
        skip_cma_sales: bool = False,
        skip_transactions_price: bool = False,
        skip_transaction_list: bool = False,
    ) -> None:
        """Run all importers in the correct sequence."""

        logger.info("Starting master import for country: %s", country_code)

        # Step 1: Import locations (required for all other imports)
        if not skip_locations:
            logger.info("=== Step 1: Importing locations ===")
            try:
                self.location_importer.process_locations(country_code, limit, dry_run)
                logger.info("Location import completed successfully")
            except Exception as e:
                logger.error("Location import failed: %s", e)
                if not dry_run:
                    logger.error("Cannot continue without locations. Exiting.")
                    return
        else:
            logger.info("Skipping location import as requested")

        # Step 2: Import properties (requires locations)
        if not skip_properties:
            logger.info("=== Step 2: Importing properties ===")
            try:
                self.property_importer.process_properties(country_code, limit, dry_run)
                logger.info("Property import completed successfully")
            except Exception as e:
                logger.error("Property import failed: %s", e)
        else:
            logger.info("Skipping property import as requested")

        # Step 3: Import property details (requires properties)
        if not skip_property_details:
            logger.info("=== Step 3: Importing property details ===")
            try:
                self.property_details_importer.process_property_details(
                    country_code, limit, dry_run
                )
                logger.info("Property details import completed successfully")
            except Exception as e:
                logger.error("Property details import failed: %s", e)
        else:
            logger.info("Skipping property details import as requested")


        # Step 5: Import indicators (requires properties and locations)
        if not skip_indicators:
            logger.info("=== Step 5: Importing indicators ===")
            try:
                self.indicator_aliased_importer.process_indicators_aliased(
                    country_code, limit, dry_run
                )
                logger.info("Property-level indicators import completed successfully")
            except Exception as e:
                logger.error("Property-level indicators import failed: %s", e)

        else:
            logger.info("Skipping indicators import as requested")

        # Step 6: Import transactions average data (requires locations)
        if not skip_transactions_avg:
            logger.info("=== Step 6: Importing transactions average data ===")
            try:
                self.transactions_avg_importer.process_transactions_avg(
                    country_code, "Apartment", "sales", "aed", "imp", None, limit, dry_run
                )
                logger.info("Transactions average import completed successfully")
            except Exception as e:
                logger.error("Transactions average import failed: %s", e)
        else:
            logger.info("Skipping transactions average import as requested")

        # Step 7: Import transaction history data (optional - requires specific areas and land numbers)
        if not skip_transaction_history:
            logger.info("=== Step 7: Importing transaction history data ===")
            try:
                # Example: Import for Saih Shuaib 2, land 248
                self.transaction_history_importer.process_transaction_history(
                    country_code=country_code,
                    municipal_property_type="Unit",
                    municipal_areas=["Saih Shuaib 2"],
                    land_numbers=["248"],
                    currency="aed",
                    measurement="imp",
                    max_pages=5,  # Limit pages for demo
                    dry_run=dry_run
                )
                logger.info("Transaction history import completed successfully")
            except Exception as e:
                logger.error("Transaction history import failed: %s", e)
        else:
            logger.info("Skipping transaction history import as requested")

        # Step 8: Import transaction rent data (optional - requires locations)
        if not skip_transaction_rent:
            logger.info("=== Step 8: Importing transaction rent data ===")
            try:
                # Import transaction rent data using optimized importer
                self.transaction_rent_importer.process_optimized_transaction_rent_batch(
                    max_locations=500,
                    offset=0
                )
                logger.info("Transaction rent import completed successfully")
            except Exception as e:
                logger.error("Transaction rent import failed: %s", e)
        else:
            logger.info("Skipping transaction rent import as requested")

        # Step 9: Import transaction sales data (optional - requires locations)
        if not skip_transaction_sales:
            logger.info("=== Step 9: Importing transaction sales data ===")
            try:
                # Import 500 records as requested
                self.transaction_sales_importer.process_transaction_sales(
                    country_code=country_code,
                    measurement="imp",
                    currency="aed",
                    transaction_type="Sales - Ready",
                    max_locations=500,
                    max_pages=5,
                    page_size=1000,
                    dry_run=dry_run
                )
                logger.info("Transaction sales import completed successfully")
            except Exception as e:
                logger.error("Transaction sales import failed: %s", e)
        else:
            logger.info("Skipping transaction sales import as requested")

        # Step 10: Import CMA sales data (optional - requires properties)
        if not skip_cma_sales:
            logger.info("=== Step 10: Importing CMA sales data ===")
            try:
                # Import CMA sales data for properties
                self.cma_sales_importer.process_cma_sales(
                    country_code=country_code,
                    max_properties=500,
                    max_combinations_per_property=10,
                    request_delay=0.25,
                    dry_run=dry_run
                )
                logger.info("CMA sales import completed successfully")
            except Exception as e:
                logger.error("CMA sales import failed: %s", e)
        else:
            logger.info("Skipping CMA sales import as requested")

        # Step 11: Import transactions price data (optional - requires locations)
        if not skip_transactions_price:
            logger.info("=== Step 11: Importing transactions price data ===")
            try:
                # Import transactions price data for locations
                self.transactions_price_importer.process_transactions_price(
                    country_code=country_code,
                    max_locations=50,
                    max_combinations_per_location=20,
                    request_delay=0.25,
                    dry_run=dry_run
                )
                logger.info("Transactions price import completed successfully")
            except Exception as e:
                logger.error("Transactions price import failed: %s", e)
        else:
            logger.info("Skipping transactions price import as requested")

        # Step 12: Import transaction list data (optional - requires locations)
        if not skip_transaction_list:
            logger.info("=== Step 12: Importing transaction list data ===")
            try:
                # Import transaction list data for locations with batch processing
                self.transaction_list_importer.process_transaction_list_etl(
                    country_code=country_code,
                    max_locations=500,
                    dry_run=dry_run
                )
                logger.info("Transaction list import completed successfully")
            except Exception as e:
                logger.error("Transaction list import failed: %s", e)
        else:
            logger.info("Skipping transaction list import as requested")

        logger.info("=== Master import completed ===")


def main():
    parser = argparse.ArgumentParser(
        description="Master importer for all Reidin data tables."
    )
    parser.add_argument("--country-code", default="ae")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--skip-locations", action="store_true", default=False)
    parser.add_argument("--skip-properties", action="store_true", default=False)
    parser.add_argument("--skip-property-details", action="store_true", default=False)
    parser.add_argument("--skip-indicators", action="store_true", default=False)
    parser.add_argument("--skip-transactions-avg", action="store_true", default=False)
    parser.add_argument("--skip-transaction-history", action="store_true", default=False)
    parser.add_argument("--skip-transaction-rent", action="store_true", default=False)
    parser.add_argument("--skip-transaction-sales", action="store_true", default=False)
    parser.add_argument("--skip-cma-sales", action="store_true", default=False)
    parser.add_argument("--skip-transactions-price", action="store_true", default=False)
    parser.add_argument("--skip-transaction-list", action="store_true", default=False)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("./reidin_master_import.log"),
        ],
    )

    try:
        logger.info("Starting master import for: %s", args.country_code)
        MasterImporter().import_all(
            args.country_code,
            args.limit,
            args.dry_run,
            args.skip_locations,
            args.skip_properties,
            args.skip_property_details,
            args.skip_indicators,
            args.skip_transactions_avg,
            args.skip_transaction_history,
            args.skip_transaction_rent,
            args.skip_transaction_sales,
            args.skip_cma_sales,
            args.skip_transactions_price,
            args.skip_transaction_list,
        )
    except Exception:
        logger.exception("Master import failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
