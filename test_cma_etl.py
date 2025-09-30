#!/usr/bin/env python3
"""
Test script for CMA ETL with dynamic schema
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from import_cma import CMAImporter
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_cma_etl():
    """Test the CMA ETL with a small sample"""
    try:
        # Create importer with test settings
        importer = CMAImporter(
            batch_size=100,  # Small batch for testing
            max_workers=4,   # Fewer workers for testing
            request_delay=1.0
        )
        
        logger.info("🧪 Starting CMA ETL test...")
        
        # Test with 5 properties, 4 combinations each
        importer.process_cma(
            country_code="AE",
            max_properties=5,
            max_combinations=4,  # Test with fewer combinations
            offset=0,
            dry_run=False
        )
        
        logger.info("✅ CMA ETL test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ CMA ETL test failed: {e}")
        raise

if __name__ == "__main__":
    test_cma_etl()
