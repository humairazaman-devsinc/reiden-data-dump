#!/usr/bin/env python3
"""
Rental Yield Analyzer

This module provides functionality to compare project rental yields with area averages,
filterable by bedroom type (1BR, 2BR, 3BR) or overall project.

Features:
- Calculate actual rental yield for specific projects
- Calculate average area rental yield for comparison
- Filter by bedroom type (1BR, 2BR, 3BR) or overall project
- Generate rental performance snapshots
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

# Ensure the parent directory is on sys.path when running this file directly
PARENT_DIR = Path(__file__).resolve().parents[1]
if str(PARENT_DIR) not in sys.path:
    sys.path.append(str(PARENT_DIR))

from src.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('rental_yield_analysis.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class RentalYieldData:
    """Data structure for rental yield information"""
    property_id: int
    property_name: str
    location_id: int
    location_name: str
    bedroom_type: Optional[int]
    actual_yield: float
    area_average_yield: float
    annual_rental_income: float
    property_value: float
    transaction_count: int
    area_transaction_count: int


@dataclass
class RentalPerformanceSnapshot:
    """Rental performance snapshot for a project"""
    property_id: int
    property_name: str
    location_name: str
    bedroom_filter: Optional[int]  # None for "All", 1, 2, 3 for specific bedroom types
    actual_rental_yield: float
    area_average_rental_yield: float
    yield_difference: float
    performance_ratio: float  # actual / area_average
    data_quality_score: float  # Based on transaction counts


class RentalYieldAnalyzer:
    """Analyzer for rental yield comparisons with bedroom filtering"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def calculate_rental_yield(self, annual_rental_income: float, property_value: float) -> float:
        """
        Calculate rental yield percentage
        
        Args:
            annual_rental_income: Annual rental income
            property_value: Property value/sale price
            
        Returns:
            Rental yield as percentage (e.g., 7.8 for 7.8%)
        """
        if property_value <= 0:
            return 0.0
        
        yield_percentage = (annual_rental_income / property_value) * 100
        return round(yield_percentage, 2)
    
    def get_project_rental_data(self, property_id: int, bedroom_type: Optional[int] = None) -> Dict[str, Any]:
        """
        Get rental data for a specific project
        
        Args:
            property_id: Property ID
            bedroom_type: Bedroom type filter (1, 2, 3) or None for all
            
        Returns:
            Dictionary with rental data
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build bedroom filter condition
                    bedroom_filter = ""
                    if bedroom_type is not None:
                        bedroom_filter = "AND attr_no_of_rooms = %s"
                    
                    # Get rental data from transaction_raw_rent
                    rental_query = f"""
                        SELECT 
                            AVG(price) as avg_monthly_rent,
                            COUNT(*) as transaction_count,
                            AVG(price_per_size) as avg_rent_per_sqm
                        FROM transaction_raw_rent 
                        WHERE prop_property_id = %s 
                        AND price IS NOT NULL 
                        AND price > 0
                        {bedroom_filter}
                    """
                    
                    params = [property_id]
                    if bedroom_type is not None:
                        params.append(bedroom_type)
                    
                    cur.execute(rental_query, params)
                    rental_result = cur.fetchone()
                    
                    if not rental_result or rental_result[0] is None:
                        return {
                            'avg_monthly_rent': 0,
                            'transaction_count': 0,
                            'avg_rent_per_sqm': 0
                        }
                    
                    return {
                        'avg_monthly_rent': float(rental_result[0]) if rental_result[0] else 0,
                        'transaction_count': rental_result[1] if rental_result[1] else 0,
                        'avg_rent_per_sqm': float(rental_result[2]) if rental_result[2] else 0
                    }
                    
        except Exception as e:
            logger.error(f"Error getting project rental data for property {property_id}: {e}")
            return {
                'avg_monthly_rent': 0,
                'transaction_count': 0,
                'avg_rent_per_sqm': 0
            }
    
    def get_area_rental_data(self, location_id: int, bedroom_type: Optional[int] = None) -> Dict[str, Any]:
        """
        Get rental data for an area/location
        
        Args:
            location_id: Location ID
            bedroom_type: Bedroom type filter (1, 2, 3) or None for all
            
        Returns:
            Dictionary with area rental data
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build bedroom filter condition
                    bedroom_filter = ""
                    if bedroom_type is not None:
                        bedroom_filter = "AND attr_no_of_rooms = %s"
                    
                    # Get rental data from transaction_raw_rent for the area
                    rental_query = f"""
                        SELECT 
                            AVG(price) as avg_monthly_rent,
                            COUNT(*) as transaction_count,
                            AVG(price_per_size) as avg_rent_per_sqm
                        FROM transaction_raw_rent 
                        WHERE loc_location_id = %s 
                        AND price IS NOT NULL 
                        AND price > 0
                        {bedroom_filter}
                    """
                    
                    params = [location_id]
                    if bedroom_type is not None:
                        params.append(bedroom_type)
                    
                    cur.execute(rental_query, params)
                    rental_result = cur.fetchone()
                    
                    if not rental_result or rental_result[0] is None:
                        return {
                            'avg_monthly_rent': 0,
                            'transaction_count': 0,
                            'avg_rent_per_sqm': 0
                        }
                    
                    return {
                        'avg_monthly_rent': float(rental_result[0]) if rental_result[0] else 0,
                        'transaction_count': rental_result[1] if rental_result[1] else 0,
                        'avg_rent_per_sqm': float(rental_result[2]) if rental_result[2] else 0
                    }
                    
        except Exception as e:
            logger.error(f"Error getting area rental data for location {location_id}: {e}")
            return {
                'avg_monthly_rent': 0,
                'transaction_count': 0,
                'avg_rent_per_sqm': 0
            }
    
    def get_project_sales_data(self, property_id: int, bedroom_type: Optional[int] = None) -> Dict[str, Any]:
        """
        Get sales data for a specific project to determine property value
        
        Args:
            property_id: Property ID
            bedroom_type: Bedroom type filter (1, 2, 3) or None for all
            
        Returns:
            Dictionary with sales data
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build bedroom filter condition
                    bedroom_filter = ""
                    if bedroom_type is not None:
                        bedroom_filter = "AND bedroom = %s"
                    
                    # Get sales data from transaction_sales
                    sales_query = f"""
                        SELECT 
                            AVG(price) as avg_sale_price,
                            COUNT(*) as transaction_count
                        FROM transaction_sales 
                        WHERE property_id = %s 
                        AND price IS NOT NULL 
                        AND price > 0
                        {bedroom_filter}
                    """
                    
                    params = [property_id]
                    if bedroom_type is not None:
                        params.append(bedroom_type)
                    
                    cur.execute(sales_query, params)
                    sales_result = cur.fetchone()
                    
                    if not sales_result or sales_result[0] is None:
                        return {
                            'avg_sale_price': 0,
                            'transaction_count': 0
                        }
                    
                    return {
                        'avg_sale_price': float(sales_result[0]) if sales_result[0] else 0,
                        'transaction_count': sales_result[1] if sales_result[1] else 0
                    }
                    
        except Exception as e:
            logger.error(f"Error getting project sales data for property {property_id}: {e}")
            return {
                'avg_sale_price': 0,
                'transaction_count': 0
            }
    
    def get_area_sales_data(self, location_id: int, bedroom_type: Optional[int] = None) -> Dict[str, Any]:
        """
        Get sales data for an area/location to determine average property value
        
        Args:
            location_id: Location ID
            bedroom_type: Bedroom type filter (1, 2, 3) or None for all
            
        Returns:
            Dictionary with area sales data
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build bedroom filter condition
                    bedroom_filter = ""
                    if bedroom_type is not None:
                        bedroom_filter = "AND bedroom = %s"
                    
                    # Get sales data from transaction_sales for the area
                    sales_query = f"""
                        SELECT 
                            AVG(price) as avg_sale_price,
                            COUNT(*) as transaction_count
                        FROM transaction_sales 
                        WHERE location_id = %s 
                        AND price IS NOT NULL 
                        AND price > 0
                        {bedroom_filter}
                    """
                    
                    params = [location_id]
                    if bedroom_type is not None:
                        params.append(bedroom_type)
                    
                    cur.execute(sales_query, params)
                    sales_result = cur.fetchone()
                    
                    if not sales_result or sales_result[0] is None:
                        return {
                            'avg_sale_price': 0,
                            'transaction_count': 0
                        }
                    
                    return {
                        'avg_sale_price': float(sales_result[0]) if sales_result[0] else 0,
                        'transaction_count': sales_result[1] if sales_result[1] else 0
                    }
                    
        except Exception as e:
            logger.error(f"Error getting area sales data for location {location_id}: {e}")
            return {
                'avg_sale_price': 0,
                'transaction_count': 0
            }
    
    def get_property_info(self, property_id: int) -> Dict[str, Any]:
        """Get basic property information"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT p.id, p.name, p.location_id, l.location_name
                        FROM property p
                        LEFT JOIN location l ON p.location_id = l.location_id
                        WHERE p.id = %s
                    """, [property_id])
                    
                    result = cur.fetchone()
                    if result:
                        return {
                            'property_id': result[0],
                            'property_name': result[1],
                            'location_id': result[2],
                            'location_name': result[3]
                        }
                    return {}
                    
        except Exception as e:
            logger.error(f"Error getting property info for {property_id}: {e}")
            return {}
    
    def analyze_rental_yield(self, property_id: int, bedroom_type: Optional[int] = None) -> Optional[RentalYieldData]:
        """
        Analyze rental yield for a specific property with bedroom filtering
        
        Args:
            property_id: Property ID to analyze
            bedroom_type: Bedroom type filter (1, 2, 3) or None for all
            
        Returns:
            RentalYieldData object or None if insufficient data
        """
        try:
            # Get property information
            property_info = self.get_property_info(property_id)
            if not property_info:
                logger.warning(f"Property {property_id} not found")
                return None
            
            # Get project rental data
            project_rental = self.get_project_rental_data(property_id, bedroom_type)
            if project_rental['transaction_count'] == 0:
                logger.warning(f"No rental data found for property {property_id}")
                return None
            
            # Get area rental data
            area_rental = self.get_area_rental_data(property_info['location_id'], bedroom_type)
            if area_rental['transaction_count'] == 0:
                logger.warning(f"No area rental data found for location {property_info['location_id']}")
                return None
            
            # Get project sales data
            project_sales = self.get_project_sales_data(property_id, bedroom_type)
            if project_sales['transaction_count'] == 0:
                logger.warning(f"No sales data found for property {property_id}")
                return None
            
            # Get area sales data
            area_sales = self.get_area_sales_data(property_info['location_id'], bedroom_type)
            if area_sales['transaction_count'] == 0:
                logger.warning(f"No area sales data found for location {property_info['location_id']}")
                return None
            
            # Calculate annual rental income (monthly rent * 12)
            annual_rental_income = project_rental['avg_monthly_rent'] * 12
            area_annual_rental_income = area_rental['avg_monthly_rent'] * 12
            
            # Calculate rental yields
            actual_yield = self.calculate_rental_yield(annual_rental_income, project_sales['avg_sale_price'])
            area_average_yield = self.calculate_rental_yield(area_annual_rental_income, area_sales['avg_sale_price'])
            
            return RentalYieldData(
                property_id=property_id,
                property_name=property_info['property_name'],
                location_id=property_info['location_id'],
                location_name=property_info['location_name'],
                bedroom_type=bedroom_type,
                actual_yield=actual_yield,
                area_average_yield=area_average_yield,
                annual_rental_income=annual_rental_income,
                property_value=project_sales['avg_sale_price'],
                transaction_count=project_rental['transaction_count'],
                area_transaction_count=area_rental['transaction_count']
            )
            
        except Exception as e:
            logger.error(f"Error analyzing rental yield for property {property_id}: {e}")
            return None
    
    def generate_rental_performance_snapshot(self, property_id: int, bedroom_type: Optional[int] = None) -> Optional[RentalPerformanceSnapshot]:
        """
        Generate a rental performance snapshot for a property
        
        Args:
            property_id: Property ID
            bedroom_type: Bedroom type filter (1, 2, 3) or None for all
            
        Returns:
            RentalPerformanceSnapshot object or None if insufficient data
        """
        yield_data = self.analyze_rental_yield(property_id, bedroom_type)
        if not yield_data:
            return None
        
        # Calculate performance metrics
        yield_difference = yield_data.actual_yield - yield_data.area_average_yield
        performance_ratio = yield_data.actual_yield / yield_data.area_average_yield if yield_data.area_average_yield > 0 else 0
        
        # Calculate data quality score (0-1) based on transaction counts
        total_transactions = yield_data.transaction_count + yield_data.area_transaction_count
        data_quality_score = min(1.0, total_transactions / 50)  # Normalize to 0-1 scale
        
        return RentalPerformanceSnapshot(
            property_id=property_id,
            property_name=yield_data.property_name,
            location_name=yield_data.location_name,
            bedroom_filter=bedroom_type,
            actual_rental_yield=yield_data.actual_yield,
            area_average_rental_yield=yield_data.area_average_yield,
            yield_difference=yield_difference,
            performance_ratio=performance_ratio,
            data_quality_score=data_quality_score
        )
    
    def get_available_bedroom_types(self, property_id: int) -> List[int]:
        """
        Get available bedroom types for a property
        
        Args:
            property_id: Property ID
            
        Returns:
            List of available bedroom types
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get bedroom types from rental data
                    cur.execute("""
                        SELECT DISTINCT attr_no_of_rooms 
                        FROM transaction_raw_rent 
                        WHERE prop_property_id = %s 
                        AND attr_no_of_rooms IS NOT NULL 
                        AND attr_no_of_rooms != ''
                        AND attr_no_of_rooms ~ '^[0-9]+$'
                        ORDER BY attr_no_of_rooms
                    """, [property_id])
                    
                    bedroom_types = [row[0] for row in cur.fetchall()]
                    return bedroom_types
                    
        except Exception as e:
            logger.error(f"Error getting bedroom types for property {property_id}: {e}")
            return []
    
    def get_properties_with_rental_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get properties that have BOTH rental and sales data available
        
        Args:
            limit: Maximum number of properties to return
            
        Returns:
            List of properties with both rental and sales data
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Find properties that have BOTH rental and sales data
                    cur.execute("""
                        SELECT 
                            r.prop_property_id,
                            r.rental_count,
                            s.sales_count,
                            p.name as property_name,
                            p.location_id,
                            l.location_name
                        FROM (
                            SELECT prop_property_id, COUNT(*) as rental_count
                            FROM transaction_raw_rent 
                            WHERE prop_property_id IS NOT NULL 
                            AND price IS NOT NULL 
                            AND price > 0
                            GROUP BY prop_property_id
                            HAVING COUNT(*) > 10
                        ) r
                        INNER JOIN (
                            SELECT property_id, COUNT(*) as sales_count
                            FROM transaction_sales 
                            WHERE property_id IS NOT NULL 
                            AND price IS NOT NULL 
                            AND price > 0
                            GROUP BY property_id
                            HAVING COUNT(*) > 5
                        ) s ON r.prop_property_id = s.property_id
                        LEFT JOIN property p ON r.prop_property_id = p.id
                        LEFT JOIN location l ON p.location_id = l.location_id
                        ORDER BY r.rental_count DESC
                        LIMIT %s
                    """, [limit])
                    
                    properties = []
                    for row in cur.fetchall():
                        properties.append({
                            'property_id': row[0],
                            'property_name': row[3] or f"Property {row[0]}",
                            'location_id': row[4],
                            'location_name': row[5],
                            'rental_count': row[1],
                            'sales_count': row[2]
                        })
                    
                    return properties
                    
        except Exception as e:
            logger.error(f"Error getting properties with rental data: {e}")
            return []


def main():
    """Main function for testing the rental yield analyzer"""
    analyzer = RentalYieldAnalyzer()
    
    # Get properties with rental data
    properties = analyzer.get_properties_with_rental_data(limit=10)
    
    if not properties:
        logger.info("No properties with rental data found")
        return
    
    logger.info(f"Found {len(properties)} properties with rental data")
    
    # Analyze first property
    property_id = properties[0]['property_id']
    property_name = properties[0]['property_name']
    
    logger.info(f"Analyzing property: {property_name} (ID: {property_id})")
    
    # Get available bedroom types
    bedroom_types = analyzer.get_available_bedroom_types(property_id)
    logger.info(f"Available bedroom types: {bedroom_types}")
    
    # Analyze overall project (no bedroom filter)
    logger.info("\n=== Overall Project Analysis ===")
    snapshot = analyzer.generate_rental_performance_snapshot(property_id)
    if snapshot:
        logger.info(f"Actual Rental Yield: {snapshot.actual_rental_yield}%")
        logger.info(f"Area Average Yield: {snapshot.area_average_rental_yield}%")
        logger.info(f"Yield Difference: {snapshot.yield_difference}%")
        logger.info(f"Performance Ratio: {snapshot.performance_ratio:.2f}")
        logger.info(f"Data Quality Score: {snapshot.data_quality_score:.2f}")
    
    # Analyze by bedroom type
    for bedroom_type in bedroom_types[:3]:  # Analyze first 3 bedroom types
        logger.info(f"\n=== {bedroom_type}BR Analysis ===")
        snapshot = analyzer.generate_rental_performance_snapshot(property_id, bedroom_type)
        if snapshot:
            logger.info(f"Actual Rental Yield: {snapshot.actual_rental_yield}%")
            logger.info(f"Area Average Yield: {snapshot.area_average_rental_yield}%")
            logger.info(f"Yield Difference: {snapshot.yield_difference}%")
            logger.info(f"Performance Ratio: {snapshot.performance_ratio:.2f}")


if __name__ == "__main__":
    main()