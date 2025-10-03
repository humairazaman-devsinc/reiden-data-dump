#!/usr/bin/env python3
"""
Rental Yield API

REST API endpoints for rental yield comparison functionality.
Provides endpoints to compare project rental yields with area averages,
filterable by bedroom type (1BR, 2BR, 3BR) or overall project.

Endpoints:
- GET /api/rental-yield/{property_id} - Get rental yield analysis for a property
- GET /api/rental-yield/{property_id}/bedroom/{bedroom_type} - Get analysis filtered by bedroom type
- GET /api/rental-yield/{property_id}/snapshot - Get rental performance snapshot
- GET /api/properties/with-rental-data - Get properties with available rental data
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from flask import Flask, jsonify, request
from flask_cors import CORS

# Ensure the parent directory is on sys.path when running this file directly
PARENT_DIR = Path(__file__).resolve().parents[1]
if str(PARENT_DIR) not in sys.path:
    sys.path.append(str(PARENT_DIR))

from src.rental_yield_analyzer import RentalYieldAnalyzer, RentalYieldData, RentalPerformanceSnapshot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('rental_yield_api.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for API access

# Initialize analyzer
analyzer = RentalYieldAnalyzer()


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'rental-yield-api',
        'version': '1.0.0'
    })


@app.route('/api/rental-yield/<int:property_id>', methods=['GET'])
def get_rental_yield_analysis(property_id: int):
    """
    Get rental yield analysis for a property (overall project)
    
    Args:
        property_id: Property ID
        
    Returns:
        JSON response with rental yield analysis
    """
    try:
        logger.info(f"Getting rental yield analysis for property {property_id}")
        
        # Get rental yield data
        yield_data = analyzer.analyze_rental_yield(property_id)
        
        if not yield_data:
            return jsonify({
                'error': 'Insufficient data for rental yield analysis',
                'property_id': property_id
            }), 404
        
        # Get available bedroom types
        bedroom_types = analyzer.get_available_bedroom_types(property_id)
        
        response = {
            'property_id': yield_data.property_id,
            'property_name': yield_data.property_name,
            'location_id': yield_data.location_id,
            'location_name': yield_data.location_name,
            'bedroom_type': yield_data.bedroom_type,
            'actual_rental_yield': yield_data.actual_yield,
            'area_average_rental_yield': yield_data.area_average_yield,
            'annual_rental_income': yield_data.annual_rental_income,
            'property_value': yield_data.property_value,
            'transaction_count': yield_data.transaction_count,
            'area_transaction_count': yield_data.area_transaction_count,
            'available_bedroom_types': bedroom_types,
            'yield_difference': yield_data.actual_yield - yield_data.area_average_yield,
            'performance_ratio': yield_data.actual_yield / yield_data.area_average_yield if yield_data.area_average_yield > 0 else 0
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting rental yield analysis for property {property_id}: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/rental-yield/<int:property_id>/bedroom/<int:bedroom_type>', methods=['GET'])
def get_rental_yield_analysis_by_bedroom(property_id: int, bedroom_type: int):
    """
    Get rental yield analysis for a property filtered by bedroom type
    
    Args:
        property_id: Property ID
        bedroom_type: Bedroom type (1, 2, 3, etc.)
        
    Returns:
        JSON response with rental yield analysis filtered by bedroom type
    """
    try:
        logger.info(f"Getting rental yield analysis for property {property_id}, bedroom type {bedroom_type}")
        
        # Get rental yield data filtered by bedroom type
        yield_data = analyzer.analyze_rental_yield(property_id, bedroom_type)
        
        if not yield_data:
            return jsonify({
                'error': 'Insufficient data for rental yield analysis',
                'property_id': property_id,
                'bedroom_type': bedroom_type
            }), 404
        
        response = {
            'property_id': yield_data.property_id,
            'property_name': yield_data.property_name,
            'location_id': yield_data.location_id,
            'location_name': yield_data.location_name,
            'bedroom_type': yield_data.bedroom_type,
            'actual_rental_yield': yield_data.actual_yield,
            'area_average_rental_yield': yield_data.area_average_yield,
            'annual_rental_income': yield_data.annual_rental_income,
            'property_value': yield_data.property_value,
            'transaction_count': yield_data.transaction_count,
            'area_transaction_count': yield_data.area_transaction_count,
            'yield_difference': yield_data.actual_yield - yield_data.area_average_yield,
            'performance_ratio': yield_data.actual_yield / yield_data.area_average_yield if yield_data.area_average_yield > 0 else 0
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting rental yield analysis for property {property_id}, bedroom {bedroom_type}: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/rental-yield/<int:property_id>/snapshot', methods=['GET'])
def get_rental_performance_snapshot(property_id: int):
    """
    Get rental performance snapshot for a property
    
    Args:
        property_id: Property ID
        
    Returns:
        JSON response with rental performance snapshot
    """
    try:
        logger.info(f"Getting rental performance snapshot for property {property_id}")
        
        # Get bedroom type filter from query parameters
        bedroom_type = request.args.get('bedroom_type', type=int)
        
        # Get rental performance snapshot
        snapshot = analyzer.generate_rental_performance_snapshot(property_id, bedroom_type)
        
        if not snapshot:
            return jsonify({
                'error': 'Insufficient data for rental performance snapshot',
                'property_id': property_id
            }), 404
        
        response = {
            'property_id': snapshot.property_id,
            'property_name': snapshot.property_name,
            'location_name': snapshot.location_name,
            'bedroom_filter': snapshot.bedroom_filter,
            'actual_rental_yield': snapshot.actual_rental_yield,
            'area_average_rental_yield': snapshot.area_average_rental_yield,
            'yield_difference': snapshot.yield_difference,
            'performance_ratio': snapshot.performance_ratio,
            'data_quality_score': snapshot.data_quality_score,
            'performance_status': 'outperforming' if snapshot.yield_difference > 0 else 'underperforming'
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting rental performance snapshot for property {property_id}: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/properties/with-rental-data', methods=['GET'])
def get_properties_with_rental_data():
    """
    Get properties that have rental data available
    
    Returns:
        JSON response with list of properties
    """
    try:
        # Get limit from query parameters
        limit = request.args.get('limit', 100, type=int)
        
        logger.info(f"Getting properties with rental data (limit: {limit})")
        
        # Get properties with rental data
        properties = analyzer.get_properties_with_rental_data(limit)
        
        response = {
            'properties': properties,
            'count': len(properties),
            'limit': limit
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting properties with rental data: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/rental-yield/<int:property_id>/bedroom-types', methods=['GET'])
def get_available_bedroom_types(property_id: int):
    """
    Get available bedroom types for a property
    
    Args:
        property_id: Property ID
        
    Returns:
        JSON response with available bedroom types
    """
    try:
        logger.info(f"Getting available bedroom types for property {property_id}")
        
        # Get available bedroom types
        bedroom_types = analyzer.get_available_bedroom_types(property_id)
        
        response = {
            'property_id': property_id,
            'available_bedroom_types': bedroom_types,
            'count': len(bedroom_types)
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting bedroom types for property {property_id}: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/rental-yield/<int:property_id>/comparison', methods=['GET'])
def get_rental_yield_comparison(property_id: int):
    """
    Get comprehensive rental yield comparison for a property
    
    Args:
        property_id: Property ID
        
    Returns:
        JSON response with comprehensive comparison data
    """
    try:
        logger.info(f"Getting comprehensive rental yield comparison for property {property_id}")
        
        # Get overall project analysis
        overall_snapshot = analyzer.generate_rental_performance_snapshot(property_id)
        
        # Get available bedroom types
        bedroom_types = analyzer.get_available_bedroom_types(property_id)
        
        # Get bedroom-specific analyses
        bedroom_analyses = {}
        for bedroom_type in bedroom_types:
            snapshot = analyzer.generate_rental_performance_snapshot(property_id, bedroom_type)
            if snapshot:
                bedroom_analyses[f"{bedroom_type}BR"] = {
                    'actual_rental_yield': snapshot.actual_rental_yield,
                    'area_average_rental_yield': snapshot.area_average_rental_yield,
                    'yield_difference': snapshot.yield_difference,
                    'performance_ratio': snapshot.performance_ratio,
                    'data_quality_score': snapshot.data_quality_score
                }
        
        response = {
            'property_id': property_id,
            'property_name': overall_snapshot.property_name if overall_snapshot else None,
            'location_name': overall_snapshot.location_name if overall_snapshot else None,
            'overall_project': {
                'actual_rental_yield': overall_snapshot.actual_rental_yield if overall_snapshot else 0,
                'area_average_rental_yield': overall_snapshot.area_average_rental_yield if overall_snapshot else 0,
                'yield_difference': overall_snapshot.yield_difference if overall_snapshot else 0,
                'performance_ratio': overall_snapshot.performance_ratio if overall_snapshot else 0,
                'data_quality_score': overall_snapshot.data_quality_score if overall_snapshot else 0
            } if overall_snapshot else None,
            'bedroom_analyses': bedroom_analyses,
            'available_bedroom_types': bedroom_types
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting rental yield comparison for property {property_id}: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'error': 'Not found',
        'message': 'The requested resource was not found'
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500


if __name__ == '__main__':
    logger.info("Starting Rental Yield API server...")
    app.run(host='0.0.0.0', port=5000, debug=True)