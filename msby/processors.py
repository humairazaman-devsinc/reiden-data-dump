import logging
from typing import List, Dict, Any
import json

logger = logging.getLogger(__name__)

class DataProcessor:
    @staticmethod
    def process_location_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed = []
        for item in raw_data:
            try:
                processed.append({
                    "location_id": item.get("location_id"),
                    "city_id": item.get("city_id"),
                    "city_name": item.get("city_name"),
                    "country_code": item.get("country_code"),
                    "county_id": item.get("county_id"),
                    "county_name": item.get("county_name"),
                    "description": item.get("description"),
                    "district_id": item.get("district_id"),
                    "district_name": item.get("district_name"),
                    "location_name": item.get("location_name"),
                    "geo_point": json.dumps(item.get("geo_point")) if item.get("geo_point") else None,
                    "photo_path": item.get("photo_path"),
                    "raw_data": json.dumps(item) if item else None,  # Convert to JSON string
                })
            except Exception as e:
                logger.warning(f"Failed to process location record: {e}")
                logger.debug(f"Problematic item: {item}")
        
        return processed

    @staticmethod
    def process_company_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process company data from company/{company_id} endpoint"""
        processed = []

        for item in raw_data:
            try:
                processed.append({
                    "id": item.get("id"),
                    "name": item.get("name"),
                })
            except Exception as e:
                logger.warning(f"Failed to process company record: {e}")
        
        return processed

    @staticmethod
    def process_property_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process property data from property/location/{location_id} endpoint"""
        processed = []
        
        for item in raw_data:
            try:
                # Extract data from _source if it exists, otherwise use item directly
                source_data = item.get("_source", item)
                
                # Log the structure for debugging
                logger.debug(f"Processing item with keys: {list(source_data.keys())}")
                
                # Extract location_id from the location object if available
                location_id = None
                if source_data.get("location") and isinstance(source_data["location"], dict):
                    location_id = source_data["location"].get("id")
                
                # Validate required fields
                if not source_data.get("id"):
                    logger.warning(f"Item missing required field 'id': {source_data}")
                    continue
                
                processed.append({
                    "id": source_data.get("id"),
                    "name": source_data.get("name"),
                    "name_local": source_data.get("name_local"),
                    "description": source_data.get("description"),
                    "description_local": source_data.get("description_local"),
                    "internal_status_id": source_data.get("internal_status_id"),
                    "loc_point": json.dumps(source_data.get("loc_point")) if source_data.get("loc_point") else None,
                    "geo_point": json.dumps(source_data.get("geo_point")) if source_data.get("geo_point") else None,
                    "loc_polygon": json.dumps(source_data.get("loc_polygon")) if source_data.get("loc_polygon") else None,
                    "level": json.dumps(source_data.get("level")) if source_data.get("level") else None,
                    "status": json.dumps(source_data.get("status")) if source_data.get("status") else None,
                    "types": json.dumps(source_data.get("types")) if source_data.get("types") else None,
                    "main_type_name": source_data.get("main_type_name"),
                    "main_subtype_name": source_data.get("main_subtype_name"),
                    "parent_id": source_data.get("parent_id"),
                    "parent_ids": source_data.get("parent_ids"),
                    "parents": json.dumps(source_data.get("parents")) if source_data.get("parents") else None,
                    "location_id": location_id,
                    "location": json.dumps(source_data.get("location")) if source_data.get("location") else None,
                    "locations": json.dumps(source_data.get("locations")) if source_data.get("locations") else None,
                    "attributes": json.dumps(source_data.get("attributes")) if source_data.get("attributes") else None,
                    "units": json.dumps(source_data.get("units")) if source_data.get("units") else None,
                    "developer_prices": json.dumps(source_data.get("developer_prices")) if source_data.get("developer_prices") else None,
                    "images": json.dumps(source_data.get("images")) if source_data.get("images") else None,
                    "primary_image": source_data.get("primary_image"),
                    "parties": json.dumps(source_data.get("parties")) if source_data.get("parties") else None,
                    "search_terms": source_data.get("search_terms"),
                    "created_on": source_data.get("created_on"),
                    "updated_on": source_data.get("updated_on"),
                    "import_date": source_data.get("import_date"),
                    "import_type": source_data.get("import_type"),
                    "dld_status": source_data.get("dld_status"),
                    "elapsed_time_status": source_data.get("elapsed_time_status"),
                    "raw_data": json.dumps(item),
                })
            except Exception as e:
                logger.warning(f"Failed to process property record: {e}")
                logger.debug(f"Problematic item: {item}")
        
        return processed

    @staticmethod
    def process_property_details_data(raw_data: Dict[str, Any] | List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process property details data from property/{property_id} endpoint"""
        processed: List[Dict[str, Any]] = []
        
        # Handle case where raw_data might be an array (should always have one item)
        if isinstance(raw_data, list):
            if len(raw_data) > 0:
                raw_data = raw_data[0]  # Extract the first (and only) item
            else:
                logger.warning("Empty list received for property details")
                return processed

        location = raw_data.get("location")

        # Property details endpoint returns single property object
        if isinstance(raw_data, dict):
            try:
                processed.append({
                    "id": raw_data.get("id"),
                    "property_id": raw_data.get("id"),
                    "name": raw_data.get("name"),
                    "name_local": raw_data.get("name_local"),
                    "description": raw_data.get("description"),
                    "description_local": raw_data.get("description_local"),
                    "developer_prices": json.dumps(raw_data.get("developer_prices")) if raw_data.get("developer_prices") else None,
                    "parties": json.dumps(raw_data.get("parties")) if raw_data.get("parties") else None,
                    "images": json.dumps(raw_data.get("images")) if raw_data.get("images") else None,
                    "units": json.dumps(raw_data.get("units")) if raw_data.get("units") else None,
                    "attributes": json.dumps(raw_data.get("attributes")) if raw_data.get("attributes") else None,
                    "level": json.dumps(raw_data.get("level")) if raw_data.get("level") else None,
                    "geo_point": json.dumps(raw_data.get("geo_point")) if raw_data.get("geo_point") else None,
                    "loc_point": json.dumps(raw_data.get("loc_point")) if raw_data.get("loc_point") else None,
                    "location_id": location.get("id") if location else None,
                    "location": json.dumps(location) if location else None,
                    "locations": json.dumps(raw_data.get("locations")) if raw_data.get("locations") else None,
                    "parent_id": raw_data.get("parent_id"),
                    "parent_ids": raw_data.get("parent_ids"),
                    "parents": json.dumps(raw_data.get("parents")) if raw_data.get("parents") else None,
                    "primary_image": raw_data.get("primary_image"),
                    "search_terms": raw_data.get("search_terms"),
                    "status": json.dumps(raw_data.get("status")) if raw_data.get("status") else None,
                    "types": json.dumps(raw_data.get("types")) if raw_data.get("types") else None,
                    "elapsed_time_status": raw_data.get("elapsed_time_status"),
                    "dld_status": raw_data.get("dld_status"),
                    "updated_on": raw_data.get("updated_on"),
                    "gla": raw_data.get("gla"),
                    "office_gla": raw_data.get("office_gla"),
                    "typical_gla_floor": raw_data.get("typical_gla_floor"),
                    "built_up_area": raw_data.get("built_up_area"),
                    "building_height": raw_data.get("building_height"),
                    "land_area": raw_data.get("land_area"),
                    "raw_data": json.dumps(raw_data),
                })
            except Exception as e:
                logger.warning(f"Failed to process property details record: {e}")
        
        return processed

    @staticmethod
    def process_indicator_aliased_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process property-level indicator data from indicators/aliased endpoint"""
        processed = []
        logger.info("Processing indicator aliased data: %s", raw_data)

        for item in raw_data:
            location = item.get("location")
            property = item.get("property")
            try:
                processed.append({
                    "series_id": item.get("id"),
                    "series_name": item.get("name"),
                    "series_name_local": item.get("name_local"),
                    "location_id": location.get("id") if location else None,
                    "location": json.dumps(location) if location else None,
                    "currency": json.dumps(item.get("currency")) if item.get("currency") else None,
                    "data_frequency": json.dumps(item.get("data_frequency")) if item.get("data_frequency") else None,
                    "update_frequency": json.dumps(item.get("update_frequency")) if item.get("update_frequency") else None,
                    "unit": json.dumps(item.get("unit")) if item.get("unit") else None,
                    "property_id": property.get("id") if property else None,
                    "property": json.dumps(property) if property else None,
                    "indicator": json.dumps(item.get("indicator")) if item.get("indicator") else None,
                    "indicator_groups": json.dumps(item.get("indicator_groups")) if item.get("indicator_groups") else None,
                    "last_value": json.dumps(item.get("last_value")) if item.get("last_value") else None,
                    "timepoints": json.dumps(item.get("timepoints")) if item.get("timepoints") else None,
                    "property_is_null": item.get("property_is_null"),
                    "raw_data": json.dumps(item),
                })
            except Exception as e:
                logger.warning(f"Failed to process indicator aliased record: {e}")
                logger.debug(f"Problematic item: {item}")
        
        return processed

    @staticmethod
    def process_indicator_area_aliased_data(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process area-level indicator data from indicators/area-aliased endpoint"""
        processed = []
        results = raw_data.get("results", [])
        
        location = raw_data.get("location")
        property = raw_data.get("property")
        
        for item in results:
            try:
                # Extract filters data
                filters = item.get("filters", {})
                data = item.get("data", {})
                
                processed.append({
                    "series_id": data.get("id"),
                    "series_name": data.get("name"),
                    "series_name_local": data.get("name_local"),

                    "alias": filters.get("alias"),
                    "property_type": filters.get("property_type"),
                    "price_type": filters.get("price_type"),
                    "no_of_bedrooms": filters.get("no_of_bedrooms"),

                    "indicator": json.dumps(data.get("indicator")) if data.get("indicator") else None,
                    "data_frequency": json.dumps(data.get("data_frequency")) if data.get("data_frequency") else None,
                    "last_value": json.dumps(data.get("last_value")) if data.get("last_value") else None,
                    "indicator_property_type": data.get("indicator_property_type"),
                    "indicator_property_subtype": data.get("indicator_property_subtype"),
                    "property_is_null": data.get("property_is_null"),
                    "property_id": property.get("id") if property else None,
                    "property": json.dumps(property) if property else None,
                    "unit": json.dumps(data.get("unit")) if data.get("unit") else None,
                    "location_id": location.get("id") if location else None,
                    "location": json.dumps(location) if location else None,
                    "indicator_groups": json.dumps(data.get("indicator_groups")) if data.get("indicator_groups") else None,
                    "currency": json.dumps(data.get("currency")) if data.get("currency") else None,
                    "timepoints": json.dumps(data.get("timepoints")) if data.get("timepoints") else None,
                    "raw_data": json.dumps(item),
                })
            except Exception as e:
                logger.warning(f"Failed to process indicator area aliased record: {e}")
                logger.debug(f"Problematic item: {item}")
        
        return processed

    @staticmethod
    def validate_data_quality(data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Validate data quality and return statistics"""
        total = len(data)
        missing = sum(1 for d in data if not d)
        return {"total": total, "missing": missing}

    @staticmethod
    def extract_pagination_info(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract pagination information from API response"""
        return {
            "page_size": raw_data.get("page_size"),
            "total_count": raw_data.get("total_count"),
            "current_page": raw_data.get("current_page"),
            "total_pages": raw_data.get("total_pages"),
        }
