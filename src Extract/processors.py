import logging
from typing import List, Dict, Any, Optional
import json

logger = logging.getLogger(__name__)


class DataProcessor:
    @staticmethod
    def jsonify_data(data: Any | None) -> str | None:
        """JSONify data"""
        return json.dumps(data) if data else None

    @staticmethod
    def process_location_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed = []
        for item in raw_data:
            try:
                processed.append(
                    {
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
                        "geo_point": DataProcessor.jsonify_data(item.get("geo_point")),
                        "photo_path": item.get("photo_path"),
                        "raw_data": json.dumps(item),  # Convert to JSON string
                    }
                )
            except Exception as e:
                logger.warning("Failed to process location record: %s", e)
                logger.debug("Problematic item: %s", item)

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
                logger.debug("Processing item with keys: %s", list(source_data.keys()))

                # Extract location_id from the location object if available
                location_id = None
                if source_data.get("location") and isinstance(
                    source_data["location"], dict
                ):
                    location_id = source_data["location"].get("id")

                # Validate required fields
                if not source_data.get("id"):
                    logger.warning("Item missing required field 'id': %s", source_data)
                    continue

                processed.append(
                    {
                        "id": source_data.get("id"),
                        "name": source_data.get("name"),
                        "name_local": source_data.get("name_local"),
                        "description": source_data.get("description"),
                        "description_local": source_data.get("description_local"),
                        "internal_status_id": source_data.get("internal_status_id"),
                        "loc_point": DataProcessor.jsonify_data(
                            source_data.get("loc_point")
                        ),
                        "geo_point": DataProcessor.jsonify_data(
                            source_data.get("geo_point")
                        ),
                        "loc_polygon": DataProcessor.jsonify_data(
                            source_data.get("loc_polygon")
                        ),
                        "level": DataProcessor.jsonify_data(source_data.get("level")),
                        "status": DataProcessor.jsonify_data(source_data.get("status")),
                        "types": DataProcessor.jsonify_data(source_data.get("types")),
                        "main_type_name": source_data.get("main_type_name"),
                        "main_subtype_name": source_data.get("main_subtype_name"),
                        "parent_id": source_data.get("parent_id"),
                        "parent_ids": source_data.get("parent_ids"),
                        "parents": DataProcessor.jsonify_data(
                            source_data.get("parents")
                        ),
                        "location_id": location_id,
                        "location": DataProcessor.jsonify_data(
                            source_data.get("location")
                        ),
                        "locations": DataProcessor.jsonify_data(
                            source_data.get("locations")
                        ),
                        "attributes": DataProcessor.jsonify_data(
                            source_data.get("attributes")
                        ),
                        "units": DataProcessor.jsonify_data(source_data.get("units")),
                        "developer_prices": DataProcessor.jsonify_data(
                            source_data.get("developer_prices")
                        ),
                        "images": DataProcessor.jsonify_data(source_data.get("images")),
                        "primary_image": source_data.get("primary_image"),
                        "parties": DataProcessor.jsonify_data(
                            source_data.get("parties")
                        ),
                        "search_terms": source_data.get("search_terms"),
                        "created_on": source_data.get("created_on"),
                        "updated_on": source_data.get("updated_on"),
                        "import_date": source_data.get("import_date"),
                        "import_type": source_data.get("import_type"),
                        "dld_status": source_data.get("dld_status"),
                        "elapsed_time_status": source_data.get("elapsed_time_status"),
                        "raw_data": json.dumps(item),
                    }
                )
            except Exception as e:
                logger.warning("Failed to process property record: %s", e)
                logger.debug("Problematic item: %s", item)

        return processed

    @staticmethod
    def process_property_details_data(
        raw_data: Dict[str, Any] | List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
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
                processed.append(
                    {
                        "id": raw_data.get("id"),
                        "property_id": raw_data.get("id"),
                        "name": raw_data.get("name"),
                        "name_local": raw_data.get("name_local"),
                        "description": raw_data.get("description"),
                        "description_local": raw_data.get("description_local"),
                        "developer_prices": DataProcessor.jsonify_data(
                            raw_data.get("developer_prices")
                        ),
                        "parties": DataProcessor.jsonify_data(raw_data.get("parties")),
                        "images": DataProcessor.jsonify_data(raw_data.get("images")),
                        "units": DataProcessor.jsonify_data(raw_data.get("units")),
                        "attributes": DataProcessor.jsonify_data(
                            raw_data.get("attributes")
                        ),
                        "level": DataProcessor.jsonify_data(raw_data.get("level")),
                        "geo_point": DataProcessor.jsonify_data(
                            raw_data.get("geo_point")
                        ),
                        "loc_point": DataProcessor.jsonify_data(
                            raw_data.get("loc_point")
                        ),
                        "location_id": location.get("id") if location else None,
                        "location": DataProcessor.jsonify_data(location),
                        "locations": DataProcessor.jsonify_data(
                            raw_data.get("locations")
                        ),
                        "parent_id": raw_data.get("parent_id"),
                        "parent_ids": raw_data.get("parent_ids"),
                        "parents": DataProcessor.jsonify_data(raw_data.get("parents")),
                        "primary_image": raw_data.get("primary_image"),
                        "search_terms": raw_data.get("search_terms"),
                        "status": DataProcessor.jsonify_data(raw_data.get("status")),
                        "types": DataProcessor.jsonify_data(raw_data.get("types")),
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
                    }
                )
            except Exception as e:
                logger.warning("Failed to process property details record: %s", e)
                logger.debug("Problematic item: %s", raw_data)
        return processed

    @staticmethod
    def process_indicator_aliased_data(
        raw_data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Process property-level indicator data from indicators/aliased endpoint"""
        processed = []
        logger.info("Processing indicator aliased data: %s", raw_data)

        for item in raw_data:
            location = item.get("location")
            property = item.get("property")
            try:
                processed.append(
                    {
                        "series_id": item.get("id"),
                        "series_name": item.get("name"),
                        "series_name_local": item.get("name_local"),
                        "location_id": location.get("id") if location else None,
                        "location": DataProcessor.jsonify_data(location),
                        "currency": DataProcessor.jsonify_data(item.get("currency")),
                        "data_frequency": DataProcessor.jsonify_data(
                            item.get("data_frequency")
                        ),
                        "update_frequency": DataProcessor.jsonify_data(
                            item.get("update_frequency")
                        ),
                        "unit": DataProcessor.jsonify_data(item.get("unit")),
                        "property_id": property.get("id") if property else None,
                        "property": DataProcessor.jsonify_data(property),
                        "indicator": DataProcessor.jsonify_data(item.get("indicator")),
                        "indicator_groups": DataProcessor.jsonify_data(
                            item.get("indicator_groups")
                        ),
                        "last_value": DataProcessor.jsonify_data(
                            item.get("last_value")
                        ),
                        "timepoints": DataProcessor.jsonify_data(
                            item.get("timepoints")
                        ),
                        "property_is_null": item.get("property_is_null"),
                        "raw_data": json.dumps(item),
                    }
                )
            except Exception as e:
                logger.warning("Failed to process indicator aliased record: %s", e)
                logger.debug("Problematic item: %s", item)

        return processed

    def process_indicator_location_id_response(
        self,
        response_data: Dict[str, Any],
        location_id: int,
        currency: str,
        measurement: str,
        page_number: int,
    ) -> List[Dict[str, Any]]:
        """Process indicator location ID data from indicators/location_id endpoint"""
        processed = []
        
        if not response_data or 'data' not in response_data:
            logger.warning("No data found in indicator location ID response")
            return processed
            
        data_items = response_data['data']
        logger.info("Processing %d indicator location ID records for location %d", len(data_items), location_id)
        
        for item in data_items:
            try:
                # Extract last_value data
                last_value = item.get("last_value", {})
                indicator_value = last_value.get("value") if isinstance(last_value, dict) else None
                indicator_value_date = last_value.get("date") if isinstance(last_value, dict) else None
                indicator_value_difference = last_value.get("differance") if isinstance(last_value, dict) else None
                
                # Handle empty strings for integer fields
                property_id = item.get("property_id")
                if property_id == "" or property_id is None:
                    property_id = None
                else:
                    try:
                        property_id = int(property_id)
                    except (ValueError, TypeError):
                        property_id = None
                
                processed.append({
                    "indicator_id": item.get("id"),
                    "indicator_name": item.get("indicator_name"),
                    "indicator_value": indicator_value,
                    "indicator_value_date": indicator_value_date,
                    "indicator_value_difference": indicator_value_difference,
                    "level_id": item.get("level_id"),
                    "level_name": item.get("level_name"),
                    "location_id": item.get("location_id"),
                    "location_name": item.get("location_name"),
                    "property_id": property_id,
                    "property_name": item.get("property_name"),
                    "property_nature": item.get("property_nature"),
                    "property_subtype": item.get("property_subtype"),
                    "property_type": item.get("property_type"),
                    "parent_info": DataProcessor.jsonify_data(item.get("parent_info")),
                    "internal_status_id": item.get("internal_status_id"),
                    "timepoints": DataProcessor.jsonify_data(item.get("timepoints")),
                    "currency": currency,
                    "measurement": measurement,
                    "page_number": page_number,
                    "raw_data": json.dumps(item),
                })
            except Exception as e:
                logger.warning("Failed to process indicator location ID record: %s", e)
                logger.debug("Problematic item: %s", item)
                
        return processed

    @staticmethod
    def process_transactions_avg_data(
        raw_data: List[Dict[str, Any]], 
        location_id: int, 
        property_type: str,
        activity_type: str,
        currency: str,
        measurement: str
    ) -> List[Dict[str, Any]]:
        """Process transactions average data and normalize it into individual records per time period"""
        processed = []
        logger.info("Processing transactions average data for location %s: %s records", location_id, len(raw_data))

        for item in raw_data:
            try:
                # Extract location name if available
                location_name = item.get("location_name")
                
                # Handle the values array structure
                values = item.get("values", [])
                if not isinstance(values, list):
                    logger.warning("No values array found in item: %s", item)
                    continue
                
                for value_item in values:
                    try:
                        # Parse date period (e.g., "2021-10")
                        date_period = value_item.get("date")
                        if not date_period:
                            continue
                            
                        # Extract year and month
                        year, month = map(int, date_period.split("-"))
                        
                        # Extract metrics with proper null handling
                        average_net_price = value_item.get("average_net_price")
                        average_unit_price = value_item.get("average_unit_price")
                        total_count = value_item.get("total_count")
                        total_price = value_item.get("total_price")
                        no_of_bedrooms = value_item.get("no_of_bedrooms")
                        
                        # Convert empty strings to None for proper database handling
                        if no_of_bedrooms == "" or no_of_bedrooms is None:
                            no_of_bedrooms = None
                        else:
                            no_of_bedrooms = int(no_of_bedrooms)
                        
                        # Calculate derived metrics
                        price_per_sqm = average_unit_price if average_unit_price else None
                        transaction_volume = total_price if total_price else None
                        
                        # Create normalized record
                        processed_item = {
                            "location_id": location_id,
                            "location_name": location_name,
                            "property_type": property_type,
                            "activity_type": activity_type,
                            "currency": currency,
                            "measurement": measurement,
                            "no_of_bedrooms": no_of_bedrooms,
                            "date_period": date_period,
                            "year": year,
                            "month": month,
                            "average_net_price": average_net_price,
                            "average_unit_price": average_unit_price,
                            "total_count": total_count,
                            "total_price": total_price,
                            "price_per_sqm": price_per_sqm,
                            "transaction_volume": transaction_volume,
                            "raw_data": json.dumps(value_item)
                        }
                        
                        processed.append(processed_item)
                        
                    except Exception as e:
                        logger.warning("Failed to process value item: %s - %s", value_item, e)
                        continue
                        
            except Exception as e:
                logger.warning("Failed to process transactions average record: %s", e)
                logger.debug("Problematic item: %s", item)

        logger.info("Processed %s normalized records from %s raw records", len(processed), len(raw_data))
        return processed

    @staticmethod
    def validate_data_quality(data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Validate data quality and return statistics"""
        total = len(data)
        missing = sum(1 for d in data if not d)
        return {"total": total, "missing": missing}

    @staticmethod
    def process_transaction_history_data(
        raw_data: List[Dict[str, Any]], 
        municipal_area: str,
        land_number: str,
        transaction_type: Optional[str] = None,
        building_number: Optional[str] = None,
        building_name: Optional[str] = None,
        unit: Optional[str] = None,
        floor: Optional[str] = None,
        unit_min_size: Optional[float] = None,
        unit_max_size: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Process transaction history data and normalize it into individual records"""
        processed = []
        logger.info("Processing transaction history data for area %s, land %s: %s records", municipal_area, land_number, len(raw_data))

        for item in raw_data:
            try:
                # Parse transaction date
                date_transaction = item.get("date_transaction")
                if not date_transaction:
                    logger.warning("No transaction date found in item: %s", item)
                    continue
                
                # Extract location information
                location = item.get("location", {})
                property_info = item.get("property", {})
                property_type = item.get("property_type", {})
                
                # Extract financial information
                price_aed = item.get("price_aed")
                price_size_aed_imp = item.get("price_size_aed_imp")
                size_imp = item.get("size_imp")
                size_land_imp = item.get("size_land_imp")
                
                # Create normalized record
                processed_item = {
                    "date_transaction": date_transaction,
                    "land_number": land_number,
                    "municipal_area": municipal_area,
                    
                    # Location information
                    "loc_city_id": location.get("city_id"),
                    "loc_city_name": location.get("city_name"),
                    "loc_county_id": location.get("county_id"),
                    "loc_county_name": location.get("county_name"),
                    "loc_district_id": location.get("district_id"),
                    "loc_district_name": location.get("district_name"),
                    "loc_location_id": location.get("location_id"),
                    "loc_location_name": location.get("location_name"),
                    "loc_municipal_area": location.get("municipal_area"),
                    
                    # Property information
                    "property_id": property_info.get("id"),
                    "property_name": property_info.get("name"),
                    "property_type_municipal_property_type": property_type.get("municipal_property_type"),
                    "property_type_subtype_name": property_type.get("subtype_name"),
                    "property_type_type_name": property_type.get("type_name"),
                    
                    # Financial information
                    "price_aed": price_aed,
                    "price_size_aed_imp": price_size_aed_imp,
                    "size_imp": size_imp,
                    "size_land_imp": size_land_imp,
                    
                    # Additional filters used
                    "transaction_type": transaction_type,
                    "building_number": building_number,
                    "building_name": building_name,
                    "unit": unit,
                    "floor": floor,
                    "unit_min_size": unit_min_size,
                    "unit_max_size": unit_max_size,
                    
                    "raw_data": json.dumps(item)
                }
                
                processed.append(processed_item)
                
            except Exception as e:
                logger.warning("Failed to process transaction history record: %s", e)
                logger.debug("Problematic item: %s", item)

        logger.info("Processed %s normalized records from %s raw records", len(processed), len(raw_data))
        return processed

    @staticmethod
    def process_transaction_rent_data(
        raw_data: List[Dict[str, Any]], 
        location_id: int,
        currency: Optional[str] = None,
        measurement: Optional[str] = None,
        property_id: Optional[int] = None,
        property_type: Optional[str] = None,
        transaction_date: Optional[str] = None,
        bedroom: Optional[int] = None,
        size_range: Optional[str] = None,
        price_range: Optional[str] = None,
        rent_type: Optional[str] = None,
        property_subtype: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Process transaction rent data and normalize it into individual records"""
        processed = []
        logger.info("Processing transaction rent data for location %s: %s records", location_id, len(raw_data))

        for item in raw_data:
            try:
                # Extract transaction details
                start_date = item.get("start_date")
                end_date = item.get("end_date")
                date = item.get("date")
                transaction_version = item.get("transaction_version")
                transaction_type = item.get("transaction_type")
                
                # Extract location information
                location = item.get("location", {})
                property_info = item.get("property", {})
                property_type_info = item.get("property_type", {})
                attributes = item.get("attributes", {})
                
                # Extract financial information
                price = item.get("price")
                price_per_size = item.get("price_per_size")
                size = item.get("size")
                size_land_imp = item.get("size_land_imp")
                
                # Create normalized record
                processed_item = {
                    "start_date": start_date,
                    "end_date": end_date,
                    "date": date,
                    "transaction_version": transaction_version,
                    "transaction_type": transaction_type,
                    
                    # Location information
                    "location_id": location_id,
                    "loc_city_id": location.get("city_id"),
                    "loc_city_name": location.get("city_name"),
                    "loc_county_id": location.get("county_id"),
                    "loc_county_name": location.get("county_name"),
                    "loc_district_id": location.get("district_id"),
                    "loc_district_name": location.get("district_name"),
                    "loc_location_id": location.get("location_id"),
                    "loc_location_name": location.get("location_name"),
                    "loc_municipal_area": location.get("municipal_area"),
                    
                    # Property information
                    "property_id": property_info.get("property_id"),
                    "property_name": property_info.get("property_name"),
                    "property_type_name": property_type_info.get("type_name"),
                    "property_subtype_name": property_type_info.get("subtype_name"),
                    
                    # Financial information
                    "price": price,
                    "price_per_size": price_per_size,
                    "size": size,
                    "size_land_imp": size_land_imp,
                    
                    # Property attributes
                    "attr_unit": attributes.get("unit"),
                    "attr_floor": attributes.get("floor"),
                    "attr_parking": attributes.get("parking"),
                    "attr_land_number": attributes.get("land_number"),
                    "attr_no_of_rooms": int(attributes.get("no_of_rooms")) if attributes.get("no_of_rooms") and str(attributes.get("no_of_rooms")).isdigit() else None,
                    "attr_balcony_area": attributes.get("balcony_area"),
                    "attr_building_name": attributes.get("building_name"),
                    "attr_building_number": attributes.get("building_number"),
                    
                    # Additional filters used
                    "currency": currency,
                    "measurement": measurement,
                    "bedroom": bedroom,
                    "size_range": size_range,
                    "price_range": price_range,
                    "rent_type": rent_type,
                    "transaction_date": transaction_date,
                    
                    "raw_data": json.dumps(item)  # Store raw JSON for data lineage
                }
                
                processed.append(processed_item)
                
            except Exception as e:
                logger.warning("Failed to process transaction rent record: %s", e)
                logger.debug("Problematic item: %s", item)

        # Deduplicate records based on unique constraint fields
        unique_records = []
        seen_keys = set()
        
        for record in processed:
            # Create a key from the unique constraint fields
            key = (
                record.get('location_id'),
                record.get('start_date'),
                record.get('end_date'),
                record.get('price'),
                record.get('transaction_type'),
                record.get('date')
            )
            
            if key not in seen_keys:
                seen_keys.add(key)
                unique_records.append(record)
        
        logger.info("Processed %s normalized records from %s raw records (deduplicated from %s)", 
                   len(unique_records), len(raw_data), len(processed))
        return unique_records

    @staticmethod
    def process_transaction_sales_data(
        raw_data: List[Dict[str, Any]], 
        location_id: int, 
        currency: str = "aed", 
        measurement: str = "imp",
        transaction_type: str = "Sales - Ready",
        bedroom: Optional[int] = None,
        size_range: Optional[str] = None,
        price_range: Optional[str] = None,
        transaction_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Process transaction sales data from API response into normalized format
        
        Args:
            raw_data: List of transaction sales records from API
            location_id: Location ID for the transactions
            currency: Currency used (default: aed)
            measurement: Measurement unit (default: imp)
            transaction_type: Type of transaction (default: Sales - Ready)
            bedroom: Number of bedrooms filter
            size_range: Size range filter
            price_range: Price range filter
            transaction_date: Transaction date filter
            
        Returns:
            List of normalized transaction sales records
        """
        processed_records = []
        seen_records = set()  # For deduplication
        
        for record in raw_data:
            try:
                # Extract transaction details
                start_date = record.get("start_date")
                end_date = record.get("end_date")
                date = record.get("date")
                transaction_version = record.get("transaction_version")
                transaction_type_actual = record.get("transaction_type", transaction_type)
                price = record.get("price")
                price_per_size = record.get("price_per_size")
                size = record.get("size")
                size_land_imp = record.get("size_land_imp")
                
                # Extract location information
                location = record.get("location", {})
                if isinstance(location, dict):
                    loc_city_id = location.get("city_id")
                    loc_city_name = location.get("city_name")
                    loc_county_id = location.get("county_id")
                    loc_county_name = location.get("county_name")
                    loc_district_id = location.get("district_id")
                    loc_district_name = location.get("district_name")
                    loc_location_id = location.get("location_id")
                    loc_location_name = location.get("location_name")
                    loc_municipal_area = location.get("municipal_area")
                else:
                    loc_city_id = loc_city_name = loc_county_id = loc_county_name = None
                    loc_district_id = loc_district_name = loc_location_id = None
                    loc_location_name = loc_municipal_area = None
                
                # Extract property information
                property_info = record.get("property", {})
                if isinstance(property_info, dict):
                    property_id = property_info.get("property_id")
                    property_name = property_info.get("property_name")
                else:
                    property_id = property_name = None
                
                # Extract property type information
                property_type = record.get("property_type", {})
                if isinstance(property_type, dict):
                    property_type_name = property_type.get("type_name")
                    property_subtype_name = property_type.get("subtype_name")
                else:
                    property_type_name = property_subtype_name = None
                
                # Extract attributes
                attributes = record.get("attributes", {})
                if not isinstance(attributes, dict):
                    attributes = {}
                
                # Create unique key for deduplication
                unique_key = (
                    location_id,
                    start_date,
                    end_date,
                    price,
                    transaction_type_actual,
                    date
                )
                
                if unique_key in seen_records:
                    continue
                seen_records.add(unique_key)
                
                processed_record = {
                    # Transaction details
                    "start_date": start_date,
                    "end_date": end_date,
                    "date": date,
                    "transaction_version": transaction_version,
                    "transaction_type": transaction_type_actual,
                    
                    # Location information
                    "location_id": location_id,
                    "loc_city_id": loc_city_id,
                    "loc_city_name": loc_city_name,
                    "loc_county_id": loc_county_id,
                    "loc_county_name": loc_county_name,
                    "loc_district_id": loc_district_id,
                    "loc_district_name": loc_district_name,
                    "loc_location_id": loc_location_id,
                    "loc_location_name": loc_location_name,
                    "loc_municipal_area": loc_municipal_area,
                    
                    # Property information
                    "property_id": property_id,
                    "property_name": property_name,
                    "property_type_name": property_type_name,
                    "property_subtype_name": property_subtype_name,
                    
                    # Financial information
                    "price": price,
                    "price_per_size": price_per_size,
                    "size": size,
                    "size_land_imp": size_land_imp,
                    
                    # Property attributes
                    "attr_unit": attributes.get("unit"),
                    "attr_floor": attributes.get("floor"),
                    "attr_parking": attributes.get("parking"),
                    "attr_land_number": attributes.get("land_number"),
                    "attr_no_of_rooms": int(attributes.get("no_of_rooms")) if attributes.get("no_of_rooms") and str(attributes.get("no_of_rooms")).isdigit() else None,
                    "attr_balcony_area": attributes.get("balcony_area"),
                    "attr_building_name": attributes.get("building_name"),
                    "attr_building_number": attributes.get("building_number"),
                    
                    # Additional filters used
                    "currency": currency,
                    "measurement": measurement,
                    "bedroom": bedroom,
                    "size_range": size_range,
                    "price_range": price_range,
                    "transaction_date": transaction_date,
                    
                    "raw_data": json.dumps(record)  # Store raw JSON for data lineage
                }
                
                processed_records.append(processed_record)
                
            except Exception as e:
                logger.error(f"Failed to process transaction sales record: {e}")
                continue
        
        logger.info(f"Processed {len(processed_records)} transaction sales records from {len(raw_data)} raw records")
        return processed_records

    @staticmethod
    def process_cma_sales_data(
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
        lon: str | None = None
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
                unique_key = (int(property_id), alias, currency, measurement, property_type, property_subtype, int(comparable_property_id))
                
                if unique_key in seen_keys:
                    continue  # Skip duplicate
                
                seen_keys.add(unique_key)
                
                # Extract and convert numeric values with proper type handling
                size_value = item.get("size")
                price_value = item.get("price")
                price_per_size_value = item.get("price_per_size")
                
                # Convert size to decimal
                try:
                    if size_value is not None:
                        size_value = float(size_value)
                    else:
                        size_value = None
                except (ValueError, TypeError):
                    size_value = None
                
                # Convert price to decimal
                try:
                    if price_value is not None:
                        price_value = float(price_value)
                    else:
                        price_value = None
                except (ValueError, TypeError):
                    price_value = None
                
                # Convert price_per_size to decimal
                try:
                    if price_per_size_value is not None:
                        price_per_size_value = float(price_per_size_value)
                    else:
                        price_per_size_value = None
                except (ValueError, TypeError):
                    price_per_size_value = None
                
                # Parse transaction date
                transaction_date_str = item.get("transaction_date")
                transaction_date = None
                if transaction_date_str:
                    try:
                        from datetime import datetime
                        # Handle ISO format with Z suffix
                        if transaction_date_str.endswith('Z'):
                            transaction_date_str = transaction_date_str[:-1] + '+00:00'
                        transaction_date = datetime.fromisoformat(transaction_date_str)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse transaction_date '{transaction_date_str}': {e}")
                        transaction_date = None
                
                # Extract location information (flatten nested location object)
                location = item.get("location", {})
                if not isinstance(location, dict):
                    location = {}
                
                # Extract parent property information
                parent_property = item.get("parent_property", {})
                if not isinstance(parent_property, dict):
                    parent_property = {}
                
                # Extract no_of_bedrooms from the item (not from parameters)
                item_no_of_bedrooms = item.get("no_of_bedrooms")
                if item_no_of_bedrooms is not None:
                    try:
                        item_no_of_bedrooms = int(item_no_of_bedrooms)
                    except (ValueError, TypeError):
                        item_no_of_bedrooms = None
                
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
                    "raw_data": item
                }
                
                processed_records.append(processed_record)
                
            except Exception as e:
                logger.error(f"Failed to process CMA sales record: {e}")
                logger.debug(f"Problematic item: {item}")
                continue
        
        logger.info(f"Processed {len(processed_records)} CMA sales records from {len(raw_data)} raw records (deduplicated from {len(raw_data)})")
        return processed_records

    @staticmethod
    def process_cma2_rents_response(
        response_data: Dict[str, Any],
        property_id: int,
        alias: str,
        currency: str,
        measurement: str,
        property_type: str,
        property_subtype: str
    ) -> List[Dict[str, Any]]:
        """Process CMA2-rents API response data"""
        processed = []
        
        try:
            # Check if response indicates empty results
            if isinstance(response_data, dict):
                # Check for empty results indicators
                if (response_data.get('info', {}).get('avg_price') == 'Result is empty!' or
                    response_data.get('results') == [] or
                    not response_data.get('results')):
                    logger.info(f"Skipping property {property_id} - API returned empty results")
                    return processed  # Return empty list - don't insert NULL records
            
            # Handle the response structure - look for 'results' field containing transaction data
            if isinstance(response_data, dict) and 'results' in response_data:
                data_items = response_data['results']
            elif isinstance(response_data, dict) and 'data' in response_data:
                data_items = response_data['data']
            elif isinstance(response_data, list):
                data_items = response_data
            else:
                data_items = [response_data] if response_data else []
            
            # Skip if no data items
            if not data_items:
                logger.info(f"Skipping property {property_id} - No data items in response")
                return processed
            
            # Collect all valid transactions first
            valid_transactions = []
            for item in data_items:
                try:
                    price = item.get('price', 0)
                    if price is None or price == 0:
                        continue
                    valid_transactions.append(item)
                except Exception as e:
                    logger.warning(f"Failed to validate transaction: {e}")
                    continue
            
            # Skip if no valid transactions
            if not valid_transactions:
                logger.info(f"Skipping property {property_id} - No valid transactions")
                return processed
            
            # Calculate aggregated metrics from all transactions
            prices = [t.get('price', 0) for t in valid_transactions]
            prices_per_sqm = [t.get('price_per_size', 0) for t in valid_transactions if t.get('price_per_size', 0) > 0]
            
            transaction_count = len(valid_transactions)
            average_rent = sum(prices) / len(prices) if prices else 0
            median_rent = sorted(prices)[len(prices)//2] if prices else 0
            min_rent = min(prices) if prices else 0
            max_rent = max(prices) if prices else 0
            avg_rent_per_sqm = sum(prices_per_sqm) / len(prices_per_sqm) if prices_per_sqm else 0
            
            # Use location data from first transaction (they should all be in same area)
            first_transaction = valid_transactions[0]
            location = first_transaction.get('location', {})
            
            processed_record = {
                # API Parameters
                'property_id': property_id,
                'country_code': 'AE',
                'alias': alias,
                'currency': currency,
                'measurement': measurement,
                'property_type': property_type,
                'property_subtype': property_subtype,
                
                # Location data from first transaction
                'city_id': location.get('city_id'),
                'city_name': location.get('city_name'),
                'county_id': location.get('county_id'),
                'county_name': location.get('county_name'),
                'district_id': location.get('district_id'),
                'location_id': location.get('location_id'),
                'district_name': location.get('district_name'),
                'location_name': location.get('location_name'),
                'municipal_area': location.get('municipal_area'),
                
                # Aggregated rent metrics
                'transaction_count': transaction_count,
                'average_rent': average_rent,
                'median_rent': median_rent,
                'min_rent': min_rent,
                'max_rent': max_rent,
                'rent_per_sqm': avg_rent_per_sqm,
                
                # Raw data as proper JSON (store as single transaction object)
                'raw_data': {'transactions': valid_transactions[0] if valid_transactions else {}}  # Store first transaction as single object
            }
            
            processed.append(processed_record)
                    
        except Exception as e:
            logger.error(f"Failed to process CMA2-rents response: {e}")
            logger.debug(f"Response data: {response_data}")
        
        logger.info(f"Processed {len(processed)} CMA2-rents records from response")
        return processed

    @staticmethod
    def process_transactions_price_data(
        raw_data: List[Dict[str, Any]] | Dict[str, Any],
        location_id: str | int,
        property_type: str,
        activity_type: str,
        property_id: int | None = None,
        property_sub_type: str | None = None,
        no_of_bedrooms: int | None = None
    ) -> List[Dict[str, Any]]:
        """Process transactions price data from API response"""
        processed_records = []
        seen_keys = set()  # For deduplication
        
        # Handle both list and dict responses
        if isinstance(raw_data, dict):
            # If it's a dict, it might be keyed by terms (e.g., months, years)
            for term, item in raw_data.items():
                try:
                    # Create unique key for deduplication
                    unique_key = (int(location_id), property_type, activity_type, property_id, property_sub_type, no_of_bedrooms, term)
                    
                    if unique_key in seen_keys:
                        continue  # Skip duplicate
                    
                    seen_keys.add(unique_key)
                    
                    # Extract value and additional data
                    if isinstance(item, dict):
                        value = item.get("value") or item.get("price") or item.get("amount")
                        additional_data = {k: v for k, v in item.items() if k not in ["value", "price", "amount"]}
                    else:
                        value = item
                        additional_data = {}
                    
                    # Convert value to decimal if possible
                    try:
                        if value is not None:
                            if isinstance(value, (int, float)):
                                value = float(value)
                            elif isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                                value = float(value)
                            else:
                                value = None
                        else:
                            value = None
                    except (ValueError, TypeError):
                        value = None
                    
                    processed_record = {
                        "location_id": int(location_id),
                        "property_type": property_type,
                        "activity_type": activity_type,
                        "property_id": property_id,
                        "property_sub_type": property_sub_type,
                        "no_of_bedrooms": no_of_bedrooms,
                        "term": term,
                        "value": value,
                        "additional_data": json.dumps(additional_data) if additional_data else None,
                        "raw_data": json.dumps(item)
                    }
                    
                    processed_records.append(processed_record)
                    
                except Exception as e:
                    logger.error(f"Failed to process transactions price record: {e}")
                    continue
        
        elif isinstance(raw_data, list):
            # If it's a list, process each item (this is the actual structure from the API)
            for item in raw_data:
                try:
                    if not isinstance(item, dict):
                        continue
                    
                    # Extract term from the item (e.g., "2023-12")
                    term = item.get("term") or item.get("month") or item.get("year") or item.get("period")
                    if not term:
                        # If no term, use a default or skip
                        continue
                    
                    # Create unique key for deduplication
                    unique_key = (int(location_id), property_type, activity_type, property_id, property_sub_type, no_of_bedrooms, str(term))
                    
                    if unique_key in seen_keys:
                        continue  # Skip duplicate
                    
                    seen_keys.add(unique_key)
                    
                    # Extract price values - use monthly_average_price as the main value
                    value = item.get("monthly_average_price") or item.get("value") or item.get("price") or item.get("amount")
                    
                    # Store all price statistics in additional_data
                    additional_data = {
                        "max_price": item.get("max_price"),
                        "min_price": item.get("min_price"),
                        "max_price_per_size": item.get("max_price_per_size"),
                        "min_price_per_size": item.get("min_price_per_size"),
                        "monthly_price_per_size": item.get("monthly_price_per_size"),
                        "total_transaction_count": item.get("total_transaction_count"),
                        "no_of_rooms": item.get("no_of_rooms"),
                        "city_id": item.get("city_id"),
                        "city_name": item.get("city_name"),
                        "county_id": item.get("county_id"),
                        "county_name": item.get("county_name"),
                        "district_id": item.get("district_id"),
                        "district_name": item.get("district_name"),
                        "property_name": item.get("property_name")
                    }
                    
                    # Convert value to decimal if possible
                    try:
                        if value is not None:
                            if isinstance(value, (int, float)):
                                value = float(value)
                            elif isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                                value = float(value)
                            else:
                                value = None
                        else:
                            value = None
                    except (ValueError, TypeError):
                        value = None
                    
                    processed_record = {
                        "location_id": int(location_id),
                        "property_type": property_type,
                        "activity_type": activity_type,
                        "property_id": property_id,
                        "property_sub_type": property_sub_type,
                        "no_of_bedrooms": no_of_bedrooms,
                        "term": str(term),
                        "value": value,
                        "additional_data": json.dumps(additional_data) if additional_data else None,
                        "raw_data": json.dumps(item)
                    }
                    
                    processed_records.append(processed_record)
                    
                except Exception as e:
                    logger.error(f"Failed to process transactions price record: {e}")
                    continue
        
        logger.info(f"Processed {len(processed_records)} transactions price records from {len(raw_data) if isinstance(raw_data, list) else len(raw_data.keys())} raw records (deduplicated)")
        return processed_records

    @staticmethod
    def process_transaction_list_data(
        raw_data: List[Dict[str, Any]], 
        location_id: str | int,
        property_type: str,
        activity_type: str,
        currency: str,
        measurement: str
    ) -> List[Dict[str, Any]]:
        """Process transaction list data from API response"""
        processed_records = []
        seen_keys = set()  # For deduplication
        
        for item in raw_data:
            try:
                if not isinstance(item, dict):
                    continue
                
                # Extract transaction details - use property_id as transaction identifier
                transaction_id = item.get("id") or item.get("transaction_id") or item.get("property_id")
                if not transaction_id:
                    logger.warning(f"No transaction ID found in record: {item}")
                    continue
                
                # Create unique key for deduplication
                unique_key = (int(location_id), property_type, activity_type, currency, measurement, str(transaction_id))
                
                if unique_key in seen_keys:
                    continue  # Skip duplicate
                
                seen_keys.add(unique_key)
                
                # Extract transaction information - direct from API response
                date_transaction = item.get("date_transaction")
                price = item.get("price")
                price_per_size = item.get("price_per_size")
                size = item.get("size")
                size_land = item.get("size_land")
                
                # Extract location information - direct from API response
                loc_city_id = item.get("city_id")
                loc_city_name = item.get("city_name")
                loc_county_id = item.get("county_id")
                loc_county_name = item.get("county_name")
                loc_district_id = item.get("district_id")
                loc_district_name = item.get("district_name")
                loc_location_id = item.get("location_id")
                loc_location_name = item.get("district_name")  # Use district_name as location name
                loc_municipal_area = None  # Not available in this API response
                
                # Extract property information - direct from API response
                property_id = item.get("property_id")
                property_name = item.get("property_name")
                
                # Extract property type information - direct from API response
                property_type_name = item.get("property_type_name")
                property_subtype_name = item.get("property_sub_type")
                municipal_property_type = item.get("property_nature")
                
                # Extract attributes - direct from API response
                # The API response has attributes directly in the record
                
                # Convert price to decimal if possible
                try:
                    if price is not None:
                        price = float(price)
                    else:
                        price = None
                except (ValueError, TypeError):
                    price = None
                
                # Convert price_per_size to decimal if possible
                try:
                    if price_per_size is not None:
                        price_per_size = float(price_per_size)
                    else:
                        price_per_size = None
                except (ValueError, TypeError):
                    price_per_size = None
                
                # Convert size to decimal if possible
                try:
                    if size is not None:
                        size = float(size)
                    else:
                        size = None
                except (ValueError, TypeError):
                    size = None
                
                processed_record = {
                    # Transaction details
                    "transaction_id": str(transaction_id),
                    "date_transaction": date_transaction,
                    "price": price,
                    "price_per_size": price_per_size,
                    "size": size,
                    "size_land": size_land,
                    
                    # Location information
                    "location_id": int(location_id),
                    "loc_city_id": loc_city_id,
                    "loc_city_name": loc_city_name,
                    "loc_county_id": loc_county_id,
                    "loc_county_name": loc_county_name,
                    "loc_district_id": loc_district_id,
                    "loc_district_name": loc_district_name,
                    "loc_location_id": loc_location_id,
                    "loc_location_name": loc_location_name,
                    "loc_municipal_area": loc_municipal_area,
                    
                    # Property information
                    "property_id": property_id,
                    "property_name": property_name,
                    "property_type_name": property_type_name,
                    "property_subtype_name": property_subtype_name,
                    "municipal_property_type": municipal_property_type,
                    
                    # Property attributes - direct from API response
                    "attr_unit": None,  # Not available in this API response
                    "attr_floor": None,  # Not available in this API response
                    "attr_parking": None,  # Not available in this API response
                    "attr_land_number": None,  # Not available in this API response
                    "attr_no_of_rooms": int(item.get("no_of_rooms")) if item.get("no_of_rooms") and str(item.get("no_of_rooms")).isdigit() else None,
                    "attr_balcony_area": None,  # Not available in this API response
                    "attr_building_name": item.get("property_name"),  # Use property_name as building name
                    "attr_building_number": None,  # Not available in this API response
                    
                    # Query parameters used
                    "query_property_type": property_type,
                    "query_activity_type": activity_type,
                    "query_currency": currency,
                    "query_measurement": measurement,
                    
                    "raw_data": json.dumps(item)  # Store raw JSON for data lineage
                }
                
                processed_records.append(processed_record)
                
            except Exception as e:
                logger.error(f"Failed to process transaction list record: {e}")
                continue
        
        logger.info(f"Processed {len(processed_records)} transaction list records from {len(raw_data)} raw records (deduplicated)")
        
        # Debug: Log first few records if no data processed
        if len(processed_records) == 0 and len(raw_data) > 0:
            logger.warning(f"No records processed. First raw record structure: {raw_data[0] if raw_data else 'No data'}")
        
        return processed_records

    @staticmethod
    def extract_pagination_info(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract pagination information from API response"""
        return {
            "page_size": raw_data.get("page_size"),
            "total_count": raw_data.get("total_count"),
            "current_page": raw_data.get("current_page"),
            "total_pages": raw_data.get("total_pages"),
        }
