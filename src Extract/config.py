import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", 5432))
    DB_NAME = os.getenv("DB_NAME", "reidin_data")
    DB_USER = os.getenv("DB_USER", "reidin_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "prypco123")

    # Reidin API
    REIDIN_BASE_URL = os.getenv("REIDIN_BASE_URL", "https://api.reidin.com/api/v2")
    API_TOKEN = os.getenv("API_TOKEN")

    # API Endpoints
    LOCATIONS_ENDPOINT = 'locations/{country_code}/'
    PROPERTY_LOCATION_ENDPOINT = 'property/location/{location_id}'
    PROPERTY_DETAILS_ENDPOINT = 'property/{property_id}'
    INDICATORS_ALIASES_ENDPOINT = '{country_code}/indicators/aliased/'
    INDICATOR_LOCATION_ID_ENDPOINT = '{country_code}/indicators/location_id/'
    
    # Transaction API Endpoints
    CMA_SALES_ENDPOINT = '{country_code}/transactions/cma2-sales/'
    TRANSACTIONS_PRICE_ENDPOINT = '{country_code}/transactions/price/'
    TRANSACTION_RAW_SALES_ENDPOINT = '{country_code}/transactions/transaction_raw_sales/'
    TRANSACTION_RAW_RENTS_ENDPOINT = '{country_code}/transactions/transaction_raw_rents/'
    TRANSACTION_HISTORY_ENDPOINT = '{country_code}/transactions/history/'
    TRANSACTIONS_AVG_ENDPOINT = '{country_code}/transactions/avg/'
    TRANSACTIONS_LIST_ENDPOINT = '{country_code}/transactions/list/'
    
    # POI API Endpoints
    POI_CMA_ENDPOINT = 'poi/poi_cma'
    CMA2_RENTS_ENDPOINT = '{country_code}/transactions/cma2-rents/'

    # Request settings
    TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))
    RETRIES = int(os.getenv("REQUEST_RETRIES", 3))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
    DATABASE_BATCH_SIZE = int(os.getenv("DATABASE_BATCH_SIZE", 5000))
    INDICATORS_BATCH_SIZE = int(os.getenv("INDICATORS_BATCH_SIZE", 10))
    
    DEFAULT_PARAMS = {
        "currency": "aed",
        "measurement": "int",
        "alias": "sales-price",
        "property_type": "residential-general",
    }

    @classmethod
    def get_api_headers(cls) -> dict:
        """Get standard API headers."""
        if cls.API_TOKEN:
            return {
                'Authorization': cls.API_TOKEN,
                'Accept': 'application/json'
            }
        return {
            'Accept': 'application/json'
        }
