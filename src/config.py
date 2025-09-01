import os
from dotenv import load_dotenv
from sqlalchemy.ext.declarative import declarative_base

# Load environment variables
load_dotenv()

# Create Base for SQLAlchemy models
Base = declarative_base()


class Config:
    # Database Configuration
    DATABASE_URL = os.getenv(
        'DATABASE_URL', "postgresql://reidin_user:prypco123@reidin_postgres:5432/reidin_data")

    # API Configuration
    API_BASE_URL = os.getenv('API_BASE_URL', 'https://api.reidin.com/api/v2/')
    API_TOKEN = os.getenv('API_TOKEN', '')

    # API Endpoints
    INDICATORS_ENDPOINT = '/{country_code}/indicators/demography/'
    LOCATIONS_ENDPOINT = '/locations/{country_code}/'

    # Request Configuration
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 30))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))

    # Data Processing Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # Application Environment
    ENV = os.getenv('ENV', 'development')
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

    @staticmethod
    def is_production():
        return Config.ENV == 'production'
