#!/usr/bin/env python3
"""
Vector Database Initialization Script
Initializes the vector database with pgvector extension and required schema
"""

import os
import sys
import psycopg2
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment():
    """Load environment variables from .env file"""
    if os.path.exists('.env'):
        load_dotenv('.env')
        logger.info("Loaded environment from .env file")
    else:
        logger.warning("No .env file found, using system environment variables")
    
    config = {
        'host': os.getenv('DB_HOST', 'prypco-reiden-data.postgres.database.azure.com'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'reidin-data'),
        'user': os.getenv('DB_USER', 'reidenuser'),
        'password': os.getenv('DB_PASSWORD', 'LRZ9Xd3gaGPhbKTculQo4Q')
    }
    
    return config

def test_database_connection(config):
    """Test database connection"""
    try:
        logger.info(f"Testing connection to {config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def check_pgvector_extension(conn):
    """Check if pgvector extension is installed"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT extname, extversion 
                FROM pg_extension 
                WHERE extname = 'vector'
            """)
            
            result = cur.fetchone()
            if result:
                logger.info(f"pgvector extension found: {result[0]} version {result[1]}")
                return True
            else:
                logger.warning("pgvector extension not found")
                return False
                
    except Exception as e:
        logger.error(f"Error checking pgvector extension: {e}")
        return False

def test_vector_type(conn):
    """Test if vector type is available"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT '[1,2,3]'::vector(3)")
            logger.info("'vector' type works - Azure Flexible Server naming confirmed")
            return True
    except Exception as e:
        logger.warning(f"'vector' type failed: {e}")
        return False

def apply_vector_schema(conn):
    """Apply vector schema to the database"""
    try:
        logger.info("Applying vector schema to database...")
        
        # Read the vector schema file
        schema_file = 'create_location_vector_embeddings.sql'
        if not os.path.exists(schema_file):
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Execute the schema
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            conn.commit()
        
        logger.info("Vector schema applied successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to apply vector schema: {e}")
        return False

def verify_vector_setup(conn):
    """Verify that vector setup is working"""
    try:
        with conn.cursor() as cur:
            # Check if vector columns exist
            cur.execute("""
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_name = 'location' 
                AND column_name = 'text_embedding'
            """)
            
            result = cur.fetchone()
            if result:
                logger.info(f"text_embedding column found: {result[0]} ({result[1]})")
            else:
                logger.warning("text_embedding column not found in location table")
                return False
            
            # Check vector indexes
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes 
                WHERE tablename = 'location' 
                AND indexdef LIKE '%vector%'
            """)
            
            results = cur.fetchall()
            if results:
                logger.info(f"Found {len(results)} vector indexes")
                for result in results:
                    logger.info(f"   Index: {result[0]}")
            else:
                logger.warning("No vector indexes found")
            
            # Test vector operations
            cur.execute("""
                SELECT 
                    '[1,2,3]'::vector(3) as vec1,
                    '[4,5,6]'::vector(3) as vec2,
                    '[1,2,3]'::vector(3) <-> '[4,5,6]'::vector(3) as distance
            """)
            
            result = cur.fetchone()
            if result:
                logger.info(f"Vector operations work:")
                logger.info(f"   Vector 1: {result[0]}")
                logger.info(f"   Vector 2: {result[1]}")
                logger.info(f"   Distance: {result[2]}")
            else:
                logger.warning("Vector operations failed")
                return False
            
            return True
            
    except Exception as e:
        logger.error(f"Error verifying vector setup: {e}")
        return False

def main():
    """Main initialization function"""
    logger.info("🚀 Starting Vector Database Initialization")
    
    # Step 1: Load environment configuration
    config = load_environment()
    logger.info(f"Database: {config['database']} on {config['host']}:{config['port']}")
    
    # Step 2: Test database connection
    conn = test_database_connection(config)
    if not conn:
        logger.error("Cannot proceed without database connection")
        sys.exit(1)
    
    try:
        # Step 3: Check pgvector extension
        if not check_pgvector_extension(conn):
            logger.error("pgvector extension is not installed")
            sys.exit(1)
        
        # Step 4: Test vector type
        if not test_vector_type(conn):
            logger.error("Vector type is not available")
            sys.exit(1)
        
        # Step 5: Apply vector schema
        if not apply_vector_schema(conn):
            logger.error("Failed to apply vector schema")
            sys.exit(1)
        
        # Step 6: Verify setup
        if not verify_vector_setup(conn):
            logger.error("Vector setup verification failed")
            sys.exit(1)
        
        logger.info("🎉 Vector Database Initialization Completed Successfully!")
        logger.info("✅ pgvector extension is installed and working")
        logger.info("✅ Vector schema has been applied")
        logger.info("✅ Vector operations are functional")
        logger.info("✅ Database is ready for vector operations")
        
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
