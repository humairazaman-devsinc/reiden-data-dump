"""
Database Migration Runner
========================

This script runs database migrations in a controlled, professional manner.
It follows best practices for:
- Transaction management
- Error handling
- Logging
- Rollback capabilities

Author: AI Assistant
Date: 2024
"""

import logging
import os
import sys
from pathlib import Path
from database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseMigrationRunner:
    """
    Professional database migration runner.
    
    This class handles:
    - Reading SQL migration files
    - Executing migrations in transactions
    - Proper error handling and rollback
    - Logging and progress tracking
    """
    
    def __init__(self):
        """Initialize the migration runner."""
        self.db = DatabaseManager()
    
    def read_migration_file(self, file_path: str) -> str:
        """
        Read SQL migration file.
        
        Args:
            file_path: Path to the SQL migration file
            
        Returns:
            SQL content as string
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            logger.info(f"✅ Read migration file: {file_path}")
            return content
        except Exception as e:
            logger.error(f"❌ Failed to read migration file {file_path}: {e}")
            raise
    
    def execute_migration(self, sql_content: str, migration_name: str) -> bool:
        """
        Execute a database migration with proper transaction handling.
        
        Args:
            sql_content: SQL content to execute
            migration_name: Name of the migration for logging
            
        Returns:
            True if successful, False otherwise
        """
        conn = None
        try:
            logger.info(f"🚀 Starting migration: {migration_name}")
            
            # Get database connection
            conn = self.db.get_connection()
            
            # Start transaction
            conn.autocommit = False
            
            # Split SQL into individual statements
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            logger.info(f"Executing {len(statements)} SQL statements")
            
            # Execute each statement
            with conn.cursor() as cur:
                for i, statement in enumerate(statements, 1):
                    if statement:
                        logger.info(f"Executing statement {i}/{len(statements)}")
                        cur.execute(statement)
            
            # Commit transaction
            conn.commit()
            logger.info(f"✅ Migration completed successfully: {migration_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {migration_name}")
            logger.error(f"Error: {e}")
            
            # Rollback transaction
            if conn:
                try:
                    conn.rollback()
                    logger.info("🔄 Transaction rolled back")
                except Exception as rollback_error:
                    logger.error(f"❌ Rollback failed: {rollback_error}")
            
            return False
            
        finally:
            # Return connection to pool
            if conn:
                self.db.return_connection(conn)
    
    def run_migration_file(self, file_path: str) -> bool:
        """
        Run a migration from a SQL file.
        
        Args:
            file_path: Path to the SQL migration file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                logger.error(f"❌ Migration file not found: {file_path}")
                return False
            
            # Read migration file
            sql_content = self.read_migration_file(file_path)
            
            # Get migration name from file path
            migration_name = os.path.basename(file_path)
            
            # Execute migration
            return self.execute_migration(sql_content, migration_name)
            
        except Exception as e:
            logger.error(f"❌ Failed to run migration from file {file_path}: {e}")
            return False
    
    def verify_migration(self) -> bool:
        """
        Verify that the migration was successful.
        
        Returns:
            True if verification passes, False otherwise
        """
        try:
            logger.info("🔍 Verifying migration...")
            
            # Check if vector extension is available
            vector_check = self.db.run_query("SELECT * FROM pg_extension WHERE extname = 'vector';")
            if not vector_check:
                logger.error("❌ Vector extension not found")
                return False
            
            # Check if text_embedding column exists
            column_check = self.db.run_query("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'location' AND column_name = 'text_embedding'
            """)
            if not column_check:
                logger.error("❌ text_embedding column not found")
                return False
            
            # Check if searchable_text column exists
            searchable_check = self.db.run_query("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'location' AND column_name = 'searchable_text'
            """)
            if not searchable_check:
                logger.error("❌ searchable_text column not found")
                return False
            
            # Check if indexes exist
            index_check = self.db.run_query("""
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = 'location' 
                AND indexname IN ('idx_location_text_embedding_cosine', 'idx_location_searchable_text_gin')
            """)
            if len(index_check) < 2:
                logger.error("❌ Required indexes not found")
                return False
            
            # Check if trigger exists
            trigger_check = self.db.run_query("""
                SELECT trigger_name 
                FROM information_schema.triggers 
                WHERE event_object_table = 'location' 
                AND trigger_name = 'update_location_searchable_text'
            """)
            if not trigger_check:
                logger.error("❌ Required trigger not found")
                return False
            
            logger.info("✅ Migration verification passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Migration verification failed: {e}")
            return False


def main():
    """Main function to run the migration."""
    try:
        # Initialize migration runner
        migration_runner = DatabaseMigrationRunner()
        
        # Get migration file path
        migration_file = Path(__file__).parent.parent / "create_location_vector_embeddings.sql"
        
        if not migration_file.exists():
            logger.error(f"❌ Migration file not found: {migration_file}")
            sys.exit(1)
        
        # Run migration
        logger.info("🚀 Starting database migration for vector embeddings")
        success = migration_runner.run_migration_file(str(migration_file))
        
        if success:
            # Verify migration
            if migration_runner.verify_migration():
                logger.info("🎉 Migration completed and verified successfully!")
                sys.exit(0)
            else:
                logger.error("❌ Migration verification failed")
                sys.exit(1)
        else:
            logger.error("❌ Migration failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Migration runner failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
