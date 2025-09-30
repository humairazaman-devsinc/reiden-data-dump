import logging
import psycopg2
import json
import threading
import time

from psycopg2.extras import execute_values
from psycopg2 import pool
from typing import List, Dict, Any, Optional, Set
from config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        # Initialize connection pool for better performance
        self._pool = None
        self._pool_lock = threading.Lock()

    def _get_pool(self):
        """Get or create connection pool (thread-safe)"""
        if self._pool is None:
            with self._pool_lock:
                if self._pool is None:
                    try:
                        self._pool = psycopg2.pool.ThreadedConnectionPool(
                            minconn=20,  # Minimum connections
                            maxconn=200,  # Maximum connections (increased for very high-volume processing)
                            host=Config.DB_HOST,
                            port=Config.DB_PORT,
                            dbname=Config.DB_NAME,
                            user=Config.DB_USER,
                            password=Config.DB_PASSWORD,
                            connect_timeout=10,
                            application_name="cma_sales_importer",
                            options="-c statement_timeout=30000",  # 30 second statement timeout
                        )
                        logger.info(
                            "✅ Database connection pool initialized (10-100 connections)"
                        )
                    except Exception as e:
                        logger.error(f"❌ Failed to create connection pool: {e}")
                        raise
        return self._pool

    def get_connection(self):
        """Get connection from pool with timeout and retry logic"""
        max_retries = 3
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                pool = self._get_pool()
                # Add timeout to prevent hanging
                conn = pool.getconn()
                if conn:
                    return conn
            except Exception as e:
                logger.warning(
                    f"⚠️ Attempt {attempt + 1} failed to get connection from pool: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"❌ All attempts failed to get connection from pool")
                    # Fallback to direct connection
                    return psycopg2.connect(
                        host=Config.DB_HOST,
                        port=Config.DB_PORT,
                        dbname=Config.DB_NAME,
                        user=Config.DB_USER,
                        password=Config.DB_PASSWORD,
                        connect_timeout=10,
                        application_name="cma_sales_importer",
                        options="-c statement_timeout=30000",
                    )

    def return_connection(self, conn):
        """Return connection to pool with proper error handling"""
        try:
            if conn and not conn.closed:
                if self._pool:
                    self._pool.putconn(conn)
                else:
                    # If no pool, close the connection
                    conn.close()
        except Exception as e:
            logger.error(f"❌ Failed to return connection to pool: {e}")
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception as close_error:
                logger.error(f"❌ Failed to close connection: {close_error}")
                pass

    def run_query(
        self, query: str, params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Run a query and return the results"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error("Query failed: %s", e)
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def transform_data(self, update_query: str, params: Optional[List[Any]] = None) -> None:
        """Transform data in the database using a provided update query."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(update_query, params)
                conn.commit()
                logger.info("Data transformation completed successfully.")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error("Data transformation failed: %s", e)
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def bulk_insert(self, query: str, data_list: List[Dict[str, Any]]) -> None:
        """Generic bulk insert method with error handling"""
        with self.get_connection() as conn, conn.cursor() as cur:
            try:
                values = [tuple(d.values()) for d in data_list]
                logger.info("Values: %s", values[0])
                execute_values(cur, query, values)
                conn.commit()
                logger.info("Inserted/updated %d rows", len(values))
            except Exception as e:
                conn.rollback()
                logger.error("Bulk insert failed: %s", e)
                raise

    def _bulk_insert_with_copy_optimized(
        self, batch_data: List[Dict[str, Any]], column_order: List[str]
    ) -> None:
        """Ultra-fast bulk insert using PostgreSQL COPY with duplicate handling"""
        import io
        import json

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Create a StringIO buffer for COPY
                    output = io.StringIO()

                    # Prepare data for COPY format (tab-delimited)
                    for item in batch_data:
                        row_data = []
                        for col in column_order:
                            value = item.get(col)
                            if value is None:
                                row_data.append("\\N")  # NULL representation for COPY
                            elif isinstance(value, dict):
                                # Convert dict to JSON string and escape for COPY
                                json_str = (
                                    json.dumps(value)
                                    .replace("\t", "\\t")
                                    .replace("\n", "\\n")
                                    .replace("\r", "\\r")
                                )
                                row_data.append(json_str)
                            else:
                                # Convert to string and escape for COPY
                                str_value = (
                                    str(value)
                                    .replace("\t", "\\t")
                                    .replace("\n", "\\n")
                                    .replace("\r", "\\r")
                                )
                                row_data.append(str_value)

                        output.write("\t".join(row_data) + "\n")

                    # Reset buffer position
                    output.seek(0)

                    # Use COPY for ultra-fast bulk insert
                    columns_str = ", ".join(column_order)
                    copy_query = f"COPY transaction_raw_rent ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"

                    try:
                        cursor.copy_expert(copy_query, output)
                        conn.commit()
                        logger.info(
                            f"✅ COPY insert successful for {len(batch_data)} records"
                        )
                    except Exception as copy_error:
                        # If COPY fails due to duplicates, fall back to execute_values with ON CONFLICT
                        logger.warning(
                            f"⚠️ COPY failed due to duplicates, falling back to execute_values: {copy_error}"
                        )
                        conn.rollback()
                        self._bulk_insert_with_execute_values_fallback(
                            batch_data, column_order
                        )

        except Exception as e:
            logger.error(f"❌ Failed bulk insert: {e}")
            raise

    def _bulk_insert_with_execute_values_fallback(
        self, batch_data: List[Dict[str, Any]], column_order: List[str]
    ) -> None:
        """Fallback method using execute_values with ON CONFLICT for duplicate handling"""
        import json

        # Build dynamic insert query with ON CONFLICT
        columns_str = ", ".join(column_order)
        insert_query = f"""
        INSERT INTO transaction_raw_rent ({columns_str}) 
        VALUES %s
        ON CONFLICT (loc_location_id, currency, measurement, date, price, size) 
        WHERE ((date IS NOT NULL) AND (price IS NOT NULL))
        DO NOTHING
        """

        # Prepare data for insertion
        values_list = []
        for item in batch_data:
            processed_item = {}
            for key, value in item.items():
                if isinstance(value, dict) and key != "raw_data":
                    processed_item[key] = json.dumps(value)
                elif key == "raw_data":
                    processed_item[key] = (
                        json.dumps(value) if isinstance(value, dict) else value
                    )
                else:
                    processed_item[key] = value

            # Create values tuple in the same order as column_order
            values = tuple(processed_item.get(col) for col in column_order)
            values_list.append(values)

        # Execute the insert with ON CONFLICT handling
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values_list)
                conn.commit()
                logger.info(
                    f"✅ execute_values fallback successful for {len(batch_data)} records"
                )

    def _bulk_insert_with_copy(self, ordered_data):
        """Ultra-fast bulk insert using PostgreSQL COPY for large batches"""
        import io
        import json

        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                # Create a StringIO buffer for COPY
                buffer = io.StringIO()

                for row in ordered_data:
                    # Convert row to tab-separated values
                    # Handle None values and JSON fields
                    formatted_row = []
                    for i, value in enumerate(row):
                        if value is None:
                            formatted_row.append(
                                "\\N"
                            )  # PostgreSQL NULL representation
                        elif i in [
                            26,
                            27,
                        ]:  # parent_property and raw_data columns (JSON)
                            # Convert JSON objects to strings
                            if isinstance(value, dict):
                                formatted_row.append(
                                    json.dumps(value)
                                    .replace("\t", " ")
                                    .replace("\n", " ")
                                )
                            else:
                                formatted_row.append(
                                    str(value).replace("\t", " ").replace("\n", " ")
                                )
                        else:
                            formatted_row.append(
                                str(value).replace("\t", " ").replace("\n", " ")
                            )

                    buffer.write("\t".join(formatted_row) + "\n")

                buffer.seek(0)

                # Use COPY for ultra-fast bulk insert
                cur.copy_from(
                    buffer,
                    "cma_sales",
                    columns=[
                        "property_id",
                        "alias",
                        "currency",
                        "measurement",
                        "property_type",
                        "property_subtype",
                        "comparable_property_id",
                        "comparable_property_name",
                        "size",
                        "price",
                        "price_per_size",
                        "transaction_date",
                        "city_id",
                        "city_name",
                        "county_id",
                        "county_name",
                        "district_id",
                        "district_name",
                        "location_id",
                        "location_name",
                        "municipal_area",
                        "activity_type",
                        "no_of_bedrooms",
                        "number_of_unit",
                        "property_nature",
                        "number_of_floors",
                        "parent_property",
                        "raw_data",
                    ],
                    sep="\t",
                    null="\\N",
                )
                conn.commit()

                logger.info(
                    f"🚀 Ultra-fast COPY insert completed for {len(ordered_data)} records"
                )

        except Exception as e:
            logger.error(f"❌ Failed COPY bulk insert: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def get_transaction_raw_rent_count(self) -> int:
        """Get the total count of records in transaction_raw_rent table"""
        try:
            with self.get_connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM transaction_raw_rent")
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get transaction raw rent count: {e}")
            return 0

    def get_duplicate_count(self) -> int:
        """Get the count of duplicate records in transaction_raw_rent table"""
        try:
            with self.get_connection() as conn, conn.cursor() as cur:
                # Count records that have duplicates based on the unique constraint
                cur.execute(
                    """
                    SELECT COUNT(*) - COUNT(DISTINCT (location_id, currency, measurement, date, price, size))
                    FROM transaction_raw_rent 
                    WHERE date IS NOT NULL AND price IS NOT NULL
                """
                )
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get duplicate count: {e}")
            return 0

    def _determine_column_type(
        self, column_name: str, sample_data: List[Dict[str, Any]] = None
    ) -> str:
        """Determine the appropriate PostgreSQL data type for a column based on sample data"""
        if not sample_data:
            return "TEXT"  # Default fallback

        # Analyze sample data for this column
        values = []
        for record in sample_data:
            if column_name in record and record[column_name] is not None:
                values.append(record[column_name])

        if not values:
            return "TEXT"

        # Check if all values are integers
        try:
            for value in values:
                if isinstance(value, str) and not value.isdigit():
                    break
                int(value)
            return "INTEGER"
        except (ValueError, TypeError):
            pass

        # Check if all values are floats
        try:
            for value in values:
                float(value)
            return "DECIMAL(15,2)"  # For monetary values
        except (ValueError, TypeError):
            pass

        # Check if all values are booleans
        if all(isinstance(v, bool) for v in values):
            return "BOOLEAN"

        # Check if all values are dates/timestamps
        if all(isinstance(v, str) and ("T" in v or "-" in v) for v in values):
            return "TIMESTAMP"

        # Check for common numeric patterns
        if all(isinstance(v, (int, float)) for v in values):
            return "DECIMAL(15,2)"

        # Check for JSON objects/arrays
        if any(isinstance(v, (dict, list)) for v in values):
            return "JSONB"

        # Check string length to determine VARCHAR size
        max_length = max(len(str(v)) for v in values if v is not None)
        if max_length <= 50:
            return "VARCHAR(50)"
        elif max_length <= 255:
            return "VARCHAR(255)"
        elif max_length <= 1000:
            return "VARCHAR(1000)"
        else:
            return "TEXT"
