import logging
import time
from typing import List, Dict, Any, Optional
import numpy as np
from sentence_transformers import SentenceTransformer
from database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LocationVectorEmbeddings:
    """
    Professional vector embeddings generator for location data.
    
    This class handles:
    - Loading and preprocessing location text data
    - Generating vector embeddings using sentence-transformers
    - Batch processing for performance
    - Database updates with proper error handling
    """
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """
        Initialize the vector embeddings generator.
        
        Args:
            model_name: Sentence transformer model name (default: all-MiniLM-L6-v2)
        """
        self.db = DatabaseManager()
        self.model_name = model_name
        self.model = None
        self.embedding_dimension = 384  # Dimension for all-MiniLM-L6-v2
        
    def load_model(self) -> None:
        """Load the sentence transformer model."""
        try:
            logger.info(f"Loading sentence transformer model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            logger.info("✅ Model loaded successfully")
        except Exception as e:
            logger.error(f"❌ Failed to load model: {e}")
            raise
    
    def get_locations_for_embedding(self, batch_size: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve location records that need vector embeddings.
        
        Args:
            batch_size: Number of records to retrieve
            offset: Starting offset for pagination
            
        Returns:
            List of location records with searchable text
        """
        try:
            query = """
                SELECT location_id, searchable_text, text_embedding
                FROM location 
                WHERE searchable_text IS NOT NULL 
                AND searchable_text != ''
                AND (text_embedding IS NULL OR array_length(text_embedding, 1) IS NULL)
                ORDER BY location_id
                LIMIT %s OFFSET %s
            """
            
            results = self.db.run_query(query, [batch_size, offset])
            
            # Convert to list of dictionaries
            locations = []
            for row in results:
                locations.append({
                    'location_id': row[0],
                    'searchable_text': row[1],
                    'text_embedding': row[2]
                })
            
            logger.info(f"Retrieved {len(locations)} locations for embedding generation")
            return locations
            
        except Exception as e:
            logger.error(f"Failed to retrieve locations: {e}")
            raise
    
    def generate_embeddings(self, texts: List[str]) -> np.ndarray:
        """
        Generate vector embeddings for a list of texts.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            Numpy array of embeddings
        """
        try:
            if not self.model:
                self.load_model()
            
            logger.info(f"Generating embeddings for {len(texts)} texts")
            embeddings = self.model.encode(texts, show_progress_bar=True)
            logger.info(f"✅ Generated embeddings with shape: {embeddings.shape}")
            return embeddings
            
        except Exception as e:
            logger.error(f"❌ Failed to generate embeddings: {e}")
            raise
    
    def update_location_embeddings(self, location_embeddings: List[Dict[str, Any]]) -> None:
        """
        Update location records with their vector embeddings.
        
        Args:
            location_embeddings: List of dicts with location_id and embedding
        """
        if not location_embeddings:
            logger.warning("No embeddings to update")
            return
        
        try:
            # Prepare batch update query
            update_query = """
                UPDATE location 
                SET text_embedding = %s::vector
                WHERE location_id = %s
            """
            
            # Prepare data for batch update
            update_data = []
            for item in location_embeddings:
                # Convert numpy array to PostgreSQL vector format
                embedding_vector = f"[{','.join(map(str, item['embedding']))}]"
                update_data.append((embedding_vector, item['location_id']))
            
            # Execute batch update
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(update_query, update_data)
                    conn.commit()
            
            logger.info(f"✅ Updated {len(location_embeddings)} location embeddings")
            
        except Exception as e:
            logger.error(f"❌ Failed to update embeddings: {e}")
            raise
    
    def process_batch(self, batch_size: int = 1000) -> int:
        """
        Process a batch of locations for vector embedding generation.
        
        Args:
            batch_size: Number of records to process in this batch
            
        Returns:
            Number of records processed
        """
        try:
            # Get locations that need embeddings
            locations = self.get_locations_for_embedding(batch_size)
            
            if not locations:
                logger.info("No locations need embedding generation")
                return 0
            
            # Extract texts for embedding
            texts = [loc['searchable_text'] for loc in locations]
            
            # Generate embeddings
            embeddings = self.generate_embeddings(texts)
            
            # Prepare data for database update
            location_embeddings = []
            for i, location in enumerate(locations):
                location_embeddings.append({
                    'location_id': location['location_id'],
                    'embedding': embeddings[i]
                })
            
            # Update database
            self.update_location_embeddings(location_embeddings)
            
            return len(location_embeddings)
            
        except Exception as e:
            logger.error(f"❌ Batch processing failed: {e}")
            raise
    
    def process_all_locations(self, batch_size: int = 1000) -> None:
        """
        Process all locations for vector embedding generation.
        
        Args:
            batch_size: Number of records to process per batch
        """
        try:
            logger.info("🚀 Starting vector embedding generation for all locations")
            start_time = time.time()
            
            total_processed = 0
            offset = 0
            
            while True:
                logger.info(f"Processing batch starting at offset {offset}")
                batch_start = time.time()
                
                processed = self.process_batch(batch_size)
                
                if processed == 0:
                    logger.info("No more locations to process")
                    break
                
                total_processed += processed
                offset += batch_size
                
                batch_duration = time.time() - batch_start
                logger.info(f"✅ Batch completed: {processed} records in {batch_duration:.2f}s")
                
                # Small delay to prevent overwhelming the database
                time.sleep(0.1)
            
            total_duration = time.time() - start_time
            logger.info(f"🎉 Vector embedding generation completed!")
            logger.info(f"📊 Total processed: {total_processed} records")
            logger.info(f"⏱️ Total time: {total_duration:.2f} seconds")
            logger.info(f"📈 Average speed: {total_processed/total_duration:.2f} records/second")
            
        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            raise
    
    def get_embedding_stats(self) -> Dict[str, Any]:
        """
        Get statistics about vector embeddings in the location table.
        
        Returns:
            Dictionary with embedding statistics
        """
        try:
            stats_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(text_embedding) as records_with_embeddings,
                    COUNT(searchable_text) as records_with_searchable_text,
                    AVG(LENGTH(searchable_text)) as avg_text_length,
                    MAX(LENGTH(searchable_text)) as max_text_length
                FROM location
            """
            
            result = self.db.run_query(stats_query)
            stats = {
                'total_records': result[0][0],
                'records_with_embeddings': result[0][1],
                'records_with_searchable_text': result[0][2],
                'avg_text_length': float(result[0][3]) if result[0][3] else 0,
                'max_text_length': result[0][4] if result[0][4] else 0
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get embedding stats: {e}")
            raise


def main():
    """Main function to run vector embedding generation."""
    try:
        # Initialize the embeddings generator
        embeddings_generator = LocationVectorEmbeddings()
        
        # Get current stats
        logger.info("📊 Current embedding statistics:")
        stats = embeddings_generator.get_embedding_stats()
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
        # Process all locations
        embeddings_generator.process_all_locations(batch_size=500)
        
        # Get final stats
        logger.info("📊 Final embedding statistics:")
        final_stats = embeddings_generator.get_embedding_stats()
        for key, value in final_stats.items():
            logger.info(f"  {key}: {value}")
            
    except Exception as e:
        logger.error(f"❌ Vector embedding generation failed: {e}")
        raise


if __name__ == "__main__":
    main()
