-- =====================================================
-- Location Table Vector Embeddings Migration Script
-- =====================================================
-- This script adds vector embedding infrastructure to the location table
-- Following best practices for database schema changes
-- =====================================================

-- Step 1: Add vector embedding column (384 dimensions for sentence-transformers)
-- Using IF NOT EXISTS to make the migration idempotent
ALTER TABLE location 
ADD COLUMN IF NOT EXISTS text_embedding vector(384);

-- Step 2: Add searchable text column that combines all relevant text fields
-- This column will contain the combined text for vector embedding generation
ALTER TABLE location 
ADD COLUMN IF NOT EXISTS searchable_text TEXT;

-- Step 3: Create function to generate searchable text from location hierarchy
-- This function combines city, county, district, and location names for semantic search
CREATE OR REPLACE FUNCTION generate_location_searchable_text() 
RETURNS TRIGGER AS $$
BEGIN
    -- Combine city, county, district, and location names for semantic search
    -- This creates a comprehensive text field for vector embedding generation
    NEW.searchable_text = COALESCE(NEW.city_name, '') || ' ' ||
                         COALESCE(NEW.county_name, '') || ' ' ||
                         COALESCE(NEW.district_name, '') || ' ' ||
                         COALESCE(NEW.location_name, '');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Step 4: Create trigger to auto-update searchable_text
-- This ensures searchable_text is always up-to-date when records are inserted/updated
DROP TRIGGER IF EXISTS update_location_searchable_text ON location;
CREATE TRIGGER update_location_searchable_text
    BEFORE INSERT OR UPDATE ON location
    FOR EACH ROW
    EXECUTE FUNCTION generate_location_searchable_text();

-- Step 5: Update existing records with searchable text
-- This populates the searchable_text column for all existing records
UPDATE location 
SET searchable_text = COALESCE(city_name, '') || ' ' ||
                     COALESCE(county_name, '') || ' ' ||
                     COALESCE(district_name, '') || ' ' ||
                     COALESCE(location_name, '');

-- Step 6: Create vector index for efficient similarity search
-- Using ivfflat index with cosine similarity for fast vector operations
CREATE INDEX IF NOT EXISTS idx_location_text_embedding_cosine 
ON location USING ivfflat (text_embedding vector_cosine_ops) 
WITH (lists = 100);

-- Step 7: Create full-text search index for searchable_text
-- This provides fast text search capabilities alongside vector search
CREATE INDEX IF NOT EXISTS idx_location_searchable_text_gin 
ON location USING gin (to_tsvector('english', searchable_text));

-- Step 8: Create composite index for common location queries
-- This optimizes queries that filter by city and county
CREATE INDEX IF NOT EXISTS idx_location_city_county 
ON location (city_name, county_name);

-- Step 9: Create index for location hierarchy queries
-- This optimizes queries that filter by district
CREATE INDEX IF NOT EXISTS idx_location_district 
ON location (district_name) 
WHERE district_name IS NOT NULL;

-- Step 10: Add comments for documentation
COMMENT ON COLUMN location.text_embedding IS 'Vector embedding (384 dimensions) for semantic search of location text';
COMMENT ON COLUMN location.searchable_text IS 'Combined text from city, county, district, and location names for embedding generation';
COMMENT ON INDEX idx_location_text_embedding_cosine IS 'Vector index for cosine similarity search on text embeddings';
COMMENT ON INDEX idx_location_searchable_text_gin IS 'Full-text search index on combined location text';
COMMENT ON INDEX idx_location_city_county IS 'Composite index for city/county filtering queries';
COMMENT ON INDEX idx_location_district IS 'Index for district filtering queries';

-- =====================================================
-- Migration completed successfully
-- =====================================================
-- The location table now has:
-- 1. text_embedding column for vector embeddings
-- 2. searchable_text column with combined location text
-- 3. Automatic trigger to maintain searchable_text
-- 4. Optimized indexes for vector and text search
-- 5. Proper documentation and comments
-- =====================================================
