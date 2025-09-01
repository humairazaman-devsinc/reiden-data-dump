# Reidin API Data Import Setup Instructions

This project sets up a complete pipeline to fetch data from the Reidin API and store it in PostgreSQL for analysis.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ installed
- Git (optional)

## Quick Start

### 1. Start PostgreSQL Database

```bash
# Start PostgreSQL and pgAdmin
docker-compose up -d

# Verify containers are running
docker-compose ps
```

**Database Details:**
- Host: `localhost`
- Port: `5432`
- Database: `reidin_data`
- Username: `reidin_user`
- Password: `prypco123`

**pgAdmin Access:**
- URL: `http://localhost:8080`
- Email: `sheraz.ahmed.inm@prypco.com`
- Password: `prypco123`

### 2. Install Python Dependencies

```bash
# Install required packages
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
# Copy example environment file
cp env_example.txt .env

# Edit .env file with your settings
# Update API_TOKEN with your actual token
```

### 4. Test the Setup

```bash
# Test with dry run (fetches data but doesn't store)
python main.py --dry-run

# Test with actual data import
python main.py --country-code ae --groups-id 8200
```

## Usage Examples

### Basic Usage

```bash
# Import data for UAE (default)
python main.py

# Import data for specific country
python main.py --country-code tr

# Import with group filter
python main.py --country-code ae --groups-id 8200

# Dry run to see what would be imported
python main.py --dry-run
```

### Advanced Usage

```bash
# Import with custom batch size
python main.py --limit 50

# Import specific group data
python main.py --country-code ae --groups-id 8200 --limit 200
```

## Database Schema

The project creates three main tables:

### 1. `indicator_data`
Stores the main indicator information:
- Location details (name, coordinates, level)
- Indicator details (name, group, unit)
- Status flags and frequencies
- Import metadata

### 2. `time_series_data`
Stores historical time series data:
- Date and value information
- Multiple currency values (AED, EUR, TRY, USD)
- Difference calculations
- Date breakdowns (year, month, quarter, etc.)

### 3. `location_hierarchy`
Stores location hierarchy information:
- Location relationships (parent-child)
- Level information (Country, City, County, District)
- Geographic hierarchy

## Data Flow

1. **API Fetch**: `api_client.py` fetches data from Reidin API
2. **Data Processing**: `data_processor.py` transforms raw API data
3. **Database Storage**: `database.py` stores processed data in PostgreSQL
4. **Main Orchestration**: `main.py` coordinates the entire process

## Monitoring and Logs

- Logs are written to `reidin_data_import.log`
- Console output shows real-time progress
- Database operations are logged with timestamps

## Troubleshooting

### Common Issues

1. **Database Connection Error**
   ```bash
   # Check if PostgreSQL is running
   docker-compose ps
   
   # Restart if needed
   docker-compose restart postgres
   ```

2. **API Authentication Error**
   - Verify your API token in `.env` file
   - Check if token is expired
   - Ensure correct API base URL

3. **Memory Issues with Large Datasets**
   - Reduce batch size: `--limit 50`
   - Monitor system resources
   - Consider processing in smaller chunks

### Useful Commands

```bash
# View logs
tail -f reidin_data_import.log

# Check database tables
docker exec -it reidin_postgres psql -U reidin_user -d reidin_data -c "\dt"

# View sample data
docker exec -it reidin_postgres psql -U reidin_user -d reidin_data -c "SELECT * FROM indicator_data LIMIT 5;"

# Stop all services
docker-compose down

# Remove all data (WARNING: This deletes all data)
docker-compose down -v
```

## Performance Tips

1. **Batch Processing**: Use appropriate `--limit` values
2. **Indexing**: Database indexes are automatically created
3. **Connection Pooling**: Database connections are managed efficiently
4. **Error Handling**: Failed records are logged but don't stop the process

## Next Steps

After successful data import, you can:

1. **Query Data**: Use SQL to analyze the imported data
2. **Create Views**: Build database views for common queries
3. **Set Up Monitoring**: Create scheduled jobs for regular data updates
4. **Build Analytics**: Connect to BI tools or create custom dashboards
