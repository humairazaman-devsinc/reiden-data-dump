# Reidin Data Importers

This directory contains importers for all the main data tables in the Reidin system. Each importer is designed to fetch data from the Reidin API and store it in the corresponding database table.

## Available Importers

### 1. Location Importer (`import_locations.py`)
- **Purpose**: Imports location data from the `locations/{country_code}/` endpoint
- **Dependencies**: None (base table)
- **Usage**: `python import_locations.py --country-code ae`


### 2. Property Importer (`import_properties.py`)
- **Purpose**: Imports property data from the `property/location/{location_id}/` endpoint
- **Dependencies**: Locations must be imported first
- **Usage**: `python import_properties.py --country-code ae`

### 3. Property Details Importer (`import_property_details.py`)
- **Purpose**: Imports detailed property information from the `property/{property_id}/` endpoint
- **Dependencies**: Properties must be imported first
- **Usage**: `python import_property_details.py --country-code ae`

### 4. Indicator Aliased Importer (`import_indicators_aliased.py`)
- **Purpose**: Imports property-level indicators from the `{country_code}/indicators/aliases/` endpoint
- **Dependencies**: Properties and locations must be imported first
- **Usage**: `python import_indicators_aliased.py --country-code ae`


### 5. Master Importer (`import_all.py`)
- **Purpose**: Runs all importers in the correct sequence
- **Dependencies**: None (manages all dependencies internally)
- **Usage**: `python import_all.py --country-code ae`

## Import Sequence

The importers should be run in this order due to data dependencies:

1. **Locations** (base table)
2. **Properties** (requires location IDs)
3. **Property Details** (requires property IDs)
4. **Indicators** (requires both property and location IDs)

## Command Line Options

All importers support the following common options:

- `--country-code`: Country code to import data for (default: "ae")
- `--limit`: Limit the number of records to process (useful for testing)
- `--dry-run`: Run without actually writing to the database

### Master Importer Additional Options

The master importer also supports skipping specific steps:

- `--skip-locations`: Skip location import
- `--skip-properties`: Skip property import
- `--skip-property-details`: Skip property details import
- `--skip-indicators`: Skip indicators import

## Usage Examples

### Import All Data (Recommended)
```bash
# Import everything for UAE
python import_all.py --country-code ae

# Import with limit for testing
python import_all.py --country-code ae --limit 10 --dry-run

# Skip certain steps
python import_all.py --country-code ae --skip-indicators
```

### Individual Imports
```bash
# Import only locations
python import_locations.py --country-code ae

# Import properties with limit
python import_properties.py --country-code ae --limit 100

```

## Data Flow

```
Locations → Properties → Property Details
     ↓           ↓           ↓
Indicators (Property Level)
```

## Logging

Each importer creates its own log file:
- `reidin_data_import.log` - Location importer
- `reidin_property_import.log` - Property importer
- `reidin_property_details_import.log` - Property details importer
- `reidin_indicators_aliased_import.log` - Property-level indicators
- `reidin_master_import.log` - Master importer

## Error Handling

- Each importer continues processing even if individual records fail
- Failed records are logged with warnings
- The master importer continues with subsequent steps even if one step fails
- All importers support dry-run mode for testing

## Performance Considerations

- Use `--limit` for testing with small datasets
- The master importer processes data sequentially to avoid overwhelming the API
- Consider running during off-peak hours for large imports
- Monitor log files for performance insights

## Troubleshooting

### Common Issues

1. **API Rate Limits**: If you encounter rate limiting, add delays between requests
2. **Database Connection**: Ensure the database is running and accessible
3. **Missing Dependencies**: Run importers in the correct sequence
4. **Memory Issues**: Use `--limit` for large datasets

### Debug Mode

To enable debug logging, modify the logging level in any importer:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Dependencies

Ensure these Python packages are installed:
- `psycopg2` - PostgreSQL adapter
- `requests` - HTTP client
- `python-dotenv` - Environment variable management

## Configuration

All importers use the configuration from `config.py`. Ensure your environment variables are set:
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `REIDIN_BASE_URL`, `API_TOKEN`
- `REQUEST_TIMEOUT`, `BATCH_SIZE`
