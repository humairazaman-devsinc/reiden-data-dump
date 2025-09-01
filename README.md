# Reidin API - Postman Collection

This project contains a Postman collection and environment setup for testing the Reidin API endpoints.

## Setup Instructions

### 1. Import Postman Collection
1. Open Postman
2. Click "Import" button
3. Select the `Reidin_API_Collection.json` file from this project
4. The collection will be imported with all endpoints pre-configured

### 2. Set Up Environment Variables
1. In Postman, click on the "Environments" tab
2. Click "Import" and select `Reidin_API_Environment.json`
3. Or manually create an environment with these variables:
   - `base_url`: The base URL for Reidin API
   - `country_code`: The country code you want to test (e.g., "TR" for Turkey)
   - `api_key`: Your Reidin API key (if required)

### 3. API Endpoints

#### Base Endpoint
- **URL**: `{{base_url}}/locations/{{country_code}}/`
- **Method**: GET
- **Description**: Retrieves location data for the specified country

#### Example Usage
- For Turkey: `{{base_url}}/locations/TR/`
- For United States: `{{base_url}}/locations/US/`

## Environment Variables

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `base_url` | Base URL for Reidin API | `https://api.reidin.com` |
| `country_code` | ISO country code | `TR`, `US`, `GB` |
| `api_key` | API authentication key | `your_api_key_here` |

## Testing

1. Select the "Reidin API" environment in Postman
2. Choose the "Get Locations" request
3. Update the `country_code` variable if needed
4. Click "Send" to test the API

## Notes

- Make sure you have valid API credentials if authentication is required
- The API may have rate limits, so be mindful of request frequency
- Check the response headers for additional information about rate limits and pagination

## Troubleshooting

- **401 Unauthorized**: Check your API key in the environment variables
- **404 Not Found**: Verify the country code is valid
- **429 Too Many Requests**: You've hit the rate limit, wait before making more requests
