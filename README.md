![image](https://github.com/user-attachments/assets/fa3788a2-9a5b-47bf-b6ef-f818ba62a404)

# GigAPI Query Engine in Bun

## Overview

GigAPI provides a SQL interface to query time-series data stored in GigAPI's parquet storage with intelligent file resolution based on metadata and time ranges. 
## Quick Start

```bash
# Install dependencies
bun install

# Start the server
bun start

# Default: http://localhost:8080 (configurable via PORT env var)
```

## API Endpoints

### Query Data

```bash
POST /query?db=mydb
Content-Type: application/json

{
  "query": "SELECT time, location, temperature FROM weather WHERE time >= '2025-04-01T00:00:00'"
}
```

### Debug Endpoints

- `GET /debug/mydb/weather`: Inspect metadata and file structure
- `GET /fs/{path}`: Browse files and directories
- `POST /sql`: Execute raw DuckDB queries

## Data Structure

```
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
```

## Query Processing Logic

1. Parse SQL query to extract measurement name and time range
2. Find relevant parquet files using metadata
3. Use DuckDB to execute optimized queries against selected files
4. Post-process results to handle BigInt timestamps

## Configuration

- `PORT`: Server port (default: 8080)
- `DATA_DIR`: Path to data directory (default: ./data)

## Notes for Developers

- File paths in metadata.json may contain absolute paths; the system handles both absolute and relative paths
- Time fields are converted from nanosecond BigInt to ISO strings
- Add `?debug=true` to query requests for detailed troubleshooting information
