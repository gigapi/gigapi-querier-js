![image](https://github.com/user-attachments/assets/fa3788a2-9a5b-47bf-b6ef-f818ba62a404)

# <img src="https://bun.sh/logo.svg" height=28> GigAPI Query Engine

GigAPI Bun provides a SQL interface to query time-series using GigAPI Catalog Metadata and DuckDB

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Quick Start

```bash
# Install dependencies
bun install

# Start the server
bun start
```

### Configuration

- `PORT`: Server port (default: 8080)
- `DATA_DIR`: Path to data directory (default: ./data)

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

- `GET /debug/{db}/{table}`: Inspect metadata and file structure
- `GET /fs/{path}`: Browse files and directories
- `POST /sql`: Execute raw DuckDB queries

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Data Structure

```
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
```

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Query Processing Logic

1. Parse SQL query to extract measurement name and time range
2. Find relevant parquet files using metadata
3. Use DuckDB to execute optimized queries against selected files
4. Post-process results to handle BigInt timestamps


## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Notes for Developers

- File paths in metadata.json may contain absolute paths; the system handles both absolute and relative paths
- Time fields are converted from nanosecond BigInt to ISO strings
- Add `?debug=true` to query requests for detailed troubleshooting information

-----

## License

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/06/AGPLv3_Logo.svg/2560px-AGPLv3_Logo.svg.png" width=200>

> Gigapipe is released under the GNU Affero General Public License v3.0 ©️ HEPVEST BV, All Rights Reserved.
