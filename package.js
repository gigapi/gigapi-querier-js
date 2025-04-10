{
  "name": "gigapi_bun",
  "version": "0.1.0",
  "description": "Query App for GigAPI using Bun and DuckDB",
  "main": "server.js",
  "type": "module",
  "scripts": {
    "start": "bun run QueryServer.js"
  },
  "dependencies": {
    "@evan/duckdb": "^0.1.5",
    "hono": "^3.12.0"
  },
  "devDependencies": {
    "bun-types": "latest"
  },
  "engines": {
    "bun": ">=1.0.0"
  },
  "author": "",
  "license": "AGPLv3"
}
