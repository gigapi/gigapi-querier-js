import { Hono } from 'hono';
import { logger } from 'hono/logger';
import { cors } from 'hono/cors';
import { prettyJSON } from 'hono/pretty-json';
import path from 'path';
import fs from 'fs';
import QueryClient from './QueryClient.js';

const app = new Hono();
const PORT = process.env.PORT || 8080;
const DATA_DIR = process.env.DATA_DIR || './data';

// Middleware
app.use('*', logger());
app.use('*', cors());
app.use('*', prettyJSON());

// Initialize QueryClient
const queryClient = new QueryClient(DATA_DIR);

/**
 * Helper function to process query results
 * Converts BigInt values to strings for JSON serialization
 */
function processBigIntInResults(results) {
  if (!results || !Array.isArray(results)) return results;
  
  return results.map(row => {
    if (typeof row !== 'object' || row === null) return row;
    
    const newRow = {};
    for (const [key, value] of Object.entries(row)) {
      // Convert BigInt to string
      if (typeof value === 'bigint') {
        newRow[key] = value.toString();
      } else if (Array.isArray(value)) {
        newRow[key] = processBigIntInResults(value);
      } else if (typeof value === 'object' && value !== null) {
        newRow[key] = processBigIntInObject(value);
      } else {
        newRow[key] = value;
      }
    }
    return newRow;
  });
}

/**
 * Helper function to process objects with BigInt values
 */
function processBigIntInObject(obj) {
  if (typeof obj !== 'object' || obj === null) return obj;
  
  const newObj = {};
  for (const [key, value] of Object.entries(obj)) {
    if (typeof value === 'bigint') {
      newObj[key] = value.toString();
    } else if (Array.isArray(value)) {
      newObj[key] = processBigIntInResults(value);
    } else if (typeof value === 'object' && value !== null) {
      newObj[key] = processBigIntInObject(value);
    } else {
      newObj[key] = value;
    }
  }
  return newObj;
}

// Health check endpoint
app.get('/health', (c) => {
  return c.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Query endpoint
app.post('/query', async (c) => {
  try {
    const params = await c.req.json();
    
    if (!params.query) {
      return c.json({ error: 'Missing query parameter' }, 400);
    }
    
    // Extract database name from query params or URL
    const dbName = c.req.query('db') || params.db || 'mydb';
    
    console.log(`Executing query for database '${dbName}':`, params.query);
    
    try {
      const result = await queryClient.query(params.query, dbName);
      
      // For aggregate queries, ensure counts aren't null
      const processedResults = result.map(row => {
        const fixedRow = {};
        for (const [key, value] of Object.entries(row)) {
          if (key.includes('count') && value === null) {
            // Always use 0 for null counts
            fixedRow[key] = 0;
          } else if (typeof value === 'bigint') {
            // Convert BigInt to string for JSON compatibility
            fixedRow[key] = value.toString();
          } else if (value !== null && typeof value === 'object' && Object.keys(value).includes('0') && !Array.isArray(value)) {
            // Check if this is a string-like object (with numeric keys and a 'ptr' property)
            if ('ptr' in value) {
              // Convert to an actual string
              let str = '';
              let i = 0;
              while (value[i.toString()] !== undefined) {
                str += value[i.toString()];
                i++;
              }
              fixedRow[key] = str;
            } else {
              fixedRow[key] = value;
            }
          } else {
            fixedRow[key] = value;
          }
        }
        return fixedRow;
      });
      
      return c.json({ results: processedResults });
    } catch (error) {
      console.error('Query execution error:', error);
      return c.json({ error: error.message }, 500);
    }
  } catch (error) {
    console.error('Query endpoint error:', error);
    return c.json({ error: error.message }, 500);
  }
});

// Raw SQL endpoint (for debugging)
app.post('/sql', async (c) => {
  try {
    const params = await c.req.json();
    
    if (!params.sql) {
      return c.json({ error: 'Missing sql parameter' }, 400);
    }
    
    console.log('Executing raw SQL:', params.sql);
    
    const result = queryClient.connection.query(params.sql);
    
    // Process results to convert BigInt to strings
    const processedResults = processBigIntInResults(result);
    
    return c.json({ results: processedResults });
  } catch (error) {
    console.error('SQL endpoint error:', error);
    return c.json({ error: error.message }, 500);
  }
});

// DEBUG endpoint to inspect data structure
app.get('/debug/:db/:measurement', async (c) => {
  try {
    const dbName = c.req.param('db');
    const measurement = c.req.param('measurement');
    
    if (!dbName || !measurement) {
      return c.json({ error: 'Database and measurement names are required' }, 400);
    }
    
    const basePath = path.join(queryClient.dataDir, dbName, measurement);
    
    if (!fs.existsSync(basePath)) {
      return c.json({ 
        error: 'Measurement not found',
        path: basePath
      }, 404);
    }
    
    // Find all files
    const parquetFiles = [];
    const metadataFiles = [];
    
    // Function to recursively find files
    async function findFiles(dirPath, level = 0) {
      try {
        const entries = await fs.promises.readdir(dirPath, { withFileTypes: true });
        
        for (const entry of entries) {
          const fullPath = path.join(dirPath, entry.name);
          
          if (entry.isFile()) {
            if (entry.name.endsWith('.parquet')) {
              parquetFiles.push({
                path: fullPath,
                name: entry.name,
                relativePath: path.relative(basePath, fullPath)
              });
            } else if (entry.name === 'metadata.json') {
              try {
                const metadata = JSON.parse(await fs.promises.readFile(fullPath, 'utf8'));
                metadataFiles.push({
                  path: fullPath,
                  relativePath: path.relative(basePath, fullPath),
                  content: metadata
                });
              } catch (error) {
                metadataFiles.push({
                  path: fullPath,
                  relativePath: path.relative(basePath, fullPath),
                  error: error.message
                });
              }
            }
          } else if (entry.isDirectory() && level < 10) { // Prevent infinite recursion
            await findFiles(fullPath, level + 1);
          }
        }
      } catch (error) {
        console.error(`Error reading directory ${dirPath}:`, error);
      }
    }
    
    await findFiles(basePath);
    
    // Test file paths in metadata
    const filePathTests = [];
    for (const metadata of metadataFiles) {
      if (metadata.content && metadata.content.files) {
        for (const file of metadata.content.files) {
          const filePathAbsolute = file.path;
          const filePathRelative = path.join(path.dirname(metadata.path), path.basename(file.path));
          
          filePathTests.push({
            metadataPath: metadata.path,
            filePathInMetadata: filePathAbsolute,
            alternativePath: filePathRelative,
            existsAtMetadataPath: fs.existsSync(filePathAbsolute),
            existsAtRelativePath: fs.existsSync(filePathRelative)
          });
        }
      }
    }
    
    // Process any BigInt values before sending response
    const result = {
      basePath,
      exists: fs.existsSync(basePath),
      parquetFiles: parquetFiles,
      metadataFiles: processBigIntInResults(metadataFiles),
      filePathTests: filePathTests
    };
    
    return c.json(result);
  } catch (error) {
    console.error('Debug endpoint error:', error);
    return c.json({ error: error.message }, 500);
  }
});

// Filesystem inspection endpoint
app.get('/fs/:path(*)', async (c) => {
  try {
    let requestedPath = c.req.param('path');
    
    // Prevent path traversal attacks
    requestedPath = path.normalize(requestedPath).replace(/^(\.\.[\/\\])+/, '');
    
    // Join with data directory to ensure we're only looking within our data
    const fullPath = path.join(queryClient.dataDir, requestedPath);
    
    // Check if path exists
    if (!fs.existsSync(fullPath)) {
      return c.json({ 
        error: 'Path not found',
        requestedPath,
        fullPath
      }, 404);
    }
    
    const stats = await fs.promises.stat(fullPath);
    
    if (stats.isFile()) {
      // If it's a file, return file info
      if (fullPath.endsWith('.json')) {
        // For JSON files, return the parsed content
        try {
          const content = await fs.promises.readFile(fullPath, 'utf8');
          return c.json({
            type: 'file',
            path: fullPath,
            size: stats.size,
            content: JSON.parse(content)
          });
        } catch (error) {
          return c.json({
            type: 'file',
            path: fullPath,
            size: stats.size,
            error: `Failed to parse JSON: ${error.message}`
          });
        }
      } else if (fullPath.endsWith('.parquet')) {
        // For parquet files, return file info and option to query
        return c.json({
          type: 'file',
          path: fullPath,
          size: stats.size,
          format: 'parquet',
          queryUrl: `/sql`,
          queryBody: {
            sql: `SELECT * FROM read_parquet('${fullPath}') LIMIT 10`
          }
        });
      } else {
        // For other files, just return basic info
        return c.json({
          type: 'file',
          path: fullPath,
          size: stats.size
        });
      }
    } else if (stats.isDirectory()) {
      // If it's a directory, list contents
      const entries = await fs.promises.readdir(fullPath, { withFileTypes: true });
      
      const contents = entries.map(entry => {
        const entryPath = path.join(fullPath, entry.name);
        const entryType = entry.isDirectory() ? 'directory' : 'file';
        const relativePath = path.relative(queryClient.dataDir, entryPath);
        
        return {
          name: entry.name,
          type: entryType,
          path: entryPath,
          relativePath,
          url: `/fs/${relativePath}`
        };
      });
      
      return c.json({
        type: 'directory',
        path: fullPath,
        relativePath: path.relative(queryClient.dataDir, fullPath),
        contents
      });
    } else {
      return c.json({
        error: 'Path is neither a file nor a directory',
        path: fullPath
      }, 400);
    }
  } catch (error) {
    console.error('Filesystem endpoint error:', error);
    return c.json({ error: error.message }, 500);
  }
});

// Start server
console.log(`Initializing QueryClient with data directory: ${DATA_DIR}`);
queryClient.initialize().then(() => {
  console.log(`GigAPI server starting on port ${PORT}`);
  
  // Register shutdown handler
  process.on('SIGINT', async () => {
    console.log('Shutting down GigAPI server...');
    await queryClient.close();
    process.exit(0);
  });
  
  // Start the server
  Bun.serve({
    port: PORT,
    fetch: app.fetch
  });
  
  console.log(`GigAPI server running at http://localhost:${PORT}`);
}).catch((error) => {
  console.error('Failed to initialize QueryClient:', error);
  process.exit(1);
});


export { processBigIntInResults, processBigIntInObject };
