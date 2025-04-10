import { open } from '@evan/duckdb';
import path from 'path';
import fs from 'fs';
import { parse as parseLineProtocol } from './lineProtocol.js';

class QueryClient {
  constructor(dataDir = './data') {
    this.dataDir = dataDir;
    this.db = null;
    this.connection = null;
    this.defaultTimeRange = 10 * 60 * 1000000000; // 10 minutes in nanoseconds
  }

  async initialize() {
    try {
      // Initialize DuckDB client
      this.db = open(':memory:');
      this.connection = this.db.connect();
      console.log('Initialized DuckDB for querying');
    } catch (error) {
      console.error('Failed to initialize DuckDB:', error);
      throw error;
    }
  }

  /**
   * Parse SQL query to extract important parts
   * @param {string} sql - The SQL query to parse
   * @param {string} dbName - The database name
   * @returns {Object} - Parsed query components
   */
  parseQuery(sql, dbName) {
    // Extract SELECT columns
    const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/i);
    const columns = selectMatch ? selectMatch[1].trim() : '*';

    // Extract measurement name (table)
    const fromMatch = sql.match(/FROM\s+(?:(\w+)\.)?(\w+)/i);
    if (!fromMatch) {
      throw new Error('Invalid query: FROM clause not found or invalid');
    }
    
    // If db name is in the query, use it, otherwise use the provided dbName
    const queryDbName = fromMatch[1] || dbName;
    const measurement = fromMatch[2];

    // Extract time range
    let timeRange = this._extractTimeRange(sql);

    // Extract additional WHERE conditions excluding time
    const whereConditions = this._extractWhereConditions(sql, timeRange.timeCondition);

    // Extract other clauses
    const orderBy = sql.match(/ORDER\s+BY\s+(.*?)(?:\s+(?:LIMIT|GROUP|HAVING|$))/i)?.[1] || '';
    const groupBy = sql.match(/GROUP\s+BY\s+(.*?)(?:\s+(?:ORDER|LIMIT|HAVING|$))/i)?.[1] || '';
    const having = sql.match(/HAVING\s+(.*?)(?:\s+(?:ORDER|LIMIT|$))/i)?.[1] || '';
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i);
    const limit = limitMatch ? parseInt(limitMatch[1]) : null;

    return {
      columns,
      dbName: queryDbName,
      measurement,
      timeRange,
      whereConditions,
      orderBy,
      groupBy,
      having,
      limit
    };
  }

  /**
   * Extract time range from SQL query
   * @private
   */
  _extractTimeRange(sql) {
    // Common time patterns in InfluxQL/SQL
    const timePatterns = [
      // time >= '2023-01-01T00:00:00'
      /time\s*(>=|>)\s*'([^']+)'/i,
      // time <= '2023-01-01T00:00:00'
      /time\s*(<=|<)\s*'([^']+)'/i,
      // time = '2023-01-01T00:00:00'
      /time\s*=\s*'([^']+)'/i,
      // time BETWEEN '2023-01-01T00:00:00' AND '2023-01-02T00:00:00'
      /time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'/i
    ];

    let start = null;
    let end = null;
    let timeCondition = null;
    let userSpecifiedTimeCondition = false;

    // Check for BETWEEN pattern first
    const betweenMatch = sql.match(timePatterns[3]);
    if (betweenMatch) {
      start = new Date(betweenMatch[1]).getTime() * 1000000; // Convert to nanoseconds
      end = new Date(betweenMatch[2]).getTime() * 1000000;
      timeCondition = `time BETWEEN '${betweenMatch[1]}' AND '${betweenMatch[2]}'`;
      userSpecifiedTimeCondition = true;
    } else {
      // Check for >= or > pattern
      const startMatch = sql.match(timePatterns[0]);
      if (startMatch) {
        start = new Date(startMatch[2]).getTime() * 1000000;
        timeCondition = `time ${startMatch[1]} '${startMatch[2]}'`;
        userSpecifiedTimeCondition = true;
      }

      // Check for <= or < pattern
      const endMatch = sql.match(timePatterns[1]);
      if (endMatch) {
        end = new Date(endMatch[2]).getTime() * 1000000;
        // If we already have a time condition, we need to combine them
        if (timeCondition) {
          timeCondition = `${timeCondition} AND time ${endMatch[1]} '${endMatch[2]}'`;
        } else {
          timeCondition = `time ${endMatch[1]} '${endMatch[2]}'`;
        }
        userSpecifiedTimeCondition = true;
      }

      // Check for = pattern
      const equalMatch = sql.match(timePatterns[2]);
      if (equalMatch) {
        const exactTime = new Date(equalMatch[1]).getTime() * 1000000;
        start = exactTime;
        end = exactTime;
        timeCondition = `time = '${equalMatch[1]}'`;
        userSpecifiedTimeCondition = true;
      }
    }

    // If no time range is specified, don't apply a default time filter
    if (!userSpecifiedTimeCondition) {
      // Return null for both start and end to indicate all files should be considered
      return { start: null, end: null, timeCondition: null };
    } else if (start && !end) {
      // If only start is specified, set end to now
      end = Date.now() * 1000000;
    } else if (!start && end) {
      // If only end is specified, set start to earliest possible
      start = 0;
    }

    return { start, end, timeCondition };
  }

  /**
   * Extract WHERE conditions excluding time
   * @private
   */
  _extractWhereConditions(sql, timeCondition) {
    const whereMatch = sql.match(/WHERE\s+(.*?)(?:\s+(?:GROUP|ORDER|LIMIT|$))/i);
    if (!whereMatch) return '';

    // Remove time conditions from WHERE clause
    let conditions = whereMatch[1];
    
    // Remove the time condition if it exists
    if (timeCondition) {
      const timeConditionWithoutQuotes = timeCondition.replace(/'/g, '');
      const timeConditionPattern = new RegExp(timeConditionWithoutQuotes.replace(/([()[{*+.$^\\|?])/g, '\\$1'), 'i');
      conditions = conditions.replace(timeConditionPattern, '');
      
      // Also try to remove other time patterns
      conditions = conditions.replace(/time\s*(>=|>|<=|<|=|BETWEEN)\s*'[^']+'\s*(AND\s*time\s*(>=|>|<=|<|=)\s*'[^']+')?\s*(AND|OR)?/gi, '');
    }
    
    // Clean up leftover AND/OR operators
    conditions = conditions.replace(/^\s*(AND|OR)\s+/i, '').replace(/\s+(AND|OR)\s*$/i, '').trim();
    
    return conditions;
  }

  /**
   * Find relevant parquet files based on measurement and time range
   * @param {string} dbName - Database name
   * @param {string} measurement - Measurement name
   * @param {Object} timeRange - Time range to search within
   * @returns {Array} - Array of file paths
   */
  async findRelevantFiles(dbName, measurement, timeRange) {
    const { start, end } = timeRange;
    
    // If no time range is specified, include all files for the measurement
    if (start === null && end === null) {
      return await this._findAllFiles(dbName, measurement);
    }
    
    // Convert nanosecond timestamps to Date objects for directory parsing
    const startDate = new Date(start / 1000000);
    const endDate = new Date(end / 1000000);
    
    // Find files using date structure if available, otherwise find all files
    let relevantFiles = [];
    
    try {
      // First try using expected Hive structure (date=*/hour=*)
      relevantFiles = await this._findFilesByHiveStructure(dbName, measurement, startDate, endDate, start, end);
      
      // If we didn't find any files, try a more generic approach
      if (relevantFiles.length === 0) {
        console.log(`No files found using Hive structure, trying recursive search...`);
        
        // Find all files and filter by time range
        const allFiles = [];
        const basePath = path.join(this.dataDir, dbName, measurement);
        await this._findMetadataFilesRecursively(basePath, allFiles, start, end);
        
        relevantFiles = allFiles;
      }
    } catch (error) {
      console.error(`Error finding relevant files:`, error);
    }
    
    console.log(`Found ${relevantFiles.length} relevant files for the query`);
    return relevantFiles;
  }
  
  /**
   * Find files using Hive partitioning structure
   * @private
   */
  async _findFilesByHiveStructure(dbName, measurement, startDate, endDate, startNs, endNs) {
    const relevantFiles = [];
    
    // Get all date directories that might contain relevant data
    const dateDirectories = await this._getDateDirectoriesInRange(dbName, measurement, startDate, endDate);
    
    for (const dateDir of dateDirectories) {
      // For each date directory, get all hour directories
      const datePath = path.join(this.dataDir, dbName, measurement, dateDir);
      const hourDirs = await this._getHourDirectoriesInRange(datePath, startDate, endDate);
      
      for (const hourDir of hourDirs) {
        const hourPath = path.join(datePath, hourDir);
        
        try {
          // Read metadata for this hour
          const metadataPath = path.join(hourPath, 'metadata.json');
          if (!fs.existsSync(metadataPath)) continue;
          
          const metadata = JSON.parse(await fs.promises.readFile(metadataPath, 'utf8'));
          
          // Skip if metadata time range doesn't overlap with requested time range
          if (startNs !== null && endNs !== null && (metadata.max_time < startNs || metadata.min_time > endNs)) continue;
          
          // Add relevant files
          for (const file of metadata.files) {
            // Apply time range filtering if specified
            if (startNs === null || endNs === null || (file.max_time >= startNs && file.min_time <= endNs)) {
              // Check if file exists
              if (fs.existsSync(file.path)) {
                relevantFiles.push(file.path);
              } else {
                // If the path in metadata doesn't exist, try to resolve it relative to the current directory
                const localPath = path.join(hourPath, path.basename(file.path));
                if (fs.existsSync(localPath)) {
                  relevantFiles.push(localPath);
                } else {
                  console.log(`File not found at either path: ${file.path} or ${localPath}`);
                }
              }
            }
          }
        } catch (error) {
          if (error.code !== 'ENOENT') {
            console.error(`Error reading metadata for ${hourPath}:`, error);
          }
        }
      }
    }
    
    return relevantFiles;
  }
  
  /**
   * Recursively find metadata files and extract relevant parquet files
   * @private
   */
  async _findMetadataFilesRecursively(dirPath, filesList, startNs, endNs) {
    try {
      const entries = await fs.promises.readdir(dirPath, { withFileTypes: true });
      
      // Look for metadata.json
      const metadataEntry = entries.find(entry => entry.isFile() && entry.name === 'metadata.json');
      
      if (metadataEntry) {
        // Use metadata to find parquet files
        const metadataPath = path.join(dirPath, 'metadata.json');
        try {
          const metadata = JSON.parse(await fs.promises.readFile(metadataPath, 'utf8'));
          
          // Check if metadata time range overlaps with requested time range
          if (startNs !== null && endNs !== null) {
            if (metadata.max_time < startNs || metadata.min_time > endNs) {
              // Skip this directory if time ranges don't overlap
              return;
            }
          }
          
          // Process files in metadata
          if (metadata.files && Array.isArray(metadata.files)) {
            for (const file of metadata.files) {
              // Apply time range filtering if specified
              if (startNs === null || endNs === null || 
                 (file.max_time >= startNs && file.min_time <= endNs)) {
                
                // Try the exact path in metadata
                if (fs.existsSync(file.path)) {
                  filesList.push(file.path);
                } else {
                  // Try local path relative to metadata file
                  const localPath = path.join(dirPath, path.basename(file.path));
                  if (fs.existsSync(localPath)) {
                    filesList.push(localPath);
                  } else {
                    console.log(`File not found at either path: ${file.path} or ${localPath}`);
                  }
                }
              }
            }
          }
        } catch (error) {
          console.error(`Error reading metadata at ${metadataPath}:`, error);
        }
      } else {
        // If no metadata.json, collect all parquet files in this directory
        for (const entry of entries) {
          if (entry.isFile() && entry.name.endsWith('.parquet')) {
            filesList.push(path.join(dirPath, entry.name));
          }
        }
      }
      
      // Recursively check subdirectories
      for (const entry of entries) {
        if (entry.isDirectory()) {
          await this._findMetadataFilesRecursively(path.join(dirPath, entry.name), filesList, startNs, endNs);
        }
      }
    } catch (error) {
      console.error(`Error reading directory ${dirPath}:`, error);
    }
  }
  
  /**
   * Find all files for a measurement regardless of time range
   * @private
   */
  async _findAllFiles(dbName, measurement) {
    const allFiles = [];
    const basePath = path.join(this.dataDir, dbName, measurement);
    
    if (!fs.existsSync(basePath)) {
      console.log(`Measurement path does not exist: ${basePath}`);
      return allFiles;
    }
    
    try {
      console.log(`Searching for files in: ${basePath}`);
      
      // Recursively find all parquet files and metadata.json files
      await this._findFilesRecursively(basePath, allFiles);
      
      console.log(`Found ${allFiles.length} total files for ${dbName}.${measurement}`);
      return allFiles;
    } catch (error) {
      console.error(`Error finding all files for ${measurement}:`, error);
      return [];
    }
  }
  
  /**
   * Recursively find all parquet files and read metadata.json files
   * @private
   */
  async _findFilesRecursively(dirPath, filesList) {
    try {
      const entries = await fs.promises.readdir(dirPath, { withFileTypes: true });
      
      // First look for metadata.json
      const metadataEntry = entries.find(entry => entry.isFile() && entry.name === 'metadata.json');
      
      if (metadataEntry) {
        console.log(`Found metadata.json in ${dirPath}`);
        // Use metadata to find parquet files
        const metadataPath = path.join(dirPath, 'metadata.json');
        try {
          const metadata = JSON.parse(await fs.promises.readFile(metadataPath, 'utf8'));
          
          if (metadata.files && Array.isArray(metadata.files)) {
            for (const file of metadata.files) {
              if (fs.existsSync(file.path)) {
                console.log(`Adding file from metadata: ${file.path}`);
                filesList.push(file.path);
              } else {
                console.log(`File from metadata doesn't exist: ${file.path}`);
              }
            }
          }
        } catch (error) {
          console.error(`Error reading metadata at ${metadataPath}:`, error);
        }
      }
      
      // Then look for parquet files directly in this directory
      for (const entry of entries) {
        if (entry.isFile() && entry.name.endsWith('.parquet')) {
          const filePath = path.join(dirPath, entry.name);
          console.log(`Adding parquet file: ${filePath}`);
          filesList.push(filePath);
        }
      }
      
      // Recursively check subdirectories
      for (const entry of entries) {
        if (entry.isDirectory()) {
          const subdirPath = path.join(dirPath, entry.name);
          console.log(`Checking subdirectory: ${subdirPath}`);
          await this._findFilesRecursively(subdirPath, filesList);
        }
      }
    } catch (error) {
      console.error(`Error reading directory ${dirPath}:`, error);
    }
  }
  
  /**
   * Get all date directories that might contain data in the given time range
   * @private
   */
  async _getDateDirectoriesInRange(dbName, measurement, startDate, endDate) {
    const basePath = path.join(this.dataDir, dbName, measurement);
    if (!fs.existsSync(basePath)) return [];
    
    const allDirs = await fs.promises.readdir(basePath);
    const dateDirs = allDirs.filter(dir => dir.startsWith('date='));
    
    // Extract dates and filter by range
    return dateDirs.filter(dateDir => {
      const dateStr = dateDir.replace('date=', '');
      const dirDate = new Date(dateStr);
      
      // Check if directory date is within range
      // Use date comparison without time
      const dirDateOnly = new Date(dirDate.getFullYear(), dirDate.getMonth(), dirDate.getDate());
      const startDateOnly = new Date(startDate.getFullYear(), startDate.getMonth(), startDate.getDate());
      const endDateOnly = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate());
      
      return dirDateOnly >= startDateOnly && dirDateOnly <= endDateOnly;
    });
  }
  
  /**
   * Get all hour directories that might contain data in the given time range
   * @private
   */
  async _getHourDirectoriesInRange(datePath, startDate, endDate) {
    if (!fs.existsSync(datePath)) return [];
    
    const allDirs = await fs.promises.readdir(datePath);
    const hourDirs = allDirs.filter(dir => dir.startsWith('hour='));
    
    // If date is the same, filter by hour
    const dateStr = path.basename(datePath).replace('date=', '');
    const dirDate = new Date(dateStr);
    
    if (startDate.getFullYear() === dirDate.getFullYear() && 
        startDate.getMonth() === dirDate.getMonth() && 
        startDate.getDate() === dirDate.getDate() &&
        endDate.getFullYear() === dirDate.getFullYear() && 
        endDate.getMonth() === dirDate.getMonth() && 
        endDate.getDate() === dirDate.getDate()) {
      
      // Same day, filter by hour
      return hourDirs.filter(hourDir => {
        const hour = parseInt(hourDir.replace('hour=', ''));
        return hour >= startDate.getHours() && hour <= endDate.getHours();
      });
    }
    
    // If start date matches dir date, filter hours >= start hour
    if (startDate.getFullYear() === dirDate.getFullYear() && 
        startDate.getMonth() === dirDate.getMonth() && 
        startDate.getDate() === dirDate.getDate()) {
      
      return hourDirs.filter(hourDir => {
        const hour = parseInt(hourDir.replace('hour=', ''));
        return hour >= startDate.getHours();
      });
    }
    
    // If end date matches dir date, filter hours <= end hour
    if (endDate.getFullYear() === dirDate.getFullYear() && 
        endDate.getMonth() === dirDate.getMonth() && 
        endDate.getDate() === dirDate.getDate()) {
      
      return hourDirs.filter(hourDir => {
        const hour = parseInt(hourDir.replace('hour=', ''));
        return hour <= endDate.getHours();
      });
    }
    
    // Otherwise, include all hours
    return hourDirs;
  }

  /**
   * Execute a SQL query
   * @param {string} sql - SQL query to execute
   * @param {string} dbName - Database name
   * @returns {Array} - Query results
   */
  async query(sql, dbName = 'mydb') {
    if (!this.connection) {
      throw new Error('QueryClient not initialized');
    }

    try {
      // Parse the query
      const parsed = this.parseQuery(sql, dbName);
      console.log('Parsed query:', JSON.stringify(parsed, null, 2));
      
      // Find relevant files
      const files = await this.findRelevantFiles(
        parsed.dbName, 
        parsed.measurement, 
        parsed.timeRange
      );
      
      if (!files.length) {
        console.log('No relevant files found for the query');
        return [];
      }

      console.log(`Found ${files.length} relevant files`);
      
      // Construct the DuckDB query
      let duckdbQuery = `
        SELECT ${parsed.columns}
        FROM read_parquet([${files.map(f => `'${f}'`).join(', ')}], union_by_name = true)
      `;
      
      // Add WHERE conditions
      const whereConditions = [];
      if (parsed.timeRange.timeCondition) {
        whereConditions.push(parsed.timeRange.timeCondition);
      }
      if (parsed.whereConditions) {
        whereConditions.push(parsed.whereConditions);
      }
      
      if (whereConditions.length > 0 && whereConditions.some(cond => cond && cond.trim() !== '')) {
        duckdbQuery += ` WHERE ${whereConditions.filter(cond => cond && cond.trim() !== '').join(' AND ')}`;
      }
      
      // Add GROUP BY, HAVING, ORDER BY, and LIMIT
      if (parsed.groupBy && parsed.groupBy.trim() !== '') {
        duckdbQuery += ` GROUP BY ${parsed.groupBy}`;
      }
      
      if (parsed.having && parsed.having.trim() !== '') {
        duckdbQuery += ` HAVING ${parsed.having}`;
      }
      
      if (parsed.orderBy && parsed.orderBy.trim() !== '') {
        duckdbQuery += ` ORDER BY ${parsed.orderBy}`;
      }
      
      if (parsed.limit !== null) {
        duckdbQuery += ` LIMIT ${parsed.limit}`;
      }
      
      console.log('Executing DuckDB query:', duckdbQuery);
      
      // Execute the query
      const result = this.connection.query(duckdbQuery);
      
      // Post-process the results to format timestamps properly if needed
      return this._postProcessResults(result);
    } catch (error) {
      console.error('Query error:', error);
      throw error;
    }
  }
  
  /**
   * Post-process query results to handle timestamps and other transformations
   * @private
   */
  _postProcessResults(results) {
    if (!results || !Array.isArray(results) || results.length === 0) {
      return results;
    }
    
    // Check if there's a timestamp/time column that needs processing
    return results.map(row => {
      const newRow = { ...row };
      
      // If there's a 'time' column that looks like a timestamp, format it properly
      if (newRow.timestamp && typeof newRow.timestamp === 'object' && newRow.timestamp instanceof Date) {
        // Keep the Date object but add an ISO string representation
        newRow.timestamp_iso = newRow.timestamp.toISOString();
      }
      
      if (newRow.time && typeof newRow.time === 'bigint') {
        // If time is stored as nanoseconds, convert to a readable format
        try {
          // Assuming time is in nanoseconds since Unix epoch
          const timeMs = Number(newRow.time / 1000000n); // Convert to milliseconds
          newRow.time_iso = new Date(timeMs).toISOString();
        } catch (e) {
          console.error('Error converting time value:', e);
        }
      }
      
      return newRow;
    });
  }

  /**
   * Insert data in InfluxDB line protocol format
   * @param {string} dbName - Database name
   * @param {string} lineProtocolData - Data in InfluxDB line protocol format
   */
  async insert(dbName, lineProtocolData) {
    try {
      // Parse line protocol data
      const measurements = parseLineProtocol(lineProtocolData);
      
      // Group by measurement
      const measurementMap = new Map();
      for (const point of measurements) {
        if (!measurementMap.has(point.measurement)) {
          measurementMap.set(point.measurement, []);
        }
        measurementMap.get(point.measurement).push(point);
      }
      
      // Process each measurement
      for (const [measurement, points] of measurementMap.entries()) {
        // Determine the time range and create appropriate directory structure
        const now = new Date();
        const dateStr = now.toISOString().split('T')[0]; // YYYY-MM-DD
        const hour = now.getHours();
        
        const measurementDir = path.join(this.dataDir, dbName, measurement);
        const dateDir = path.join(measurementDir, `date=${dateStr}`);
        const hourDir = path.join(dateDir, `hour=${hour}`);
        
        // Ensure directories exist
        await fs.promises.mkdir(hourDir, { recursive: true });
        
        // TODO: Here you would typically write to parquet files and update metadata
        // For this example, we'll just log what would happen
        console.log(`Would insert ${points.length} points into ${hourDir}`);
        console.log('Points sample:', points.slice(0, 2));
      }
      
      return { success: true, message: `Inserted ${measurements.length} points` };
    } catch (error) {
      console.error('Insert error:', error);
      throw error;
    }
  }

  async close() {
    if (this.connection) {
      this.connection.close();
      this.connection = null;
    }
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}

export default QueryClient;
