/**
 * Parse InfluxDB line protocol data
 * 
 * Format: 
 * measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 [timestamp]
 * 
 * @param {string} data - Line protocol data
 * @returns {Array} - Array of parsed data points
 */
export function parse(data) {
  const lines = data.trim().split('\n');
  const points = [];

  for (const line of lines) {
    // Skip empty lines and comments
    if (!line.trim() || line.trim().startsWith('#')) {
      continue;
    }

    try {
      const point = parseLine(line);
      points.push(point);
    } catch (error) {
      console.error(`Error parsing line: ${line}`, error);
    }
  }

  return points;
}

/**
 * Parse a single line of InfluxDB line protocol
 * @param {string} line - Line to parse
 * @returns {Object} - Parsed data point
 */
function parseLine(line) {
  // Split the line into its components
  const parts = line.trim().split(' ');
  
  // There must be at least 2 parts (measurement+tags and fields)
  if (parts.length < 2) {
    throw new Error(`Invalid line protocol: ${line}`);
  }
  
  // Parse measurement and tags
  const measurementAndTags = parts[0];
  const measurementTagsParts = measurementAndTags.split(',');
  const measurement = measurementTagsParts[0];
  
  // Parse tags
  const tags = {};
  for (let i = 1; i < measurementTagsParts.length; i++) {
    const tagPair = measurementTagsParts[i].split('=');
    if (tagPair.length === 2) {
      tags[tagPair[0]] = tagPair[1];
    }
  }
  
  // Parse fields
  const fieldsPart = parts[1];
  const fieldPairs = fieldsPart.split(',');
  const fields = {};
  
  for (const fieldPair of fieldPairs) {
    const [key, rawValue] = fieldPair.split('=');
    
    // Parse field value based on type (integer, float, string, boolean)
    let value;
    
    if (rawValue.endsWith('i')) {
      // Integer
      value = parseInt(rawValue.slice(0, -1));
    } else if (rawValue === 't' || rawValue === 'T' || rawValue === 'true' || rawValue === 'True') {
      // Boolean true
      value = true;
    } else if (rawValue === 'f' || rawValue === 'F' || rawValue === 'false' || rawValue === 'False') {
      // Boolean false
      value = false;
    } else if (rawValue.startsWith('"') && rawValue.endsWith('"')) {
      // String
      value = rawValue.slice(1, -1);
    } else {
      // Float
      value = parseFloat(rawValue);
    }
    
    fields[key] = value;
  }
  
  // Parse timestamp if present
  let timestamp = null;
  if (parts.length > 2) {
    timestamp = BigInt(parts[2]);
  } else {
    // If no timestamp provided, use current time in nanoseconds
    timestamp = BigInt(Date.now()) * 1000000n; // Convert milliseconds to nanoseconds
  }
  
  return {
    measurement,
    tags,
    fields,
    timestamp
  };
}

/**
 * Format data as InfluxDB line protocol
 * @param {string} measurement - Measurement name
 * @param {Object} tags - Tags key-value pairs
 * @param {Object} fields - Fields key-value pairs
 * @param {BigInt|number} [timestamp] - Optional timestamp in nanoseconds
 * @returns {string} - Line protocol formatted string
 */
export function format(measurement, tags, fields, timestamp) {
  // Format measurement and tags
  let line = measurement;
  
  if (tags && Object.keys(tags).length > 0) {
    const tagStrings = [];
    for (const [key, value] of Object.entries(tags)) {
      tagStrings.push(`${key}=${value}`);
    }
    line += ',' + tagStrings.join(',');
  }
  
  // Format fields
  const fieldStrings = [];
  for (const [key, value] of Object.entries(fields)) {
    if (typeof value === 'string') {
      fieldStrings.push(`${key}="${value}"`);
    } else if (typeof value === 'boolean') {
      fieldStrings.push(`${key}=${value ? 't' : 'f'}`);
    } else if (Number.isInteger(value)) {
      fieldStrings.push(`${key}=${value}i`);
    } else {
      fieldStrings.push(`${key}=${value}`);
    }
  }
  
  line += ' ' + fieldStrings.join(',');
  
  // Add timestamp if provided
  if (timestamp !== undefined) {
    line += ` ${timestamp}`;
  }
  
  return line;
}

export default {
  parse,
  format
};
