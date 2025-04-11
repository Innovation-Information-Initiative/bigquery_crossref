const fs = require('fs');
const readline = require('readline');
const zlib = require('zlib');
const os = require('os');
const { pipeline } = require('stream');

// Memory monitoring
function getMemoryUsage() {
    const used = process.memoryUsage();
    return {
        heapUsed: Math.round(used.heapUsed / 1024 / 1024),
        heapTotal: Math.round(used.heapTotal / 1024 / 1024),
        rss: Math.round(used.rss / 1024 / 1024)
    };
}

/**
 * Removes or converts NULL values to be BigQuery compatible
 * Additional handling for special fields that cannot be NULL
 * @param {Object} obj - The object to process
 * @param {string} [parentKey=null] - The parent key (for nested objects)
 */
function cleanNullValues(obj, parentKey = null) {
    // Handle arrays
    if (Array.isArray(obj)) {
        // Filter out null values from arrays
        return obj
            .filter(item => item !== null)
            .map(item => cleanNullValues(item));
    } 
    // Handle objects
    else if (obj !== null && typeof obj === 'object') {
        const result = {};
        for (const key in obj) {
            const value = obj[key];
            
            // Special handling for null values
            if (value === null) {
                // Special case for fields that cannot be NULL
                if (key.includes('colidentifier') || 
                    (typeof key === 'string' && key.startsWith('*') && key.endsWith('*'))) {
                    // Replace NULL with empty string for these fields
                    result[key] = "";
                } else {
                    // Skip other null values entirely to remain compatible with original behavior
                    continue;
                }
            } else {
                // Process non-null values recursively
                result[key] = cleanNullValues(value, key);
            }
        }
        return result;
    }
    return obj;
}

/**
 * Fixes problematic fields for BigQuery ingest
 * @param {Object} obj - The object to process
 */
function fixBigQueryIssues(obj) {
    if (Array.isArray(obj)) {
        obj.forEach(fixBigQueryIssues);
    } else if (obj !== null && typeof obj === 'object') {
        // Handle year field - ensure it's a valid integer or convert to string
        if (obj.year !== undefined) {
            const yearValue = obj.year;
            // If year contains non-numeric characters (like hyphens in ranges)
            if (typeof yearValue === 'string' && !(/^\d+$/.test(yearValue))) {
                // Convert to year_string field and remove the original year
                obj.year_string = yearValue;
                delete obj.year;
            } else if (typeof yearValue === 'string') {
                // Try to convert string year to integer if it's numeric
                try {
                    const yearInt = parseInt(yearValue, 10);
                    if (!isNaN(yearInt)) {
                        obj.year = yearInt;
                    } else {
                        obj.year_string = yearValue;
                        delete obj.year;
                    }
                } catch {
                    // If conversion fails, move to year_string
                    obj.year_string = yearValue;
                    delete obj.year;
                }
            }
        }

        // Process nested objects
        for (const key in obj) {
            const value = obj[key];
            if (value !== null && typeof value === 'object') {
                fixBigQueryIssues(value);
            }
        }
    }
}

/**
 * Flattens all nested arrays to make them BigQuery compatible
 * @param {Object} obj - The object to process
 * @param {string} [parentKey=null] - The parent key (for tracking context)
 */
function flattenNestedArrays(obj, parentKey = null) {
    if (Array.isArray(obj)) {
        // Check if this array contains other arrays (nested array)
        const containsArrays = obj.some(item => Array.isArray(item));
        
        if (containsArrays) {
            // For date-parts specifically, we flatten differently
            if (parentKey === 'date-parts') {
                // Extract the first entry if it's an array (CrossRef format)
                if (obj.length === 1 && Array.isArray(obj[0])) {
                    return obj[0];
                }
                // If it's already flattened, just return it
                return obj;
            }
            
            // For other nested arrays, we concatenate all sub-arrays
            // This is a simplistic approach - adjust based on your data needs
            const flattened = [];
            for (const item of obj) {
                if (Array.isArray(item)) {
                    // Flatten each nested array and add its items
                    for (const subItem of item) {
                        flattened.push(subItem);
                    }
                } else {
                    // Add non-array items directly
                    flattened.push(item);
                }
            }
            return flattened.map(item => flattenNestedArrays(item));
        }
        
        // Regular array - process each item
        return obj.map(item => flattenNestedArrays(item));
    } 
    else if (obj !== null && typeof obj === 'object') {
        const result = {};
        
        for (const key in obj) {
            const value = obj[key];
            // Recursively process all values
            result[key] = flattenNestedArrays(value, key);
        }
        
        return result;
    }
    
    // Return primitive values as is
    return obj;
}

/**
 * Converts date-parts array to ISO date string
 * @param {Array} dateParts - The date-parts array (usually [year, month, day])
 * @returns {string|null} The ISO date string or null if invalid
 */
function convertDatePartsToISOString(dateParts) {
    if (!Array.isArray(dateParts) || dateParts.length === 0) {
        return null;
    }
    
    // Handle nested arrays - extract the first date array
    let parts = dateParts;
    if (Array.isArray(dateParts[0]) && dateParts.length === 1) {
        parts = dateParts[0];
    }
    
    // Extract year, month, day from date parts
    const year = parts[0];
    const month = parts.length > 1 ? parts[1] : 1; // Default to January if no month
    const day = parts.length > 2 ? parts[2] : 1;   // Default to 1st if no day
    
    // Validate values - note that some values may be strings
    const yearNum = parseInt(year, 10);
    const monthNum = parseInt(month, 10);
    const dayNum = parseInt(day, 10);
    
    if (isNaN(yearNum)) {
        return null;
    }
    
    // Format with padding
    const formattedYear = String(yearNum).padStart(4, '0');
    const formattedMonth = String(isNaN(monthNum) ? 1 : monthNum).padStart(2, '0');
    const formattedDay = String(isNaN(dayNum) ? 1 : dayNum).padStart(2, '0');
    
    // Create ISO format (YYYY-MM-DD)
    return `${formattedYear}-${formattedMonth}-${formattedDay}`;
}

/**
 * Replace dashes with underscores in all key names
 * @param {Object} obj - The object to process
 */
function replaceKeyDashes(obj) {
    if (Array.isArray(obj)) {
        return obj.map(item => replaceKeyDashes(item));
    } else if (obj !== null && typeof obj === 'object') {
        const result = {};
        
        for (const key in obj) {
            const newKey = key.replace(/-/g, '_');
            result[newKey] = replaceKeyDashes(obj[key]);
        }
        
        return result;
    }
    
    return obj;
}

/**
 * Processes date fields in the object, converting from arrays to ISO strings
 * Optimized to only keep the formatted date fields
 * @param {Object} obj - The object to process
 */
function processDateFields(obj) {
    if (Array.isArray(obj)) {
        obj.forEach(processDateFields);
    } else if (obj !== null && typeof obj === 'object') {
        // Look for any date-parts fields and convert them
        if (obj['date-parts'] !== undefined) {
            let dateParts = obj['date-parts'];
            
            // If date-parts is a nested array, flatten it
            if (Array.isArray(dateParts) && dateParts.length === 1 && Array.isArray(dateParts[0])) {
                dateParts = dateParts[0];
            }
            
            // Try to convert to ISO date string
            const isoDate = convertDatePartsToISOString(dateParts);
            if (isoDate) {
                obj['date'] = isoDate;
            }
            
            // Remove the original date-parts field
            delete obj['date-parts'];
        }
        
        // Handle specific date fields in CrossRef schema
        const dateFields = ['published', 'created', 'deposited', 'indexed', 'issued', 
                           'published-online', 'published-print'];
        for (const field of dateFields) {
            if (obj[field] && typeof obj[field] === 'object') {
                // For fields with direct date-parts
                if (obj[field]['date-parts']) {
                    // Normalize date-parts format (handle nested arrays)
                    let dateParts = obj[field]['date-parts'];
                    if (Array.isArray(dateParts) && dateParts.length === 1 && Array.isArray(dateParts[0])) {
                        dateParts = dateParts[0];
                    }
                    
                    // Add an ISO date string and replace the nested object
                    const isoDate = convertDatePartsToISOString(dateParts);
                    if (isoDate) {
                        // Replace the entire object with just the ISO date string
                        obj[field] = isoDate;
                    } else {
                        // If we couldn't generate a date, just delete the field
                        delete obj[field];
                    }
                }
            }
        }
        
        // Special handling for license start dates
        if (obj.license && Array.isArray(obj.license)) {
            obj.license.forEach(license => {
                if (license && license.start && license.start['date-parts']) {
                    // Normalize date format
                    let dateParts = license.start['date-parts'];
                    if (Array.isArray(dateParts) && dateParts.length === 1 && Array.isArray(dateParts[0])) {
                        dateParts = dateParts[0];
                    }
                    
                    // Replace the nested object with just the ISO date
                    const isoDate = convertDatePartsToISOString(dateParts);
                    if (isoDate) {
                        license.start = isoDate;
                    } else {
                        delete license.start;
                    }
                }
            });
        }
        
        // Process nested objects
        for (const key in obj) {
            const value = obj[key];
            if (value !== null && typeof value === 'object') {
                processDateFields(value);
            }
        }
    }
}

/**
 * Process a JSONL file line by line and write to output file
 * @param {string} inputPath - Path to the input JSONL file (can be .gz)
 * @param {string} outputPath - Path to the output JSONL file (will be .gz)
 * @param {Object} options - Additional options
 * @param {boolean} [options.debugMode=false] - Whether to log problematic JSON
 * @param {boolean} [options.quiet=false] - Suppress all console output
 * @returns {Promise<{processedCount: number, errorCount: number}>}
 */
async function processJsonlFile(inputPath, outputPath, options = {}) {
    const { debugMode = false, quiet = false } = options;
    
    return new Promise((resolve, reject) => {
        let processedCount = 0;
        let errorCount = 0;
        let isFinished = false;
        let transformStream = null;

        // Logging function that respects quiet mode
        const log = (message) => {
            if (!quiet) {
                console.log(message);
            }
        };

        const logError = (message) => {
            if (!quiet) {
                console.error(message);
            }
        };

        // Create input stream
        const inputStream = inputPath.endsWith('.gz') 
            ? fs.createReadStream(inputPath).pipe(zlib.createGunzip())
            : fs.createReadStream(inputPath);

        // Create output compression stream
        const gzipStream = zlib.createGzip();
        const outputStream = fs.createWriteStream(outputPath);
        
        // Debug log file - conditionally created
        let debugLogStream = null;
        if (debugMode) {
            const debugPath = `${outputPath}.debug.log`;
            debugLogStream = fs.createWriteStream(debugPath);
            debugLogStream.write(`Debug log for ${inputPath}\n`);
        }

        // Create a transform stream to process each line
        const { Transform } = require('stream');
        transformStream = new Transform({
            objectMode: true,
            transform(line, encoding, callback) {
                try {
                    const lineStr = line.toString().trim();
                    if (!lineStr) {
                        // Skip empty lines
                        return callback(null, '');
                    }
                    
                    const obj = JSON.parse(lineStr);
                    
                    // Apply all transformations in the correct order
                    const flattenedObj = flattenNestedArrays(obj);
                    processDateFields(flattenedObj);
                    fixBigQueryIssues(flattenedObj);
                    const cleanedObj = cleanNullValues(flattenedObj);
                    
                    // Replace dashes with underscores in all keys
                    const standardizedObj = replaceKeyDashes(cleanedObj);
                    
                    // Final validation - check for any remaining nested arrays
                    const jsonStr = JSON.stringify(standardizedObj);
                    if (jsonStr.includes('[[')) {
                        if (debugMode && debugLogStream) {
                            debugLogStream.write(`WARNING: Line ${processedCount} may still contain nested arrays\n`);
                            debugLogStream.write(`JSON: ${jsonStr.substring(0, 500)}...\n\n`);
                        }
                        
                        // Last-ditch effort to fix any remaining nested arrays
                        const lastFixObj = JSON.parse(jsonStr);
                        const lastFixedObj = JSON.parse(JSON.stringify(lastFixObj, (key, value) => {
                            // If this is an array that contains an array, flatten it
                            if (Array.isArray(value) && value.some(item => Array.isArray(item))) {
                                return [].concat(...value);
                            }
                            return value;
                        }));
                        
                        // Final result with newline
                        const result = JSON.stringify(lastFixedObj) + '\n';
                        
                        // Additional debug info
                        if (debugMode && debugLogStream) {
                            debugLogStream.write(`After final fix: ${result.substring(0, 500)}...\n\n`);
                        }
                        
                        processedCount++;
                        if (processedCount % 1000 === 0) {
                            const mem = getMemoryUsage();
                            log(`Processed ${processedCount} lines. Memory: ${mem.heapUsed}MB used, ${mem.heapTotal}MB total`);
                        }
                        
                        callback(null, result);
                    } else {
                        // Normal result with newline
                        const result = jsonStr + '\n';
                        
                        // Debug logging for special cases
                        if (debugMode) {
                            // Check for year fields that might cause issues
                            if (lineStr.includes('"year":')) {
                                const originalYearMatch = /"year"\s*:\s*("[^"]*"|[^,\}]*)/g;
                                const matches = [...lineStr.matchAll(originalYearMatch)];
                                
                                for (const match of matches) {
                                    const yearValue = match[1];
                                    // Check if this is a problematic year value (non-numeric)
                                    if (yearValue.includes('-') || yearValue.includes('/') || 
                                        (yearValue.startsWith('"') && !(/^\"\d+\"$/.test(yearValue)))) {
                                        debugLogStream.write(`Line ${processedCount}: Found problematic year value: ${yearValue}\n`);
                                        debugLogStream.write(`Original: ${lineStr.substring(0, 200)}...\n`);
                                        debugLogStream.write(`Processed: ${result.substring(0, 200)}...\n\n`);
                                    }
                                }
                            }
                        }
                        
                        processedCount++;
                        if (processedCount % 1000 === 0) {
                            const mem = getMemoryUsage();
                            log(`Processed ${processedCount} lines. Memory: ${mem.heapUsed}MB used, ${mem.heapTotal}MB total`);
                        }
                        
                        callback(null, result);
                    }
                } catch (err) {
                    errorCount++;
                    logError(`Error processing JSON at line ${processedCount + 1}: ${err.message}`);
                    
                    if (debugMode && debugLogStream) {
                        debugLogStream.write(`ERROR at line ${processedCount + 1}: ${err.message}\n`);
                        debugLogStream.write(`Line content: ${line.toString()}\n\n`);
                    }
                    
                    // Continue processing even if one line fails
                    callback(null, '');
                }
            }
        });

        // Set up readline interface
        const rl = readline.createInterface({
            input: inputStream,
            crlfDelay: Infinity
        });

        // Handle cleanup properly
        function cleanup() {
            if (!isFinished) {
                isFinished = true;
                rl.close();
                if (transformStream) {
                    transformStream.end();
                }
                if (debugLogStream) {
                    debugLogStream.end();
                }
            }
        }

        // Set up the pipeline
        rl.on('line', (line) => {
            transformStream.write(line);
        });

        rl.on('close', () => {
            if (transformStream) {
                transformStream.end();
            }
        });

        // Use proper pipeline for better error handling
        pipeline(
            transformStream,
            gzipStream,
            outputStream,
            (err) => {
                if (err) {
                    logError('Pipeline error:', err.message);
                    cleanup();
                    // Make sure the output file is removed if there was an error
                    try {
                        fs.unlinkSync(outputPath);
                    } catch (unlinkErr) {
                        // Ignore errors when trying to delete the file
                    }
                    reject(err);
                } else {
                    log(`Finished processing ${processedCount} lines with ${errorCount} errors`);
                    resolve({ processedCount, errorCount });
                }
            }
        );

        // Error handling for input stream and readline
        inputStream.on('error', (err) => {
            logError('Input stream error:', err.message);
            cleanup();
            reject(err);
        });

        rl.on('error', (err) => {
            logError('Readline error:', err.message);
            cleanup();
            reject(err);
        });
    });
}

module.exports = {
    processJsonlFile
};