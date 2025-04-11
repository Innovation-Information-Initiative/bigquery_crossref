const fs = require('fs');
const path = require('path');
const { processJsonlFile } = require('./processor');
const os = require('os');
const zlib = require('zlib');

// Configuration
const INPUT_DIR = path.join(__dirname, '..', 'data', 'raw');
const OUTPUT_DIR = path.join(__dirname, '..', 'data', 'processed');
const LOG_DIR = path.join(__dirname, '..', 'logs');
const CONCURRENT_JOBS = Math.max(1, os.cpus().length - 1); // Use N-1 cores
const DEBUG_MODE = process.env.DEBUG === 'true';
const RESUME_MODE = process.env.RESUME !== 'false'; // Default to true
const SPECIFIC_FILE = process.env.FILE || null; // Process a specific file if specified
const QUIET_MODE = process.env.QUIET === 'true'; // Set to true to suppress progress display
const SHOW_PROGRESS = !QUIET_MODE && process.stdout.isTTY; // Only show progress in interactive terminal

// Ensure directories exist
[OUTPUT_DIR, LOG_DIR].forEach(dir => {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
});

// Setup logging
const logFile = path.join(LOG_DIR, `process_${new Date().toISOString().replace(/[:.]/g, '-')}.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

function log(message, level = 'INFO', consoleOnly = false) {
    const timestamp = new Date().toISOString();
    const formattedMessage = `[${timestamp}] [${level}] ${message}`;
    
    // Always write to log file
    if (!consoleOnly) {
        logStream.write(formattedMessage + '\n');
    }
    
    // Only print to console when needed
    if (!SHOW_PROGRESS || level === 'ERROR' || consoleOnly) {
        if (level === 'ERROR') {
            console.error(formattedMessage);
        } else if (!SHOW_PROGRESS) {
            console.log(formattedMessage);
        }
    }
}

function logError(message, error) {
    log(`${message}: ${error.message}`, 'ERROR');
    if (error.stack) {
        logStream.write(`${error.stack}\n`);
    }
}

// Progress bar generator
class ProgressBar {
    constructor(total, width = 40) {
        this.total = total;
        this.width = width;
        this.current = 0;
        this.startTime = Date.now();
        this.stats = {
            successful: 0,
            failed: 0,
            totalRecords: 0,
            totalErrors: 0
        };
        this.lastUpdate = 0;
        this.refreshRate = 100; // ms
    }

    update(current, stats = {}) {
        this.current = current;
        
        // Update stats if provided
        if (stats.successful !== undefined) this.stats.successful = stats.successful;
        if (stats.failed !== undefined) this.stats.failed = stats.failed;
        if (stats.records !== undefined) this.stats.totalRecords += stats.records;
        if (stats.errors !== undefined) this.stats.totalErrors += stats.errors;
        
        // Only update the display at most every refreshRate ms
        const now = Date.now();
        if (now - this.lastUpdate < this.refreshRate && current < this.total) {
            return;
        }
        this.lastUpdate = now;
        
        // Don't display progress if not in interactive mode
        if (!SHOW_PROGRESS) return;
        
        // Calculate progress
        const percent = Math.min(100, Math.round((current / this.total) * 100));
        const elapsed = (now - this.startTime) / 1000;
        const eta = current === 0 ? 0 : (elapsed / current) * (this.total - current);
        
        // Create the progress bar
        const completeWidth = Math.round((percent / 100) * this.width);
        const incomplete = this.width - completeWidth;
        const bar = '█'.repeat(completeWidth) + '░'.repeat(incomplete);
        
        // Create the stats display
        const statsStr = `Files: ${this.stats.successful}✓ ${this.stats.failed}✗ | Records: ${this.stats.totalRecords.toLocaleString()} | Errors: ${this.stats.totalErrors}`;
        
        // Format time
        const formatTime = (seconds) => {
            if (seconds === Infinity || isNaN(seconds)) return '--:--';
            const mins = Math.floor(seconds / 60);
            const secs = Math.floor(seconds % 60);
            return `${mins}:${secs.toString().padStart(2, '0')}`;
        };
        
        // Assemble the progress line
        const progressLine = `[${bar}] ${percent}% | ${current}/${this.total} | ${formatTime(elapsed)} elapsed | ETA: ${formatTime(eta)}`;
        const statsLine = `${statsStr}`;
        
        // Clear the line and write the new progress
        process.stdout.write('\r\x1b[K'); // Clear current line
        process.stdout.write(progressLine);
        process.stdout.write('\n\r\x1b[K'); // Move to next line and clear it
        process.stdout.write(statsLine);
        process.stdout.write('\x1b[1A'); // Move cursor back up
    }
    
    finish() {
        if (!SHOW_PROGRESS) return;
        
        // Ensure we show 100% at the end
        this.update(this.total);
        
        // Add a newline after the progress bar
        process.stdout.write('\n\n');
        
        // Calculate final stats
        const elapsed = (Date.now() - this.startTime) / 1000;
        const filesPerSecond = this.total / elapsed;
        const recordsPerSecond = this.stats.totalRecords / elapsed;
        
        // Format the elapsed time in a human-readable format
        let elapsedStr;
        if (elapsed < 60) {
            elapsedStr = `${elapsed.toFixed(1)} seconds`;
        } else if (elapsed < 3600) {
            elapsedStr = `${(elapsed / 60).toFixed(1)} minutes`;
        } else {
            elapsedStr = `${(elapsed / 3600).toFixed(1)} hours`;
        }
        
        // Generate summary
        const summary = [
            '📊 Processing Summary 📊',
            '------------------------',
            `🕒 Total time: ${elapsedStr}`,
            `📁 Files processed: ${this.stats.successful} successful, ${this.stats.failed} failed`,
            `📝 Records processed: ${this.stats.totalRecords.toLocaleString()}`,
            `⚠️ Errors encountered: ${this.stats.totalErrors}`,
            `⚡ Performance: ${filesPerSecond.toFixed(2)} files/sec, ${Math.round(recordsPerSecond)} records/sec`,
            '------------------------'
        ].join('\n');
        
        // Display the summary
        console.log(summary);
        
        // Also log the summary to the log file
        logStream.write('\n' + summary + '\n');
    }
}

// Function to validate gzip file
function validateGzipFile(filePath) {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath);
        const gunzip = zlib.createGunzip();
        
        let dataReceived = false;
        
        stream.on('error', (err) => {
            reject(new Error(`Failed to read file: ${err.message}`));
        });
        
        gunzip.on('error', (err) => {
            reject(new Error(`Invalid gzip file: ${err.message}`));
        });
        
        stream.pipe(gunzip)
            .on('data', () => {
                dataReceived = true;
                // Close streams after receiving some data to speed up validation
                if (!DEBUG_MODE) {
                    stream.destroy();
                    gunzip.destroy();
                    resolve(true);
                }
            })
            .on('end', () => {
                if (!dataReceived) {
                    reject(new Error('Empty gzip file'));
                } else {
                    resolve(true);
                }
            })
            .on('error', (err) => {
                reject(new Error(`Error during validation: ${err.message}`));
            });
    });
}

// Get list of files to process
function getFilesToProcess() {
    // If a specific file is requested, only process that one
    if (SPECIFIC_FILE) {
        const file = path.basename(SPECIFIC_FILE);
        if (fs.existsSync(path.join(INPUT_DIR, file))) {
            log(`Processing specific file: ${file}`);
            return [file];
        } else {
            log(`Specified file not found: ${file}`, 'ERROR');
            return [];
        }
    }

    // Get all files and sort them numerically
    const allFiles = fs.readdirSync(INPUT_DIR)
        .filter(file => file.endsWith('.jsonl.gz'))
        .sort((a, b) => {
            const numA = parseInt(a.split('.')[0]);
            const numB = parseInt(b.split('.')[0]);
            return numA - numB;
        });

    // In resume mode, skip files that are already processed successfully
    if (RESUME_MODE) {
        const processedFiles = new Set();
        
        // Collect names of successfully processed files
        fs.readdirSync(OUTPUT_DIR)
            .filter(file => file.endsWith('_processed.jsonl.gz'))
            .forEach(file => {
                const baseNum = file.split('_')[0];
                const originalFile = `${baseNum}.jsonl.gz`;
                processedFiles.add(originalFile);
            });
        
        const filesToProcess = allFiles.filter(file => !processedFiles.has(file));
        log(`Found ${allFiles.length} total files, ${filesToProcess.length} remaining to process`);
        return filesToProcess;
    } else {
        log(`Found ${allFiles.length} files to process`);
        return allFiles;
    }
}

// Handle process termination
let isShuttingDown = false;
const cleanup = () => {
    if (!isShuttingDown) {
        isShuttingDown = true;
        log('\nShutting down gracefully...', 'WARN');
        logStream.end();
        setTimeout(() => process.exit(0), 1000); // Give time for log to flush
    }
};

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

// Process a single file
async function processFile(file, progressBar = null) {
    const inputPath = path.join(INPUT_DIR, file);
    const outputPath = path.join(OUTPUT_DIR, `${file.split('.')[0]}_processed.jsonl.gz`);
    
    // Log to file only, not to console when using progress bar
    log(`Starting processing: ${file}`);
    
    try {
        // Process the file with debug mode if enabled - always use quiet mode with progress bar
        const startTime = Date.now();
        const { processedCount, errorCount } = await processJsonlFile(inputPath, outputPath, { 
            debugMode: DEBUG_MODE,
            quiet: true // Always suppress processor output
        });
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        
        // Update progress bar if available
        if (progressBar) {
            progressBar.update(progressBar.current, { 
                records: processedCount, 
                errors: errorCount 
            });
        }
        
        log(`Processed ${processedCount} records with ${errorCount} errors in ${duration}s`);
        
        // Validate the output file
        log(`Validating output file: ${outputPath}`);
        await validateGzipFile(outputPath);
        
        log(`Successfully completed processing: ${file}`);
        return { success: true, records: processedCount, errors: errorCount };
    } catch (err) {
        logError(`Error processing ${file}`, err);
        
        if (fs.existsSync(outputPath)) {
            log(`Removing invalid output file: ${outputPath}`, 'WARN');
            fs.unlinkSync(outputPath);
        }
        return { success: false, records: 0, errors: 0 };
    }
}

// Process files with limited concurrency
async function processFiles() {
    const files = getFilesToProcess();
    
    if (files.length === 0) {
        log('No files to process.');
        return;
    }
    
    // Create progress bar
    const progressBar = new ProgressBar(files.length);
    
    // For single-threaded processing
    if (CONCURRENT_JOBS <= 1 || files.length === 1) {
        log(`Processing ${files.length} files sequentially`);
        
        // Process files one by one
        for (let i = 0; i < files.length; i++) {
            if (isShuttingDown) break;
            
            const file = files[i];
            const result = await processFile(file, progressBar);
            
            // Update progress bar
            progressBar.update(i + 1, { 
                successful: result.success ? progressBar.stats.successful + 1 : progressBar.stats.successful,
                failed: !result.success ? progressBar.stats.failed + 1 : progressBar.stats.failed
            });
        }
    } else {
        // For multi-threaded processing
        log(`Processing ${files.length} files with ${CONCURRENT_JOBS} concurrent jobs`);
        
        // We need to track completions separately because of concurrent execution
        const completionTracker = {
            totalFiles: files.length,
            completed: 0,
            successful: 0,
            failed: 0,
            totalRecords: 0,
            totalErrors: 0
        };
        
        // Create a pool of worker promises
        const processingPromises = [];
        
        // Function to process next file from the queue
        async function processNext(index) {
            if (index >= files.length || isShuttingDown) {
                return;
            }
            
            const file = files[index];
            try {
                const result = await processFile(file, progressBar);
                
                // Update counters atomically
                completionTracker.completed++;
                if (result.success) {
                    completionTracker.successful++;
                } else {
                    completionTracker.failed++;
                }
                completionTracker.totalRecords += result.records;
                completionTracker.totalErrors += result.errors;
                
                // Update progress bar
                progressBar.update(completionTracker.completed, {
                    successful: completionTracker.successful,
                    failed: completionTracker.failed
                });
                
                // Process the next file in the queue
                return processNext(index + CONCURRENT_JOBS);
            } catch (err) {
                logError(`Unexpected error processing file ${file}`, err);
                completionTracker.failed++;
                completionTracker.completed++;
                
                // Update progress bar
                progressBar.update(completionTracker.completed, {
                    successful: completionTracker.successful,
                    failed: completionTracker.failed
                });
                
                return processNext(index + CONCURRENT_JOBS);
            }
        }
        
        // Start initial batch of workers
        for (let i = 0; i < Math.min(CONCURRENT_JOBS, files.length); i++) {
            processingPromises.push(processNext(i));
        }
        
        // Wait for all processing to complete
        await Promise.all(processingPromises);
    }
    
    // Finalize the progress bar and show summary
    progressBar.finish();
    
    // Final log entry
    log(`Processing complete. Successfully processed ${progressBar.stats.successful} of ${files.length} files (${progressBar.stats.failed} failures).`);
}

// Run the main process
async function main() {
    try {
        // Process all files
        await processFiles();
        
        log('All processing complete');
        logStream.end();
    } catch (err) {
        logError('Fatal error in main process', err);
        logStream.end();
        process.exit(1);
    }
}

// Start the process
main();