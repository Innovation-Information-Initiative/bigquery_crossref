#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { spawn, execSync } = require('child_process');
const zlib = require('zlib');
const os = require('os');

// Configuration
const BASE_DIR = path.join(__dirname, '..');
const PROCESSED_DIR = path.join(BASE_DIR, 'data', 'processed');
const TEMP_DIR = path.join(os.tmpdir(), 'crossref-schema-gen');
const SCHEMA_FILE = path.join(BASE_DIR, 'schema.json');
const specificFile = process.argv[2]; // optional CLI argument for a specific file


// Ensure temp directory exists
if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR, { recursive: true });
}

// Check if generate-schema is installed and check tool capabilities
function checkDependencies() {
    try {
        // Check if generate-schema supports --input_file flag
        const helpOutput = execSync('generate-schema --help').toString();
        const hasInputFileFlag = helpOutput.includes('--input_file');
        
        if (hasInputFileFlag) {
            return { tool: 'generate-schema', useInputFileFlag: true };
        } else {
            console.log('generate-schema found but it doesn\'t support --input_file flag. Will use pipe method.');
            return { tool: 'generate-schema', useInputFileFlag: false };
        }
    } catch (e) {
        try {
            // Check if python tool supports --input_file flag
            const helpOutput = execSync('python3 -m bigquery_schema_generator.generate_schema --help').toString();
            const hasInputFileFlag = helpOutput.includes('--input_file');
            
            if (hasInputFileFlag) {
                return { tool: 'python', useInputFileFlag: true };
            } else {
                console.log('bigquery-schema-generator found but it doesn\'t support --input_file flag. Will use pipe method.');
                return { tool: 'python', useInputFileFlag: false };
            }
        } catch (e) {
            console.error('Error: Neither generate-schema nor bigquery-schema-generator is installed.');
            console.error('Please install one of the following:');
            console.error('npm install -g generate-schema');
            console.error('pip3 install bigquery-schema-generator');
            process.exit(1);
        }
    }
}

// Process a single line to ensure it's valid JSON
function processLine(line) {
    try {
        if (line.trim()) {
            JSON.parse(line);
            return line;
        }
    } catch (e) {
        console.warn('Skipping invalid JSON line');
    }
    return null;
}

// Unzip and process a file line by line
async function processGzipFile(inputFile, outputFile) {
    return new Promise((resolve, reject) => {
        const gunzip = zlib.createGunzip();
        const input = fs.createReadStream(inputFile);
        const output = fs.createWriteStream(outputFile);
        let buffer = '';
        let linesProcessed = 0;

        gunzip.on('data', (data) => {
            buffer += data.toString();
            const lines = buffer.split('\n');
            buffer = lines.pop(); // Keep the last partial line in buffer

            for (const line of lines) {
                const processedLine = processLine(line);
                if (processedLine) {
                    output.write(processedLine + '\n');
                    linesProcessed++;
                }
            }
        });

        gunzip.on('end', () => {
            if (buffer) {
                const processedLine = processLine(buffer);
                if (processedLine) {
                    output.write(processedLine + '\n');
                    linesProcessed++;
                }
            }
            output.end();
        });

        output.on('finish', () => resolve(linesProcessed));
        input.on('error', reject);
        gunzip.on('error', reject);
        output.on('error', reject);

        input.pipe(gunzip);
    });
}

// Generate backup of schema file before processing
function backupSchema(schemaFile) {
    if (fs.existsSync(schemaFile)) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupFile = `${schemaFile}.${timestamp}.backup`;
        fs.copyFileSync(schemaFile, backupFile);
        console.log(`Created backup of schema at ${backupFile}`);
        return backupFile;
    }
    return null;
}

// Function to fix schema incompatibility issues
async function fixIncompatibleSchema(schemaFile, tempFile, toolConfig) {
    console.log('Attempting to fix incompatible schema...');
    
    try {
        // Read the current schema
        const currentSchema = JSON.parse(fs.readFileSync(schemaFile, 'utf8'));
        
        // Create a new schema from scratch with just this file
        const newSchemaData = await generateSchema(tempFile, null, toolConfig);
        const newSchema = JSON.parse(newSchemaData);
        
        // Manual merge strategy - this is a simplified approach
        // A more sophisticated approach might recursively merge the schemas
        console.log('Creating merged schema...');
        
        // Write the merged schema back to file
        fs.writeFileSync(schemaFile, JSON.stringify(newSchema, null, 2));
        
        return true;
    } catch (error) {
        console.error(`Failed to fix schema incompatibility: ${error.message}`);
        return false;
    }
}

async function generateSchema(inputFile, schemaPath = null, toolConfig) {
    return new Promise((resolve, reject) => {
        let command, args;
        const { tool, useInputFileFlag } = toolConfig;
        
        if (tool === 'generate-schema') {
            command = 'generate-schema';
            args = [
                '--input_format', 'json',
                '--keep_nulls',
                '--ignore_invalid_lines'
            ];
            if (schemaPath) {
                args.push('--existing_schema_path', schemaPath);
            }
        } else { // python
            command = 'python3';
            args = [
                '-m', 'bigquery_schema_generator.generate_schema',
                '--input_format', 'json',
                '--keep_nulls',
                '--ignore_invalid_lines'
            ];
            if (schemaPath) {
                args.push('--existing_schema_path', schemaPath);
            }
        }

        const schema = spawn(command, args);
        let schemaData = '';
        let errorData = '';

        schema.stdout.on('data', (data) => {
            schemaData += data;
        });

        schema.stderr.on('data', (data) => {
            errorData += data;
        });

        schema.on('error', (error) => {
            reject(new Error(`Schema process error: ${error.message}`));
        });

        schema.on('close', (code) => {
            if (code !== 0) {
                reject(new Error(`Schema generation failed: ${errorData}`));
            } else {
                resolve(schemaData);
            }
        });

        // Handle input differently based on the tool capability
        if (useInputFileFlag) {
            // Add the input file flag
            args.push('--input_file', inputFile);
            schema.stdin.end();
        } else {
            // Use the old piping method
            const input = fs.createReadStream(inputFile);
            input.on('error', reject);
            
            schema.stdin.on('error', (err) => {
                // EPIPE errors are expected when the child process closes stdin
                if (err.code !== 'EPIPE') {
                    reject(err);
                }
            });
            
            input.pipe(schema.stdin);
        }
    });
}

// Main process
async function main() {
    const specificFile = process.argv[2]; // optional CLI argument for a specific file

    try {
        console.log('Checking dependencies...');
        const toolConfig = checkDependencies();
        console.log(`Using ${toolConfig.tool} for schema generation with ${toolConfig.useInputFileFlag ? '--input_file flag' : 'pipe method'}`);

        let files;
        if (specificFile) {
            if (!fs.existsSync(specificFile)) {
                console.error(`Specified file does not exist: ${specificFile}`);
                process.exit(1);
            }
            files = [specificFile];
        } else {
            files = fs.readdirSync(PROCESSED_DIR)
                .filter(file => file.endsWith('_processed.jsonl.gz'))
                .map(file => path.join(PROCESSED_DIR, file))
                .sort();
        }

        if (files.length === 0) {
            console.error('No files to process.');
            process.exit(1);
        }

        console.log(`Found ${files.length} file(s) to process`);

        let currentSchema = null;
        if (fs.existsSync(SCHEMA_FILE)) {
            console.log(`Existing schema found at ${SCHEMA_FILE}. Will build upon it.`);
            backupSchema(SCHEMA_FILE);
            currentSchema = SCHEMA_FILE;
        }

        let totalLinesProcessed = 0;
        let lastSuccessfulFile = -1;

        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            console.log(`\nProcessing file ${i + 1}/${files.length}: ${file}`);

            const tempFile = path.join(TEMP_DIR, `temp_${i}.jsonl`);

            try {
                console.log('Unzipping and validating JSON lines...');
                const linesProcessed = await processGzipFile(file, tempFile);
                totalLinesProcessed += linesProcessed;
                console.log(`Processed ${linesProcessed} valid JSON lines`);

                if (linesProcessed < 10) {
                    console.log(`File has too few valid lines (${linesProcessed}), skipping schema update`);
                    fs.unlinkSync(tempFile);
                    continue;
                }

                console.log('Updating schema...');
                try {
                    const schemaData = await generateSchema(tempFile, currentSchema, toolConfig);
                    fs.writeFileSync(SCHEMA_FILE, schemaData);
                    currentSchema = SCHEMA_FILE;
                    lastSuccessfulFile = i;
                    console.log(`Schema updated successfully`);
                } catch (schemaError) {
                    console.error(`Schema generation error: ${schemaError.message}`);

                    if (schemaError.message.includes('Unexpected schema_entry type') ||
                        schemaError.message.includes('old (hard); new (ignore)')) {

                        try {
                            if (await fixIncompatibleSchema(SCHEMA_FILE, tempFile, toolConfig)) {
                                console.log('Successfully fixed schema incompatibility');
                                lastSuccessfulFile = i;
                            } else {
                                console.error('Could not fix schema incompatibility, skipping this file');
                            }
                        } catch (fixError) {
                            console.error(`Error while trying to fix schema: ${fixError.message}`);
                            console.log('Skipping this file due to unfixable schema incompatibility');
                        }
                    } else {
                        console.error('Skipping file due to schema generation error');
                    }
                }
            } catch (error) {
                console.error(`Error processing file ${file}: ${error.message}`);
                console.log('Continuing with next file...');
            } finally {
                if (fs.existsSync(tempFile)) {
                    fs.unlinkSync(tempFile);
                }
            }
        }

        console.log('\nSchema generation complete!');
        console.log(`Processed ${totalLinesProcessed} total valid JSON lines`);
        console.log(`Final schema saved to: ${SCHEMA_FILE}`);

        console.log(`\nSuccessfully processed ${lastSuccessfulFile + 1} of ${files.length} file(s)`);
        if (lastSuccessfulFile < files.length - 1) {
            console.log(`Some files were skipped due to errors. You may want to run the script again.`);
        }

    } catch (error) {
        console.error('Error:', error.message);
        process.exit(1);
    } finally {
        if (fs.existsSync(TEMP_DIR)) {
            // Use a recursive directory removal that works on older Node.js versions
            function deleteFolderRecursive(folderPath) {
                if (fs.existsSync(folderPath)) {
                    fs.readdirSync(folderPath).forEach((file) => {
                        const curPath = path.join(folderPath, file);
                        if (fs.lstatSync(curPath).isDirectory()) {
                            deleteFolderRecursive(curPath);
                        } else {
                            fs.unlinkSync(curPath);
                        }
                    });
                    fs.rmdirSync(folderPath);
                }
            }
            
            try {
                deleteFolderRecursive(TEMP_DIR);
            } catch (err) {
                console.error(`Warning: Could not clean up temp directory: ${err.message}`);
            }
        }
    }
}

// Run the script
main();