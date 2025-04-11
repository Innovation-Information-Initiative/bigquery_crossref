This tool transforms the [2025 Crossref Public Data File](https://www.crossref.org/blog/2025-public-data-file-now-available/) into a BigQuery-compatible format. The processor handles several critical transformations:

- Converts date structures into ISO-standard date strings
- Resolves NULL value incompatibilities
- Standardizes field names
- Flattens nested arrays to comply with BigQuery's schema requirements
- Provides special handling for problematic year fields and identifiers

The resulting dataset is publicly available on the I3 Bigquery Data repository [here](https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1snber-i3!2scrossref!3scr_2025)

Join our mailing group: [https://groups.google.com/g/i3-bigquery](https://groups.google.com/g/i3-bigquery)
Visit our website: [https://iii.pubpub.org/](https://iii.pubpub.org/)

## Project Structure

```
.
├── jsonl-processor/     # Main processing module
│   ├── src/            # Source code
│   │   ├── process-all.js    # Main processing script
│   │   ├── processor.js      # Core processing logic
│   │   └── generate-schema.js # Schema generation for BigQuery
│   ├── data/          # Data directories
│   │   ├── raw/       # Raw input files (.jsonl.gz)
│   │   └── processed/ # Processed output files
│   └── logs/          # Processing logs
```

## Requirements

- Node.js
- Sufficient disk space (around 400GB)
- Linux environment

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Innovation-Information-Initiative/bigquery_crossref.git
```

2. Install dependencies:
```bash
cd jsonl-processor
npm install
```

## Usage

### Processing Files

The main processing script can be run with various options:

```bash
# Process all files
node src/process-all.js

# Process with debug mode
DEBUG=true node src/process-all.js

# Process a specific file
FILE=example.jsonl.gz node src/process-all.js

# Process without resuming previous work
RESUME=false node src/process-all.js

# Run in quiet mode (no progress display)
QUIET=true node src/process-all.js
```

### Processing Output

Processed files are saved in the `data/processed` directory with the naming format:
`[file_number]_processed.jsonl.gz`

Logs are stored in the `logs` directory with timestamps.


### Generating BigQuery Schema

Before loading data into BigQuery, generate the schema from your processed files:

```bash
# First, install one of these schema generators:
npm install -g generate-schema
# OR
pip3 install bigquery-schema-generator

# Then run the schema generator
node jsonl-processor/src/generate-schema.js
```

For the complete 2025 Crossref public data file, the automatically generated schema will require additional manual edits. For convenience, a pre-configured schema file is provided in the repository.

## Uploading Data to Bigquery

Upload all processed files to Google Cloud Storage:
```bash
gsutil -m cp jsonl-processor/data/processed/* gs://[bucket]
```

### Loading Data into BigQuery

Create a BigQuery table from the uploaded files:
```bash
bq load --source_format=NEWLINE_DELIMITED_JSON \
        --replace=true \
        --project_id=[project_name] \
        --max_bad_records=10 \ 
        [dataset_name.table_name] \
        gs://[bucket]/* \
        schema.json
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

