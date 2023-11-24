#!/bin/bash

substring_to_replace="%PLACEHOLDER%"
replacement="parquet_none_arrow_0"

# Using sed to replace the substring in the variable
new_string=$(echo "$original_string" | sed "s/$substring_to_replace/$replacement/g")

echo "Original string: $original_string"
echo "New string: $new_string"

# Function to read CSV file and sync URIs
sync_uris() {
  input_file="$1"

  while IFS=',' read -r _ uri _; do
    echo "Syncing $(echo "$uri" | sed "s/$substring_to_replace/$replacement/g") to S3..."
    # aws s3 sync "$(echo "$uri" | sed "s/$substring_to_replace/$replacement/g")" --nosign

  done < "$input_file"
}

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
  echo "AWS CLI not found. Please install it and configure."
  exit 1
fi

# Check if uris.csv exists
if [[ ! -f "parquet_s3_files.csv" ]]; then
  echo "parquet_s3_files.csv file not found."
  exit 1
fi

# build the benchmark thing
make parquet_benchmark

# Sync URIs from the CSV file
sync_uris "parquet_s3_files.csv"