#!/bin/bash

substring_to_replace="%PLACEHOLDER%"
replacement="parquet_none_arrow_0"
repetitions=5

# Function to read CSV file and sync URIs
sync_uris() {
  input_file="$1"
  index=1

  while IFS=',' read -r _ uri _; do
    mod_uri=$(echo "$uri" | sed "s/$substring_to_replace/$replacement/g")
    echo "aws s3 cp $mod_uri ./$index"
    
    filename=$(basename "$mod_uri")

    if [[ ! -f "./$index/$filename" ]]; then
      mkdir ./$index -p
      aws s3 cp $mod_uri ./$index/ --no-sign
    fi 

    ./parquet_benchmark_low_level ./$index/$filename $repetitions 64 >> "./decompression-output-$replacement.txt"

    ((index++))

  done < "$input_file"
}

if ! command -v aws &> /dev/null; then
  echo "AWS CLI not found. Please install it and configure."
  exit 1
fi

# Check if uris.csv exists
if [[ ! -f "../parquet_s3_files.csv" ]]; then
  echo "parquet_s3_files.csv file not found."
  exit 1
fi

# install things
sudo apt-get install libthrift-dev libbrotli-dev libboost-all-dev libsnappy-dev libssl-dev libcurl4-openssl-dev -y

# build the benchmark thing
make -j4 parquet_benchmark_low_level

# Sync URIs from the CSV file
# sync_uris "parquet_s3_files.csv" > "./decompression-output-$replacement.txt"
sync_uris "../parquet_s3_files.csv"