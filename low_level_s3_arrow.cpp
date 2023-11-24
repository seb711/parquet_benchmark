#include <arrow/filesystem/api.h>
#include <arrow/result.h>

#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include <arrow/util/future.h>
#include <arrow/io/memory.h>

#include <aws/core/Aws.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <numeric>

#include "common.hpp"

#include "tbb/parallel_for_each.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"


static std::string bucket = "sauerwein-public-bi-benchmark-parquet";
static std::string region = "us-east-1";

static std::vector<std::string> getParquetFiles(std::string &prefix) {
    std::vector<std::string> result;

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::S3Crt::Model::ListObjectsV2Request listRequest;
        listRequest.SetBucket(bucket);
        listRequest.SetPrefix(prefix);

        std::string extension = ".parquet";

        Aws::S3Crt::ClientConfiguration config;
        config.region = region;

        Aws::S3Crt::S3CrtClient s3_client(config);

        while (true) {
            auto outcome = s3_client.ListObjectsV2(listRequest);
            if (!outcome.IsSuccess()) {
                throw std::runtime_error(outcome.GetError().GetMessage());
            }

            // Add all keys to result that end on extension
            for (auto &object: outcome.GetResult().GetContents()) {
                const std::string &key = object.GetKey();
                if (key.size() < extension.size()) {
                    continue;
                }

                if (key.compare(key.length() - extension.length(), extension.length(), extension) != 0) {
                    continue;
                }

                result.push_back(key);
            }

            // Check if we need to make another request for the remaining keys
            if (!outcome.GetResult().GetIsTruncated()) {
                break;
            }

            // Don't expect this to happen during testing, so won't bother to implement it.
            throw std::runtime_error("Not implemented");
        }
    }
    return result;
}

template<typename DType>
static inline void measure_read_typed(parquet::TypedColumnReader<DType> *reader, void *output_buffer, int64_t batch_size) {
    typedef typename DType::c_type T;
    T* output = reinterpret_cast<T *>(output_buffer);

    /*
     * Not sure if all of them actually need an array, but some of them do. Otherwise, we will segfault.
     * Declared static so we can reuse the same vector.
     */
    const int64_t max_batch_size = batch_size;
    thread_local std::vector<int16_t> def_levels;
    increase_vector_size(def_levels, max_batch_size);
    thread_local std::vector<int16_t> rep_levels;
    increase_vector_size(rep_levels, max_batch_size);
    thread_local std::vector<int64_t> values_read;
    increase_vector_size(values_read, max_batch_size);

    while (reader->HasNext()) {
        int64_t tuples_read = reader->ReadBatch(batch_size, def_levels.data(), rep_levels.data(), output, values_read.data());
        batch_size -= tuples_read;
        output += tuples_read;
    }

    if (batch_size != 0 || reader->HasNext()) {
        throw std::runtime_error("Invalid reader state after reading");
    }
}

static inline void measure_read(parquet::ColumnReader *column_reader, void *output_buffer, int64_t batch_size) {
    switch (column_reader->type()) {
        case parquet::Type::INT32: {
            auto *reader = dynamic_cast<parquet::Int32Reader *>(column_reader);
            return measure_read_typed(reader, output_buffer, batch_size);
        }
        case parquet::Type::FLOAT: {
            auto *reader = dynamic_cast<parquet::FloatReader *>(column_reader);
            return measure_read_typed(reader, output_buffer, batch_size);
        }
        case parquet::Type::DOUBLE: {
            auto *reader = dynamic_cast<parquet::DoubleReader *>(column_reader);
            return measure_read_typed(reader, output_buffer, batch_size);
        }
        case parquet::Type::BYTE_ARRAY: {
            auto *reader = dynamic_cast<parquet::ByteArrayReader *>(column_reader);
            measure_read_typed(reader, output_buffer, batch_size);
#if 0
            auto arrays = reinterpret_cast<parquet::ByteArray *>(output_buffer);
            for (int64_t idx = 0; idx < 10; idx++) {
                std::cerr << arrow::util::string_view(reinterpret_cast<const char *>(arrays[idx].ptr), arrays[idx].len) << std::endl;
            }
#endif
            return;
        }
        case parquet::Type::UNDEFINED:
        case parquet::Type::BOOLEAN:
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        case parquet::Type::INT64:
        case parquet::Type::INT96:
            throw std::runtime_error("Unexpected column type in parquet file");
    }
}

static void usage(const char *program) {
    std::cerr << "Usage: " << program << " prefix threads repetitions columns" << std::endl;
    exit(EXIT_FAILURE);
}

int main(int argc, char **argv) {
    if (argc != 5) {
        usage(argv[0]);
    }

    std::string prefix(argv[1]);

    int threads = std::stoi(argv[2]);
    tbb::task_scheduler_init init(threads);

    int repetitions = std::stoi(argv[3]);

    // If all is given, columns will stay empty and all columns are selected
    std::vector<int> columns;
    char *column_string = argv[4];
    if (strcmp(column_string, "all") != 0) {
        char *i_str = strtok(column_string, ",");
        while (i_str != nullptr) {
            int i = std::stoi(i_str);
            columns.push_back(i);
            i_str = strtok(nullptr, ",");
        }
        if (columns.empty()) {
            usage(argv[0]);
        }
    }

    std::vector parquet_files = getParquetFiles(prefix);
    if (parquet_files.empty()) {
        throw std::runtime_error("No parquet files found at prefix");
    }

    std::string uri = "s3://" + bucket + "/";
    auto fs = arrow::fs::FileSystemFromUri(uri).ValueOrDie();

    int num_columns;
    {
        // Get number of columns
        std::stringstream bucket_path;
        bucket_path << bucket << "/" << parquet_files[0];
        auto file = fs->OpenInputFile(bucket_path.str()).ValueOrDie();
        auto file_reader = parquet::ParquetFileReader::Open(file);
        num_columns = file_reader->metadata()->num_columns();
    }

    if (columns.empty()) {
        columns.resize(num_columns);
        std::iota(columns.begin(), columns.end(), 0);
    } else {
        for (int column : columns) {
            if (column < 0 || column >= num_columns) {
                throw std::runtime_error("Received invalid column index");
            }
        }
    }

    std::atomic<uint64_t> total_downloaded_size(0);

    auto t1 = std::chrono::high_resolution_clock::now();

    for (int rep = 0; rep < repetitions; rep++) {
        tbb::parallel_for_each(parquet_files, [&](const std::string &key) {
            std::stringstream bucket_path;
            bucket_path << bucket << "/" << key;
            auto file = fs->OpenInputFile(bucket_path.str()).ValueOrDie();
            total_downloaded_size += file->GetSize().ValueOrDie();

            auto file_reader = parquet::ParquetFileReader::Open(file);
            int num_row_groups = file_reader->metadata()->num_row_groups();
            tbb::parallel_for(int(0), num_row_groups, [&](int row_group_i) {
                std::shared_ptr<parquet::RowGroupReader> row_group_reader = file_reader->RowGroup(row_group_i);
                int64_t tuple_count = row_group_reader->metadata()->num_rows();
                size_t decompressed_size = (tuple_count + 100) * sizeof(parquet::ByteArray);
                tbb::parallel_for(std::size_t(0), columns.size(), [&](std::size_t column_idx) {
                    int column_i = columns[column_idx];
                    std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(column_i);
                    thread_local std::vector<char> output_buffer;
                    increase_vector_size(output_buffer, decompressed_size);
                    measure_read(column_reader.get(), output_buffer.data(), tuple_count);
                });
            });
        });
    }

    auto t2 = std::chrono::high_resolution_clock::now();

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    double s = static_cast<double>(us.count()) / static_cast<double>(1e6);

    double total_downloaded_size_gib = static_cast<double>(total_downloaded_size) / static_cast<double>(1<<30);
    double total_downloaded_size_gigabits = static_cast<double>(total_downloaded_size * 8) / static_cast<double>(1<<30);
    double gbps = total_downloaded_size_gigabits / s;

    std::cout << "Runtime[s]: " << s
              << " Downloaded[GiB]: " << total_downloaded_size_gib
              << " Bandwidth[Gbps]: " << gbps
              << std::endl;
}