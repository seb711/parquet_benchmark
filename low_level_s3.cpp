#include "s3-management.hpp"

#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include <arrow/util/future.h>
#include <arrow/io/memory.h>

#include "tbb/parallel_for.h"

#include <aws/s3-crt/model/ListObjectsV2Request.h>

#include "common.hpp"

static std::atomic<uint64_t> row_group_count = 0;
static std::atomic<uint64_t> row_count = 0;
static std::atomic<uint64_t> part_count = 0;
static std::vector<int> columns;

static std::vector<std::string> getParquetFiles(const s3_client_t &s3_client, const char *prefix) {
    s3::Model::ListObjectsV2Request listRequest;
    listRequest.SetBucket(s3_bucket);
    listRequest.SetPrefix(prefix);

    std::string extension = ".parquet";
    std::vector<std::string> result;

    while (true) {
        auto outcome = s3_client.ListObjectsV2(listRequest);
        if (!outcome.IsSuccess()) {
            throw std::runtime_error(outcome.GetError().GetMessage());
        }

        // Add all keys to result that end on extension
        for (auto &object : outcome.GetResult().GetContents()) {
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
            /* This prints string contents for debugging */
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

static uint64_t decompressPart(long idx) {
    part_count += 1;
    uint8_t *buffer = streambufarrays[idx].data();
    auto buffer_len = streambuflens[idx];

    std::shared_ptr<arrow::io::RandomAccessFile> file = std::make_shared<arrow::io::BufferReader>(buffer, buffer_len);
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::Open(file);

    int num_row_groups = reader->metadata()->num_row_groups();
    row_group_count += num_row_groups;
    int num_columns = reader->metadata()->num_columns();
    uint64_t local_row_count = 0;

    for (int row_group_i = 0; row_group_i < num_row_groups; row_group_i++) {
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = reader->RowGroup(row_group_i);
        int64_t tuple_count = row_group_reader->metadata()->num_rows();
        local_row_count += tuple_count;
        size_t decompressed_size = (tuple_count + 100) * sizeof(parquet::ByteArray);
        if (columns.empty()) {
            for (int column_idx = 0; column_idx < num_columns; column_idx++) {
                std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(column_idx);
                thread_local std::vector<char> output_buffer;
                increase_vector_size(output_buffer, decompressed_size);
                measure_read(column_reader.get(), output_buffer.data(), tuple_count);
            }
        } else {
            for (int column_idx : columns) {
                std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(column_idx);
                thread_local std::vector<char> output_buffer;
                increase_vector_size(output_buffer, decompressed_size);
                measure_read(column_reader.get(), output_buffer.data(), tuple_count);
            }
        }
    }

    row_count += local_row_count;

    s3_decompressPartFinish(idx);
    return 0;
}

static void usage(const char *program) {
    std::cerr << "Usage: " << program << " prefix prealloc threads repetitions columns" << std::endl;
    exit(1);
}

int main(int argc, char **argv) {
    // Parse args
    if (argc != 6) {
        usage(argv[0]);
    }
    char *prefix = argv[1];
    auto prealloc = std::stoi(argv[2]);
    auto threads = std::stoi(argv[3]);
    if (threads <= 0) {
        threads = tbb::flow::unlimited;
    }
    auto repetitions = std::stoi(argv[4]);

    // If all is given, columns will stay empty and all columns are selected
    char *column_string = argv[5];
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

    // Prepare S3 API.
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        s3_client_t s3_client = s3_get_client();

        auto files = getParquetFiles(s3_client, prefix);

        s3_init(repetitions * files.size(), prealloc, threads, decompressPart);

        auto t1 = std::chrono::high_resolution_clock::now();
        // Start all requests asynchronously
        for (uint64_t rep = 0; rep < repetitions; rep++) {
            for (const std::string &key : files) {
                s3_requestFile(s3_client, key);
            }
        }

        s3_wait_for_end();
        auto t2 = std::chrono::high_resolution_clock::now();

        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
        double s = static_cast<double>(us.count()) / static_cast<double>(1e6);

        double total_downloaded_size_gib = static_cast<double>(total_downloaded_size) / static_cast<double>(1<<30);
        double total_downloaded_size_gigabits = static_cast<double>(total_downloaded_size * 8) / static_cast<double>(1<<30);
        double gbps = total_downloaded_size_gigabits / s;

#if 0
        std::cerr << "part_count = " << part_count
                  << " row_group_count = " << row_group_count
                  << " row_count = " << row_count
                  << std::endl;
#endif
        std::cout << "Runtime[s]: " << s
                  << " Downloaded[GiB]: " << total_downloaded_size_gib
                  << " Bandwidth[Gbps]: " << gbps
                  << " Requests: " << total_requests
                  << std::endl;

        s3_free_buffers();
    }
}
