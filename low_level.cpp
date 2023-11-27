#include <iostream>
#include <memory>
#include <vector>
#include <chrono>
#include <exception>
#include <algorithm>

#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include <arrow/util/future.h>
#include <arrow/io/memory.h>
#include <fstream>
#include <numeric>

#include "common.hpp"
#include "tbb/parallel_for_each.h"
#include "tbb/task_scheduler_init.h"

#define OUTPUTOLUMN
#define OUTPUTTOTAL

template<typename DType>
static inline uint64_t measure_read_typed(parquet::TypedColumnReader<DType> *reader, void *output_buffer, int64_t batch_size) {
    typedef typename DType::c_type T;
    T* output = reinterpret_cast<T *>(output_buffer);

    uint64_t total_time = 0;

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

    auto start_time = std::chrono::steady_clock::now();
    while (reader->HasNext()) {
        int64_t tuples_read = reader->ReadBatch(batch_size, def_levels.data(), rep_levels.data(), output, values_read.data());
        batch_size -= tuples_read;
        output += tuples_read;
    }
    auto end_time = std::chrono::steady_clock::now();
    auto runtime = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    total_time += runtime.count();

    if (batch_size != 0 || reader->HasNext()) {
        throw std::runtime_error("Invalid reader state after reading");
    }
    return total_time;
}

static inline uint64_t measure_read(parquet::ColumnReader *column_reader, void *output_buffer, int64_t batch_size) {
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
#if 0
            auto arrays = reinterpret_cast<parquet::ByteArray *>(output_buffer);
            for (int64_t idx = 0; idx < 10; idx++) {
                std::cerr << arrow::util::string_view(reinterpret_cast<const char *>(arrays[idx].ptr), arrays[idx].len) << std::endl;
            }
#endif
            return measure_read_typed(reader, output_buffer, batch_size);
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
    std::cerr << "Usage: " << program << " <parquetfile> <repetitions> <threads> <columnlist>" << std::endl;
    exit(EXIT_FAILURE);
}

int main(int argc, char **argv) {
    if (argc < 2 || argc > 5) {
        usage(argv[0]);
    }

    auto parquet_files = get_parquet_files(argv[1]);
    int repetitions = 1;
    if (argc >= 3) {
        repetitions = std::stoi(argv[2]);
    }

    int threads = -1;
    if (argc >= 4) {
        threads = std::stoi(argv[3]);
    }
    tbb::task_scheduler_init init(threads);

    std::vector<parquet::Type::type> types;
    int num_columns = get_column_info(parquet_files[0], types);
    std::vector<int> columns;
    if (argc >= 5) {
        char *select_string = argv[4];
        if (strcmp(select_string, "integer") == 0) {
            for (int idx = 0; idx < types.size(); idx++) {
                switch (types[idx]) {
                    case parquet::Type::INT32: /* Fall-through */
                    case parquet::Type::INT64: /* Fall-through */
                    case parquet::Type::INT96: {
                        columns.push_back(idx);
                        break;
                    }
                    default: /* NOP */ break;
                }
            }
        } else if (strcmp(select_string, "double") == 0) {
            for (int idx = 0; idx < types.size(); idx++) {
                switch (types[idx]) {
                    case parquet::Type::DOUBLE: /* Fall-through */
                    case parquet::Type::FLOAT: {
                        columns.push_back(idx);
                        break;
                    }
                    default: /* NOP */ break;
                }
            }
        } else if (strcmp(select_string, "string") == 0) {
            for (int idx = 0; idx < types.size(); idx++) {
                switch (types[idx]) {
                    case parquet::Type::BYTE_ARRAY: /* Fall-through */
                    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                        columns.push_back(idx);
                        break;
                    }
                    default: /* NOP */ break;
                }
            }
        } else {
            char *i_str = strtok(select_string, ",");
            while (i_str != nullptr) {
                int i = std::stoi(i_str);
                if (i < 0 || i >= num_columns) {
                    std::cerr << "selected column not within valid range [0," << num_columns - 1 << "]. Got: " << i
                              << std::endl;
                    exit(EXIT_FAILURE);
                }
                columns.push_back(i);
                i_str = strtok(nullptr, ",");
            }

            if (columns.empty()) {
                usage(argv[0]);
            }
        }
    } else {
        columns.resize(num_columns);
        std::iota(columns.begin(), columns.end(), 0);
    }

    std::cerr << "Decompressing columns: ";
    for (auto idx : columns) {
        std::cerr << idx << " ";
    }
    std::cerr << std::endl;

    std::vector<std::atomic<uint64_t>> runtimes(num_columns);
    for (auto &r : runtimes) {
        r.store(0);
    }

    std::vector<std::atomic<uint64_t>> compressed_sizes(num_columns);
    for (auto &r : compressed_sizes) {
        r.store(0);
    }

    std::vector<std::atomic<uint64_t>> uncompressed_sizes(num_columns);
    for (auto &r : uncompressed_sizes) {
        r.store(0);
    }

    // Read all files into memory
    std::vector<std::vector<char>> file_vector(parquet_files.size());
    tbb::parallel_for(std::size_t(0), parquet_files.size(), [&](std::size_t idx) {
        auto path = parquet_files[idx];
        std::ifstream file_stream(path, std::ios::binary | std::ios::ate);
        if (file_stream.fail()) {
            auto msg = "Error opening " + path.string();
            perror(msg.c_str());
            throw std::runtime_error(msg);
        }
        std::streamsize filesize = file_stream.tellg();
        file_stream.seekg(0, std::ios::beg);
        file_vector[idx].resize(filesize);
        file_stream.read(file_vector[idx].data(), filesize);
        if (file_stream.bad()) {
            throw std::runtime_error("Reading metadata failed");
        }
        file_stream.close();
    });

    // Create arrow files
    std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> inputs;
    for (std::size_t idx = 0; idx < file_vector.size(); idx++) {
        inputs.push_back(std::make_shared<arrow::io::BufferReader>(reinterpret_cast<const uint8_t *>(file_vector[idx].data()), static_cast<int64_t>(file_vector[idx].size())));
    }

    // Create parquet readers
    std::vector<std::unique_ptr<parquet::ParquetFileReader>> parquet_readers;
    for (std::size_t idx = 0; idx < inputs.size(); idx++) {
        parquet_readers.push_back(parquet::ParquetFileReader::Open(inputs[idx]));
    }

    int num_row_groups = parquet_readers[0]->metadata()->num_row_groups();
    auto start_time = std::chrono::steady_clock::now();
    for (int rep = 0; rep < repetitions; rep++) {
        tbb::parallel_for(std::size_t(0), parquet_readers.size(), [&](size_t reader_i) {
            tbb::parallel_for(int(0), num_row_groups, [&](int row_group_i) {
                std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_readers[reader_i]->RowGroup(row_group_i);
                int64_t tuple_count = row_group_reader->metadata()->num_rows();
                size_t decompressed_size = (tuple_count + 100) * sizeof(parquet::ByteArray);
                tbb::parallel_for(std::size_t(0), columns.size(), [&](std::size_t column_idx) {
                    int column_i = columns[column_idx];
                    std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(column_i);
                    uint64_t uncompressed_size = row_group_reader->metadata()->ColumnChunk(column_i)->total_uncompressed_size();
                    uncompressed_sizes[column_i] += uncompressed_size;
                    uint64_t compressed_size = row_group_reader->metadata()->ColumnChunk(column_i)->total_compressed_size();
                    compressed_sizes[column_i] += compressed_size;
                    thread_local std::vector<char> output_buffer;
                    increase_vector_size(output_buffer, decompressed_size);
                    runtimes[column_i] += measure_read(column_reader.get(), output_buffer.data(), tuple_count);
                });
            });
        });
    }
    auto end_time = std::chrono::steady_clock::now();
    auto runtime = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    uint64_t total_runtime = runtime.count();

    uint64_t total_runtime_col = 0;
    uint64_t total_uncompressed_size = 0;
    uint64_t total_compressed_size = 0;
    for (int column = 0; column < num_columns; column++) {
        total_runtime_col = static_cast<double>(runtimes[column]) / static_cast<double>(repetitions);
        // double compression_ratio = static_cast<double>(uncompressed_sizes[column]) / static_cast<double>(compressed_sizes[column]);
        total_uncompressed_size += uncompressed_sizes[column];
        total_compressed_size += compressed_sizes[column];
        // std::cout << "Column " << column << " " << uncompressed_sizes[column] << " Bytes " << compressed_sizes[column] << " Bytes " << compression_ratio << " Ratio " << avg_runtime << " us" << std::endl;
    }
    double compression_ratio = static_cast<double>(total_uncompressed_size) / static_cast<double>(total_compressed_size);
    double avg_runtime = static_cast<double>(total_runtime) / static_cast<double>(repetitions);
    double total_compressed_size_ratio = static_cast<double>(total_compressed_size) / static_cast<double>(repetitions);

    // new format needed for python script
    std::cout << total_compressed_size << " " << avg_runtime << " " << total_runtime_col << std::endl;

    return 0;
}
