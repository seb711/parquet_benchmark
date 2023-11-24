#include <iostream>
#include <vector>
#include <memory>
#include <numeric>
#include <chrono>

#include <parquet/file_reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <arrow/status.h>
#include <arrow/api.h>

#include "common.hpp"

int main(int argc, char **argv) {
    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: " << argv[0] << " <parquetfile> <repetitions>" << std::endl;
        return 1;
    }

    auto parquet_files = get_parquet_files(argv[1]);
    int repetitions = 1;
    if (argc == 3) {
        repetitions = std::stoi(argv[2]);
    }

    std::vector<parquet::Type::type> types;
    int num_columns = get_column_info(parquet_files[0], types);
    std::vector<uint64_t> runtimes(num_columns, 0);

    for (auto const& parquet_file : parquet_files) {
        // Fetch necessary metadata first
        int num_row_groups = get_num_row_groups(parquet_file);

        // Perform reads for every column
        for (int column = 0; column < num_columns; column++) {
            for (int rep = 0; rep < repetitions; rep++) {
                // Prepare parameters for PreBuffer
                std::vector<int> row_groups(num_row_groups);
                std::iota(row_groups.begin(), row_groups.end(), 0);
                std::vector<int> columns{column};
                const auto &io_context = arrow::io::default_io_context();
                auto cache_options = arrow::io::CacheOptions::Defaults();


                // Create a reader for the file
                std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(
                        parquet_file,
                        false);
                // Read the column into memory for all row groups before processing
                //parquet_reader->PreBuffer(row_groups, columns, io_context, cache_options);

                // Create an arrow reader from the parquet reader.
                // The parquet_reader is invalid after this point.
                std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
                auto arrow_reader_properties = parquet::default_arrow_reader_properties();
                arrow_reader_properties.set_use_threads(true);
                arrow_reader_properties.set_pre_buffer(true);
                PARQUET_THROW_NOT_OK(
                        parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(parquet_reader),
                                                         arrow_reader_properties, &arrow_reader));

                std::shared_ptr<::arrow::ChunkedArray> out;
                arrow::Status s;

                auto start_time = std::chrono::steady_clock::now();

                s = arrow_reader->ReadColumn(column, &out);

                auto end_time = std::chrono::steady_clock::now();
                auto runtime = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
                runtimes[column] += runtime.count();

                // Check if any errors occured
                PARQUET_THROW_NOT_OK(s);
                //PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*out, 4, &std::cout));
            }
            // Average runtime over all repetitions
        }
    }

    double avg_runtime_table = 0.0; 

    for (int column = 0; column < num_columns; column++) {
        double avg_runtime = static_cast<double>(runtimes[column]) / static_cast<double>(repetitions);

        avg_runtime_table += avg_runtime; 
        // std::cout << "Column " << column << " " << avg_runtime << " us" << std::endl;
    }

    std::cout << avg_runtime_table << std::endl;

    return 0;
}
