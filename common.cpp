//
// Created by david on 29.06.22.
//

#include "common.hpp"

#include <cassert>
#include <iostream> 

#include <parquet/file_reader.h>

std::vector<std::filesystem::path> get_parquet_files(const char *parquet_c_path) {
    std::filesystem::path parquet_path(parquet_c_path);
    std::vector<std::filesystem::path> parquet_files;
    {
        std::filesystem::directory_entry entry(parquet_path);
        if (entry.is_directory()) {
            for (auto const& dir_entry : std::filesystem::directory_iterator{parquet_path}) {
                if (dir_entry.path().extension() == ".parquet") {
                    std::cout << dir_entry.path() << std::endl; 
                    parquet_files.push_back(dir_entry.path());
                }
            }
        } else {
            parquet_files.push_back(parquet_path);
        }
    }
    assert(!parquet_files.empty());

    return parquet_files;
}

int get_column_info(const std::filesystem::path &parquet_file, std::vector<parquet::Type::type> &types) {
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(
            parquet_file,
            false);
    //RowGroupMetaData
    //std::unique_ptr<ColumnChunkMetaData> ColumnChunk(int index) const;
    int num_columns = parquet_reader->metadata()->num_columns();
    for (int column = 0; column < num_columns; column++) {
        types.push_back(parquet_reader->metadata()->RowGroup(0)->ColumnChunk(column)->type());
    }
    parquet_reader->Close();

    return num_columns;
}

int get_num_row_groups(const std::filesystem::path &parquet_file) {
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(
            parquet_file,
            false);
    int num_row_groups = parquet_reader->metadata()->num_row_groups();
    parquet_reader->Close();

    return num_row_groups;
}