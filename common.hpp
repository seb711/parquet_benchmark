#pragma once

#include <vector>
#include <filesystem>
#include "parquet/types.h"

std::vector<std::filesystem::path> get_parquet_files(const char *parquet_c_path);
int get_column_info(const std::filesystem::path &parquet_file, std::vector<parquet::Type::type> &types);
int get_num_row_groups(const std::filesystem::path &parquet_file);
template<typename T>
static inline void increase_vector_size(std::vector<T> &v, std::size_t new_size) {
    v.resize(std::max(v.size(), new_size));
}