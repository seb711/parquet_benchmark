include(ExternalProject)

ExternalProject_Add(
        arrow
        GIT_REPOSITORY "https://github.com/apache/arrow.git"
        GIT_TAG release-8.0.0
        UPDATE_COMMAND "" # to prevent rebuilding everytime
        SOURCE_SUBDIR cpp
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/contrib
        -DCMAKE_INSTALL_LIBDIR=lib
        -DARROW_BUILD_STATIC="OFF"
        -DARROW_COMPUTE="ON"
        -DARROW_CSV="ON"
        -DARROW_DATASET="ON"
        -DARROW_FILESYSTEM="ON"
        -DARROW_JSON="ON"
        -DARROW_MIMALLOC="ON"
        -DARROW_PARQUET="ON"
        -DARROW_SUBSTRAIT="ON"
        -DARROW_WITH_BROTLI="ON"
        -DARROW_WITH_BZ2="ON"
        -DARROW_WITH_LZ4="ON"
        -DARROW_WITH_RE2="ON"
        -DARROW_WITH_SNAPPY="ON"
        -DARROW_WITH_UTF8PROC="ON"
        -DARROW_WITH_ZLIB="ON"
        -DARROW_WITH_ZSTD="ON"
        -DARROW_S3="ON"
)

set(ARROW_INSTALL_DIR ${CMAKE_BINARY_DIR}/contrib)
set(ARROW_INCLUDE_DIR ${ARROW_INSTALL_DIR}/include)
set(ARROW_LIBRARY_PATH ${ARROW_INSTALL_DIR}/lib)

include(CMakePrintHelpers)
cmake_print_variables(ARROW_INCLUDE_DIR)
cmake_print_variables(ARROW_LIBRARY_PATH)

add_library(libarrow_dataset SHARED IMPORTED)
set_property(TARGET libarrow_dataset PROPERTY IMPORTED_LOCATION ${ARROW_LIBRARY_PATH}/libarrow_dataset.so)

add_library(libarrow SHARED IMPORTED)
set_property(TARGET libarrow PROPERTY IMPORTED_LOCATION ${ARROW_LIBRARY_PATH}/libarrow.so)

add_library(libarrow_substrait SHARED IMPORTED)
set_property(TARGET libarrow_substrait PROPERTY IMPORTED_LOCATION ${ARROW_LIBRARY_PATH}/libarrow_substrait.so)

add_library(libparquet SHARED IMPORTED)
set_property(TARGET libparquet PROPERTY IMPORTED_LOCATION ${ARROW_LIBRARY_PATH}/libparquet.so)
