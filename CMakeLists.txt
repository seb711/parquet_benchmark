cmake_minimum_required(VERSION 3.16)
project(parquet_benchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -g")

find_package(Git REQUIRED)

find_package(Threads REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)

include(${CMAKE_SOURCE_DIR}/arrow.cmake)
include(${CMAKE_SOURCE_DIR}/tbb.cmake)
include(${CMAKE_SOURCE_DIR}/aws.cmake)

add_executable(parquet_benchmark main.cpp common.cpp)
add_dependencies(parquet_benchmark arrow tbb_src)
target_include_directories(parquet_benchmark PRIVATE ${ARROW_INCLUDE_DIR} ${TBB_INCLUDE_DIR})
target_link_libraries(parquet_benchmark libarrow_dataset libarrow libarrow_substrait libparquet)

add_executable(parquet_benchmark_low_level low_level.cpp common.cpp)
add_dependencies(parquet_benchmark_low_level arrow tbb_src)
target_include_directories(parquet_benchmark_low_level PRIVATE ${ARROW_INCLUDE_DIR} ${TBB_INCLUDE_DIR})
target_link_libraries(parquet_benchmark_low_level libarrow_dataset libarrow libarrow_substrait libparquet tbb)

add_executable(parquet_benchmark_low_level_s3 low_level_s3.cpp common.cpp)
add_dependencies(parquet_benchmark_low_level_s3 arrow tbb_src libawscpp-download)
target_include_directories(parquet_benchmark_low_level_s3 PRIVATE ${ARROW_INCLUDE_DIR} ${TBB_INCLUDE_DIR} ${AWS_INCLUDE_DIR})
target_link_libraries(parquet_benchmark_low_level_s3 libarrow_dataset libarrow libarrow_substrait libparquet tbb libaws-cpp-sdk-core libaws-cpp-sdk-s3-crt)

add_executable(s3-test s3-test.cpp)
add_dependencies(s3-test tbb_src libawscpp-download)
target_include_directories(s3-test PRIVATE ${TBB_INCLUDE_DIR} ${AWS_INCLUDE_DIR})
target_link_libraries(s3-test tbb libaws-cpp-sdk-core libaws-cpp-sdk-s3-crt)

add_executable(parquet_benchmark_low_level_s3_arrow low_level_s3_arrow.cpp common.cpp)
add_dependencies(parquet_benchmark_low_level_s3_arrow arrow tbb_src libawscpp-download)
target_include_directories(parquet_benchmark_low_level_s3_arrow PRIVATE ${ARROW_INCLUDE_DIR} ${TBB_INCLUDE_DIR} ${AWS_INCLUDE_DIR})
target_link_libraries(parquet_benchmark_low_level_s3_arrow libarrow_dataset libarrow libarrow_substrait libparquet tbb libaws-cpp-sdk-core libaws-cpp-sdk-s3-crt)
