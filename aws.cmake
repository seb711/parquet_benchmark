include(ExternalProject)
find_package(Git REQUIRED)

# libawscpp
ExternalProject_Add(
        libawscpp-download
        PREFIX "vendor/libawscpp-download"
        GIT_REPOSITORY "https://github.com/aws/aws-sdk-cpp.git"
        GIT_TAG "1.9.239"
        TIMEOUT 10
        LIST_SEPARATOR "|"
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/libawscpp-download
        -DCMAKE_INSTALL_LIBDIR=lib
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DENABLE_TESTING=OFF # Test are broken with gcc-11, gcc-10 leads to errors about missing atomic operations. Gave up and disabled tests.
        -DBUILD_ONLY=s3-crt
        UPDATE_COMMAND ""
        TEST_COMMAND ""
)

ExternalProject_Get_Property(libawscpp-download install_dir)
set(AWS_INCLUDE_DIR ${install_dir}/include)
set(AWS_LIBRARY_PATH ${install_dir}/lib)
file(MAKE_DIRECTORY ${AWS_INCLUDE_DIR})

include(CMakePrintHelpers)
cmake_print_variables(AWS_INCLUDE_DIR)
cmake_print_variables(AWS_LIBRARY_PATH)

# Prepare libaws-cpp-sdk-core
add_library(libaws-cpp-sdk-core SHARED IMPORTED)
set_property(TARGET libaws-cpp-sdk-core PROPERTY IMPORTED_LOCATION ${AWS_LIBRARY_PATH}/libaws-cpp-sdk-core.so)
#add_dependencies(libaws-cpp-sdk-core libawscpp-download)

# Prepare libaws-cpp-sdk-s3-crt
add_library(libaws-cpp-sdk-s3-crt SHARED IMPORTED)
set_property(TARGET libaws-cpp-sdk-s3-crt PROPERTY IMPORTED_LOCATION ${AWS_LIBRARY_PATH}/libaws-cpp-sdk-s3-crt.so)
#add_dependencies(libaws-cpp-sdk-s3-crt libawscpp-download)