#include "s3-management.hpp"

static uint64_t decompressPart(long idx) {
    s3_decompressPartFinish(idx);
    return 0;
}

static void usage(const char *msg) {
    std::cerr << "Usage: " << msg << " prealloc repeat " << std::endl;
    exit(1);
}

int main(int argc, char **argv) {
    if (argc != 3) {
        usage(argv[0]);
    }

    int prealloc = std::stoi(argv[1]);
    int repeat = std::stoi(argv[2]);

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        s3_client_t s3_client = s3_get_client();
        s3_init(repeat, prealloc, 72, decompressPart);

        std::string key = "generator/Generico_1/parquet_snappy_spark_0_none/part-00009-0521d44c-7d76-41e9-b0fc-1d191421288b-c000.snappy.parquet";

        auto t1 = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < repeat; i++) {
            s3_requestFile(s3_client, key);
        }
        s3_wait_for_end();
        auto t2 = std::chrono::high_resolution_clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
        double s = static_cast<double>(us.count()) / static_cast<double>(1e6);

        s3_free_buffers();

        std::cout << "allocated = " << allocated << " "
            << "releasedStreams = " << releasedStreams << " "
            << "releasedBuffers = " << releasedBuffers << " "
            << "total_downloaded_size = " << total_downloaded_size << " "
            << "Runtime[s] = " << s << " "
            << std::endl;
    }
    Aws::ShutdownAPI(options);
}
