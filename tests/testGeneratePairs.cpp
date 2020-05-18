

#define _GLIBCXX_USE_CXX11_ABI 1
#define BOOST_TEST_MODULE TestReadParquet
#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include "enumeratePairs.hpp"
#include <boost/test/unit_test.hpp>

using std::string;


BOOST_AUTO_TEST_CASE(read_parquet) {
    std::vector<MedicalEvent> events_parquet;
    readParquetFile("../tests/test.parquet", std::ref(events_parquet));
    BOOST_CHECK_EQUAL( events_parquet.size(), 9801);
    int thread_num  = 3;
    int window = 30;
    std::vector<std::thread> threads;
    Table freq_map;
    std::atomic<int> rowCounter;
    std::atomic_init(&rowCounter, 0);

    for (int i =0; i<thread_num; i++) {
        threads.emplace_back(generatePairs, std::ref(freq_map), std::ref(events_parquet), i, thread_num, window, std::ref(rowCounter));
    }
    for (int i=0; i<thread_num; i++) {
        threads[i].join();
    }
    BOOST_CHECK_EQUAL(freq_map.size() , 480152);
}
