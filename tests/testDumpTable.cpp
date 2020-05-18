

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
    int window = 30;
    std::string outputFilePath("table.csv");
    using std::cout;
    std::vector<std::thread> threads;
    Table freq_map;
    Table::accessor a;
    KeyType k(1,2);
    freq_map.insert(a, k);
    a->second++;
    auto t = std::thread(dumpTable, std::ref(freq_map), outputFilePath);
    t.join();
}
