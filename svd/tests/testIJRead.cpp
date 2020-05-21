

#define _GLIBCXX_USE_CXX11_ABI 1
#define BOOST_TEST_MODULE TestReadParquet
#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include "ppmisvd.hpp"
#include <boost/test/unit_test.hpp>

using std::string;


BOOST_AUTO_TEST_CASE(read_parquet) {
    std::vector<IJPair> events;
    readParquetFile("../tests/test.parquet", events, PETSC_FALSE); 
    BOOST_TEST_MESSAGE("Events Size:" << events.size());
    BOOST_CHECK_EQUAL(events.size(), 1489);
    for (auto event : events) {
        BOOST_CHECK_EQUAL(event.i+event.j, event.count);
    }
}

BOOST_AUTO_TEST_CASE(read_parquet_sym) {
    std::vector<IJPair> events;
    readParquetFile("../tests/test.parquet", events, PETSC_TRUE); 
    BOOST_TEST_MESSAGE("Events Size:" << events.size());
    BOOST_CHECK_EQUAL(events.size(), 2961);
    for (auto event : events) {
        if (event.i == event.j){
            BOOST_CHECK_EQUAL( (event.i+event.j)*2, event.count);
        } else {
            BOOST_CHECK_EQUAL(event.i+event.j, event.count);
        }
    }
}