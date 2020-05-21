

#define _GLIBCXX_USE_CXX11_ABI 1
#define BOOST_TEST_MODULE TestReadParquet
#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include "enumeratePairs.hpp"
#include <boost/test/unit_test.hpp>



BOOST_AUTO_TEST_CASE(read_parquet) {
    std::vector<MedicalEvent> events_parquet;
    auto t = std::thread(readParquetFile,"../tests/test.parquet", std::ref(events_parquet)); 
    t.join();
    //readParquetFile("../tests/test.parquet", std::ref(events_parquet));
    BOOST_CHECK_EQUAL( events_parquet.size(), 9801);
    /*for (int i =0 ; i < events_parquet.size(); i++) {
        BOOST_CHECK_EQUAL( events_parquet[i].psid, events_gzip[i].psid);
        BOOST_CHECK_EQUAL( events_parquet[i].numDays, events_gzip[i].numDays);
        BOOST_CHECK_EQUAL( events_parquet[i].word, events_gzip[i].word);
    }*/
}
