#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <atomic>
#include <boost/timer/timer.hpp>
#include "enumeratePairs.hpp"


using std::string;


int main(int argc, char** argv) {
    if (argc < 4) {
        std::cout << "./orderedPairsSameDay <outputFileName> <numberOfThreads> [input files] ..." << std::endl;
        exit(0);
    }
    int thread_num=boost::lexical_cast<int>(argv[2]);
    //int bufferSize = 2000000011;
    int bufferSize = 200011;
    int window = 0;
    std::string outputFilePath(argv[1]);
    using std::cout;
    std::vector<std::thread> threads;
    Table freq_map(bufferSize);
    std::vector<MedicalEvent> events;

    for (int n = 3; n < argc; n++) {
        std::atomic<int> rowCounter;
        std::atomic_init(&rowCounter, 0);
        std::string inputFilePath(argv[n]);
        cout << "Reading File:" << argv[n] << std::endl;
        readParquetFile(inputFilePath, std::ref(events));
        boost::timer::cpu_timer timer;
        for (int i =0; i<thread_num; i++) {
            threads.emplace_back(generatePairsSameDay, std::ref(freq_map), std::ref(events), i, thread_num, window, std::ref(rowCounter));
        }
        while(rowCounter < events.size()-2) {
            usleep(5 * 10e5);
            auto elapsedTime  = timer.elapsed().wall/ 1e9;
            auto rate = rowCounter / elapsedTime;
            if (rate > 0) {cout << "Estimated Completion Time: " << events.size()/rate << std::endl;}
        }

        for (int i=0; i<thread_num; i++) {
            threads[i].join();
        }
        events.clear();
        threads.clear();
        cout << "Finished File:" << argv[n] << std::endl;
        //dumpTable(freq_map, outputFilePath, tempFilePath);
    }
    dumpTable(freq_map, outputFilePath);
    return 0;
}

