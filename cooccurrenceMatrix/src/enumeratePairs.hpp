#include <string>
#include <vector>
#include <atomic>
#include <boost/lexical_cast.hpp>
#include "tbb/concurrent_hash_map.h"
#include <boost/functional/hash.hpp>


typedef std::pair<int, int> KeyType;

struct BoostHashCompare {
    static size_t hash( const KeyType &key ) {
        return static_cast<size_t>(boost::hash_value(key));
    }
    static bool equal( const KeyType &key1, const KeyType &key2) {
        return key1.first==key2.first && key1.second==key2.second;
    }
};

typedef tbb::concurrent_hash_map<KeyType, long long int, BoostHashCompare> Table;

using std::string;

class MedicalEvent{
public:
    int psid, numDays, word;
    MedicalEvent(int psid, int numDays, int word) {
        this->psid = psid;
        this->numDays = numDays;
        this->word = word;
    }
    MedicalEvent(string psid, string numDays, string word) {
        this->psid = boost::lexical_cast<int>(psid);
        this->numDays = boost::lexical_cast<int>(numDays);
        this->word = boost::lexical_cast<int>(word);

    }
};


void readParquetFile(string parquetFilePath, std::vector<MedicalEvent> &events);
void readGzipFile(string filePath, std::vector<MedicalEvent> &events);
void generatePairsBefore( Table &pairs, std::vector<MedicalEvent> &events, int threadId, int totalThreads, int window, std::atomic<int> &rowCounter);
void generatePairsSameDay( Table &pairs, std::vector<MedicalEvent> &events, int threadId, int totalThreads, int window, std::atomic<int> &rowCounter);
void dumpTable( Table &pairs, string outPath);
