
//#define _GLIBCXX_USE_CXX11_ABI 1
#include <vector>
#include <atomic>
#include <slepcsvd.h>
#include <string>


class IJPair{
public:
    int i, j;
    long long count;
    IJPair(int i, int j, int count) {
        this->i = i;
        this->j = j;
        this->count = count;
    }
};


int readParquetFile(std::string parquetFilePath, std::vector<IJPair> &pairs);
int buildMatrix(std::vector<IJPair> &pairs, Mat *A, int ndim);
