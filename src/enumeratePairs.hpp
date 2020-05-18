
#include <vector>
#include <slepcsvd.h>
#include <petscsf.h>
#include <string>


class IJPair{
public:
    int i, j;
    long long count;
    IJPair(int i, int j, int count) : i{i}, j{j},count{count} {
        //this->i = i;
        //this->j = j;
        //this->count = count;
    }
};


int readParquetFile(std::string parquetFilePath, std::vector<IJPair> &pairs);
int buildMatrix(std::vector<IJPair> &pairs, Mat *A, int ndim, PetscScalar alpha);
