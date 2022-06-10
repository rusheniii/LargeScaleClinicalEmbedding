
#include <vector>
#include <slepcsvd.h>
#include <petscsf.h>
#include <string>


struct IJPair{
    int i;
    int j;
    long long int count;
    bool operator<(IJPair &b) {
        if (i==b.i){
            return j<b.j;
        }
        return i<b.i;
    }
    bool operator<(const IJPair &b) const {
        if (i==b.i){
            return j<b.j;
        }
        return i<b.i;
    }
};

struct MatrixInfo {
    int start, end, ndim;
};

MatrixInfo readParquetFile(std::string parquetFilePath, std::vector<IJPair> &pairs, PetscBool isSymmetric, PetscMPIInt worldRank, PetscMPIInt worldSize, long long int * &wCounts, int n);
int buildMatrix(std::vector<IJPair> &pairs, Mat *A, MatrixInfo mi, PetscScalar alpha, long long int * wCounts);
