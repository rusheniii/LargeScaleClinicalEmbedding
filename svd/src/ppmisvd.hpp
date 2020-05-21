
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


int readParquetFile(std::string parquetFilePath, std::vector<IJPair> &pairs, PetscBool isSymmetric);
int buildMatrix(std::vector<IJPair> &pairs, Mat *A, int ndim, PetscScalar alpha);
