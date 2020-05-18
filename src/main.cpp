//#define _GLIBCXX_USE_CXX11_ABI 1
#include <iostream>
#include <string>
#include <vector>
#include "enumeratePairs.hpp"
#include <petscsf.h>


static char help[] = "Singular value decomposition of a matrix.\n";

int main(int argc, char** argv) {
    using std::cout;
    using std::string;
    std::vector<IJPair> pairs;
    PetscErrorCode ierr=0;
    ierr = SlepcInitialize(&argc,&argv,(char*)0,help);
    if (ierr){
        return ierr;
    }

    ierr = PetscPrintf(PETSC_COMM_WORLD,"Compute PPMI Matrix\n");CHKERRQ(ierr);
    Mat A;
    SVD            svd;             /* singular value problem solver context */
    SVDType        type;
    PetscReal      error,tol,sigma,mu=PETSC_SQRT_MACHINE_EPSILON;
    PetscInt       nsv,maxit,its,nconv;
    PetscBool flg;
    int MAXSTRINGLENGTH = 255;
    char filename[MAXSTRINGLENGTH], outputUFilePath[MAXSTRINGLENGTH];
    PetscOptionsGetString(NULL,NULL,"-f",filename, MAXSTRINGLENGTH,&flg);
    std::string inputFilePath(filename);
    PetscOptionsGetString(NULL,NULL,"-o",outputUFilePath, MAXSTRINGLENGTH,&flg);

    if (std::string(argv[1]) =="-h") {
        ierr = MatCreate(PETSC_COMM_WORLD,&A);CHKERRQ(ierr);
        ierr = MatSetFromOptions(A);CHKERRQ(ierr);
        return 0;
    }
    //for (int n = 1; n < argc; n++) {
    cout << "Reading File:" << inputFilePath << std::endl;
    int ndim = readParquetFile(inputFilePath, pairs);
    cout << "BUILDING MATRIX"<< std::endl;
    buildMatrix(pairs, &A, ndim);
    cout << "BUILT MATRIX" << std::endl;
    //ierr = MatCreateVecs(A,&v,&u);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD,"Writing Matrix\n");CHKERRQ(ierr);
    PetscViewer viewer;
    PetscViewerBinaryOpen(PETSC_COMM_WORLD, outputUFilePath, FILE_MODE_WRITE, &viewer);
    MatView(A,viewer);
    PetscViewerDestroy(&viewer);
    ierr = MatDestroy(&A);CHKERRQ(ierr);
    return 0;
}

