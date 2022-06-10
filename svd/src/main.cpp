
#include <iostream>
#include "ppmisvd.hpp"

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
    PetscInt       nrows,nsv,maxit,its,nconv;
    PetscBool helpOption, isSymmetricOption, eigenWeight, flg;
    PetscScalar alpha;
    int MAXSTRINGLENGTH = 255;
    char filename[MAXSTRINGLENGTH]={'\0'}, outputUFilePath[MAXSTRINGLENGTH]={'\0'};
    PetscOptionsBegin(PETSC_COMM_WORLD,"","PPMI SVD Options","none");
    PetscOptionsString("-inputFilePath","Parquet file with schema: i,j,count","",filename, filename, MAXSTRINGLENGTH,&flg);
    PetscOptionsString("-outputFilePath","This will write the U matrix to file","",outputUFilePath, outputUFilePath, MAXSTRINGLENGTH,&flg);
    helpOption = PETSC_FALSE;
    PetscOptionsBool("-help", "Display Help Menu", "", helpOption, &helpOption, NULL);
    isSymmetricOption = PETSC_FALSE;
    PetscOptionsBool("-sym", "Has the input file stored the symmetric matrix as triangular matrix?", "", isSymmetricOption, &isSymmetricOption, NULL);
    eigenWeight = PETSC_FALSE;
    PetscOptionsBool("-eigenWeight", "multiply left singular vectors by sqrt(sigma)?", "", eigenWeight, &eigenWeight, NULL);
    alpha = 0.75;
    PetscOptionsScalar("-alpha","The exponent for smoothing PPMI", "", alpha, &alpha, NULL);
    nrows = 0;
    PetscOptionsInt("-nrows","size of input matrix","",nrows,&nrows,NULL);
    PetscOptionsEnd();
    if (helpOption) {
        ierr = MatCreate(PETSC_COMM_WORLD,&A);CHKERRQ(ierr);
        ierr = MatSetFromOptions(A);CHKERRQ(ierr);
        ierr = SVDCreate(PETSC_COMM_WORLD,&svd);CHKERRQ(ierr);
        ierr = SVDSetFromOptions(svd);CHKERRQ(ierr);
        ierr = SlepcFinalize();
        return 0;
    }
    std::string inputFilePath(filename);
    //for (int n = 1; n < argc; n++) {
    //cout << "Reading File:" << inputFilePath<< " "<<helpOption << std::endl;
    PetscMPIInt worldSize, worldRank;
    ierr = MPI_Comm_size(PETSC_COMM_WORLD,&worldSize); CHKERRQ(ierr);
    ierr = MPI_Comm_rank(PETSC_COMM_WORLD,&worldRank); CHKERRQ(ierr);
    long long int * wCounts=NULL;
    MatrixInfo mi = readParquetFile(inputFilePath, pairs, isSymmetricOption, worldRank, worldSize, wCounts, nrows);
    int ndim = mi.ndim;
    if (ndim == -1) {
        return 1;
    }
    buildMatrix(pairs, &A, mi,alpha, wCounts);
    pairs.clear();

    /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Create the singular value solver and set various options
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
    /*
    Create singular value solver context
    */
    ierr = SVDCreate(PETSC_COMM_WORLD,&svd);CHKERRQ(ierr);
    /*
    Set operator
    */
    ierr = SVDSetOperator(svd,A);CHKERRQ(ierr);
    /*
    Use thick-restart Lanczos as default solver
    */
    ierr = SVDSetType(svd,SVDTRLANCZOS);CHKERRQ(ierr);
    /*
    Set solver parameters at runtime
    */
    ierr = SVDSetFromOptions(svd);CHKERRQ(ierr);
    /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Solve the singular value system
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
    ierr = SVDSolve(svd);CHKERRQ(ierr);
    ierr = SVDGetIterationNumber(svd,&its);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Number of iterations of the method: %D\n",its);CHKERRQ(ierr);


    /*
    Get some information from the solver and display it
    */
    ierr = SVDGetType(svd,&type);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Solution method: %s\n\n",type);CHKERRQ(ierr);
    ierr = SVDGetDimensions(svd,&nsv,NULL,NULL);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Number of singular values: %D\n",nsv);CHKERRQ(ierr);
    ierr = SVDGetTolerances(svd,&tol,&maxit);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Stopping condition: tol=%.4g, maxit=%D\n",(double)tol,maxit);CHKERRQ(ierr);
    ierr = SVDGetConverged(svd,&nconv);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Number of converged approximate singular triplets: %D\n\n",nconv);CHKERRQ(ierr);
    /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Display solution and clean up
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
    if (nconv> 0){
        for (int i=0;i<nconv;i++) {
            PetscViewer viewer;
            Vec u, v;
            /*
            Get converged singular triplets: i-th singular value is stored in sigma
            */
            ierr = MatCreateVecs(A, &v, &u); CHKERRQ(ierr);
            ierr = SVDGetSingularTriplet(svd,i,&sigma, u, v);CHKERRQ(ierr);
            PetscScalar sqrt_sigma = (PetscScalar) std::sqrt(sigma);
            if (eigenWeight){
                VecScale(u, sqrt_sigma);
            }
            if (i==0) {
                PetscViewerBinaryOpen(PETSC_COMM_WORLD, outputUFilePath, FILE_MODE_WRITE, &viewer);
                VecView(u, viewer);
                PetscViewerDestroy(&viewer);
            } else {
                PetscViewerBinaryOpen(PETSC_COMM_WORLD, outputUFilePath, FILE_MODE_APPEND, &viewer);
                VecView(u, viewer);
                PetscViewerDestroy(&viewer);
            }
            MPI_Barrier(PETSC_COMM_WORLD);
            ierr = VecDestroy(&u); CHKERRQ(ierr);
            ierr = VecDestroy(&v); CHKERRQ(ierr);
        }
        ierr = PetscPrintf(PETSC_COMM_WORLD,"Wrote U Matrix\n");CHKERRQ(ierr);
        MPI_Barrier(PETSC_COMM_WORLD);
    }
    /*
    Free work space
    */
    ierr = SVDDestroy(&svd);CHKERRQ(ierr);
    ierr = MatDestroy(&A);CHKERRQ(ierr);
    ierr = SlepcFinalize();
    return 0;
}

