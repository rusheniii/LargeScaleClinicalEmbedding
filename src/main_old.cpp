#define _GLIBCXX_USE_CXX11_ABI 1
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <boost/timer/timer.hpp>
#include "enumeratePairs.hpp"
#include <petscsf.h>

using std::string;

static char help[] = "Singular value decomposition of a matrix.\n";

int main(int argc, char** argv) {
    using std::cout;
    std::vector<std::thread> threads;
    std::vector<IJPair> pairs;
    PetscErrorCode ierr=0;

    ierr = SlepcInitialize(&argc,&argv,(char*)0,help);
    if (ierr){
        return ierr;
    }

    ierr = PetscPrintf(PETSC_COMM_WORLD,"Singular value decomposition\n");CHKERRQ(ierr);
    Mat A;
    SVD            svd;             /* singular value problem solver context */
    SVDType        type;
    PetscReal      error,tol,sigma,mu=PETSC_SQRT_MACHINE_EPSILON;
    PetscInt       nsv,maxit,its,nconv;

    if (std::string(argv[1]) =="-h") {
        /*
        Create singular value solver context
        */
        ierr = MatCreate(PETSC_COMM_WORLD,&A);CHKERRQ(ierr);
        ierr = MatSetFromOptions(A);CHKERRQ(ierr);
        ierr = SVDCreate(PETSC_COMM_WORLD,&svd);CHKERRQ(ierr);
        /*
        Set solver parameters at runtime
        */
        ierr = SVDSetFromOptions(svd);CHKERRQ(ierr);
        return 0;
    }
    PetscBool flg;
    int MAXSTRINGLENGTH = 255;
    char filename[MAXSTRINGLENGTH], outputUFilePath[MAXSTRINGLENGTH];
    PetscOptionsGetString(NULL,NULL,"-f",filename, MAXSTRINGLENGTH,&flg);
    PetscOptionsGetString(NULL,NULL,"-o",outputUFilePath, MAXSTRINGLENGTH,&flg);
    //for (int n = 1; n < argc; n++) {
    std::atomic<int> rowCounter;
    std::atomic_init(&rowCounter, 0);
    std::string inputFilePath(filename);
    cout << "Reading File:" << filename << std::endl;
    int ndim = readParquetFile(inputFilePath, pairs);
    boost::timer::cpu_timer timer;
    cout << "BUILDING MATRIX"<< std::endl;
    buildMatrix(pairs, &A, ndim);
    cout << "BUILT MATRIX" << std::endl;
    //ierr = MatCreateVecs(A,&v,&u);CHKERRQ(ierr);
    cout << "CREATING SVD" << std::endl;
    //MatView(A,PETSC_VIEWER_STDOUT_WORLD);
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
    cout << "SOLVING SVD" << std::endl;
    ierr = SVDSolve(svd);CHKERRQ(ierr);
    cout << "SOLVED SVD" << std::endl;
    ierr = SVDGetIterationNumber(svd,&its);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Number of iterations of the method: %D\n",its);CHKERRQ(ierr);

    /*
    Optional: Get some information from the solver and display it
    */
    ierr = SVDGetType(svd,&type);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Solution method: %s\n\n",type);CHKERRQ(ierr);
    ierr = SVDGetDimensions(svd,&nsv,NULL,NULL);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Number of requested singular values: %D\n",nsv);CHKERRQ(ierr);
    ierr = SVDGetTolerances(svd,&tol,&maxit);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Stopping condition: tol=%.4g, maxit=%D\n",(double)tol,maxit);CHKERRQ(ierr);
    ierr = SVDGetConverged(svd,&nconv);CHKERRQ(ierr);
    /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Display solution and clean up
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
    if (nconv> 0){
        std::vector<Vec> U_columns;
        Vec u,v;
        ierr = MatCreateVecs(A,&v,&u);CHKERRQ(ierr);
        ierr = PetscPrintf(PETSC_COMM_WORLD,"Creating U Matrix\n");CHKERRQ(ierr);
        Mat U;
        PetscScalar * UData=0;
        //MatCreate(PETSC_COMM_WORLD, &U);
        MatCreate(PETSC_COMM_WORLD, &U);
        MatSetSizes(U,ndim,nconv, ndim, nconv);
        MatSetType(U,MATDENSE);
        PetscMalloc1(ndim*nconv,&UData);
        MatSeqDenseSetPreallocation(U,UData);
        MatAssemblyBegin(U,MAT_FINAL_ASSEMBLY);
        MatAssemblyEnd(U,MAT_FINAL_ASSEMBLY);

        ierr = PetscPrintf(PETSC_COMM_WORLD,"Initializing U Matrix\n");CHKERRQ(ierr);
        for (int i=0;i<nconv;i++) {

            /*
            Get converged singular triplets: i-th singular value is stored in sigma
            */

            ierr = SVDGetSingularTriplet(svd,i,&sigma,u,v);CHKERRQ(ierr);
            //TODO: multiply each vector by square root sigma
            //VecCreateMPIWithArray(PETSC_COMM_WORLD, bs, ndim, PETSC_DECIDE, cols[i]);
            // assume that dense matrix is stored by column.
            VecCreateSeqWithArray(PETSC_COMM_WORLD, 1, ndim, &UData[i*ndim], &u);
            /*
            Compute the error associated to each singular triplet
            */
            //ierr = SVDComputeError(svd,i,SVD_ERROR_RELATIVE,&error);CHKERRQ(ierr);
        }
        ierr = PetscPrintf(PETSC_COMM_WORLD,"Writing U Matrix\n");CHKERRQ(ierr);
        PetscViewer viewer;
        PetscViewerBinaryOpen(PETSC_COMM_WORLD, outputUFilePath, FILE_MODE_WRITE, &viewer);
        MatView(U,viewer);
        PetscViewerDestroy(&viewer);
        ierr = VecDestroy(&u);CHKERRQ(ierr);
        ierr = VecDestroy(&v);CHKERRQ(ierr);

    }

    /*
    Get number of converged singular triplets
    */
    ierr = SVDGetConverged(svd,&nconv);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_WORLD," Number of converged approximate singular triplets: %D\n\n",nconv);CHKERRQ(ierr);
    pairs.clear();
    cout << "Finished File:" << filename << std::endl;
    /*
    Free work space
    */
    ierr = SVDDestroy(&svd);CHKERRQ(ierr);
    ierr = MatDestroy(&A);CHKERRQ(ierr);
    //ierr = VecDestroy(&u);CHKERRQ(ierr);
    //ierr = VecDestroy(&v);CHKERRQ(ierr);
    ierr = SlepcFinalize();
    //dumpTable(freq_map, outputFilePath, tempFilePath); 
    //}
    return 0;
}

