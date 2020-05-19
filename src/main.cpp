
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
    PetscInt       nsv,maxit,its,nconv;
    PetscBool helpOption, isSymmetricOption, flg;
    PetscScalar alpha;
    int MAXSTRINGLENGTH = 255;
    char filename[MAXSTRINGLENGTH], outputUFilePath[MAXSTRINGLENGTH];
    PetscOptionsBegin(PETSC_COMM_SELF,"","PPMI SVD Options","none");
    PetscOptionsString("-inputFilePath","Parquet file with schema: i,j,count","",filename, filename, MAXSTRINGLENGTH,&flg);
    PetscOptionsString("-outputFilePath","This will write the U matrix to file","",outputUFilePath, outputUFilePath, MAXSTRINGLENGTH,&flg);
    helpOption = PETSC_FALSE;
    PetscOptionsBool("-help", "Display Help Menu", "", helpOption, &helpOption, NULL);
    isSymmetricOption = PETSC_FALSE;
    PetscOptionsBool("-sym", "Has the input file stored the symmetric matrix as triangular matrix?", "", isSymmetricOption, &isSymmetricOption, NULL);
    alpha = 0.75;
    PetscOptionsScalar("-alpha","The exponent for smoothing PPMI", "", alpha, &alpha, NULL);
    PetscOptionsEnd();
    if (helpOption) {
        ierr = MatCreate(PETSC_COMM_SELF,&A);CHKERRQ(ierr);
        ierr = MatSetFromOptions(A);CHKERRQ(ierr);
        ierr = SVDCreate(PETSC_COMM_SELF,&svd);CHKERRQ(ierr);
        ierr = SVDSetFromOptions(svd);CHKERRQ(ierr);
        return 0;
    }
    std::string inputFilePath(filename);
    //for (int n = 1; n < argc; n++) {
    cout << "Reading File:" << inputFilePath<< " "<<helpOption << std::endl;
    int ndim = readParquetFile(inputFilePath, pairs, isSymmetricOption);
    buildMatrix(pairs, &A, ndim,alpha);
    pairs.clear();

    /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Create the singular value solver and set various options
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
    /*
    Create singular value solver context
    */
    ierr = SVDCreate(PETSC_COMM_SELF,&svd);CHKERRQ(ierr);
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
    ierr = PetscPrintf(PETSC_COMM_SELF," Number of iterations of the method: %D\n",its);CHKERRQ(ierr);
    /*
    Get some information from the solver and display it
    */
    ierr = SVDGetType(svd,&type);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_SELF," Solution method: %s\n\n",type);CHKERRQ(ierr);
    ierr = SVDGetDimensions(svd,&nsv,NULL,NULL);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_SELF," Number of requested singular values: %D\n",nsv);CHKERRQ(ierr);
    ierr = SVDGetTolerances(svd,&tol,&maxit);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_SELF," Stopping condition: tol=%.4g, maxit=%D\n",(double)tol,maxit);CHKERRQ(ierr);
    ierr = SVDGetConverged(svd,&nconv);CHKERRQ(ierr);
    ierr = PetscPrintf(PETSC_COMM_SELF," Number of converged approximate singular triplets: %D\n\n",nconv);CHKERRQ(ierr);
    /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Display solution and clean up
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
    if (nconv> 0){
        std::vector<Vec> U_columns;
        Vec u,v;
        ierr = MatCreateVecs(A,&v,&u);CHKERRQ(ierr);
        ierr = PetscPrintf(PETSC_COMM_SELF,"Creating U Matrix\n");CHKERRQ(ierr);
        Mat U;
        PetscScalar * UData=0, * uiData = 0;
        //MatCreate(PETSC_COMM_WORLD, &U);
        //MatCreate(PETSC_COMM_SELF, &U);
        MatCreateDense(PETSC_COMM_SELF, ndim, nconv, ndim, nconv, NULL, &U);
        MatDenseGetArray(U,&UData);
        //MatSetSizes(U,ndim,nconv, ndim, nconv);
        //MatSetType(U,MATDENSE);
        //PetscMalloc1(ndim*nconv,&UData);
        //MatSeqDenseSetPreallocation(U,UData);
        //MatAssemblyBegin(U,MAT_FINAL_ASSEMBLY);
        //MatAssemblyEnd(U,MAT_FINAL_ASSEMBLY);

        ierr = PetscPrintf(PETSC_COMM_SELF,"Initializing U Matrix\n");CHKERRQ(ierr);
        for (int i=0;i<nconv;i++) {
            /*
            Get converged singular triplets: i-th singular value is stored in sigma
            */
            ierr = SVDGetSingularTriplet(svd,i,&sigma,u,v);CHKERRQ(ierr);
            //TODO: multiply each vector by square root sigma
            //VecCreateMPIWithArray(PETSC_COMM_WORLD, bs, ndim, PETSC_DECIDE, cols[i]);
            // assume that dense matrix is stored by column.
            //VecCreateSeqWithArray(PETSC_COMM_SELF, 1, ndim, &UData[i*ndim], &u);
            //VecCreateSeqWithArray(PETSC_COMM_SELF, 1, ndim, UData, &u);
            VecGetArray(u,&uiData);
            PetscMemcpy(&UData[i*ndim],uiData,ndim*(sizeof(PetscScalar)));
            /*
            Compute the error associated to each singular triplet
            */
            //ierr = SVDComputeError(svd,i,SVD_ERROR_RELATIVE,&error);CHKERRQ(ierr);
        }
        ierr = PetscPrintf(PETSC_COMM_SELF,"Writing U Matrix\n");CHKERRQ(ierr);
        PetscViewer viewer;
        PetscViewerBinaryOpen(PETSC_COMM_SELF, outputUFilePath, FILE_MODE_WRITE, &viewer);
        MatView(U,viewer);
        PetscViewerDestroy(&viewer);
        ierr = VecDestroy(&u);CHKERRQ(ierr);
        ierr = VecDestroy(&v);CHKERRQ(ierr);

    }

    /*
    Free work space
    */
    ierr = SVDDestroy(&svd);CHKERRQ(ierr);
    ierr = MatDestroy(&A);CHKERRQ(ierr);
    ierr = SlepcFinalize();
    return 0;
}

