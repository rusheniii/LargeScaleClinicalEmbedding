# PPMI-SVD 
This is a C++ implementation that performs singular value decomposition on a large sparse matrix. The input is a word-word cooccurrence matrix in parquet format. The application will calculate the positive pointwise mutual information (PPMI) matrix. Then it performs SVD and writes the left singular vectors to a file. The PETSc ecosystem is designed for large sparse matrices. The SLEPc eigenvalue problem solver is implemented on top of PETSc. 

## Requirements:
1. Anaconda

## Installation Instructions
`conda install -c conda-forge gxx_linux-64 arrow-cpp=0.15 cmake gfortran_linux-64 slepc=3.12` <br/><br/>
`mkdir build` <br/><br/>
`cd build` <br/><br/>
`cmake ..` <br/><br/>
`make -j 5` <br/><br/>
These steps should compile the application.
## Usage

`-inputFilePath <input path>`: This is the filepath to a parquet file with schema: i (int32),j (int32), count (int64) <br/><br/>
`-outputFilePath <output path>`: This is the location to write the left singular vectors as a PETSc Matrix <br/><br/>
`-sym `: For each i,j pair in input file, this flag also adds pair j,i into the matrix. (ie the input is stored as a triangular matrix but should be symmetric).<br/><br/>
`-alpha <smoothing parameter>`: This is context distributional smoothing. <br/><br/>
`-svd_nsv <number of singular vectors>`: This specifies the number of singular vectors to find. <br/><br/>
