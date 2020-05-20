# PPMI-SVD 
This is a C++ implementation that performs singular value decomposition on a large sparse matrix. The input is a word-word cooccurrence matrix in parquet format. The application will calculate the positive pointwise mutual information (PPMI) matrix. Then it performs SVD and writes the left singular vectors to a file. The PETSc ecosystem is designed for large sparse matrices. The SLEPc eigenvalue problem solver is implemented on top of PETSc. 

## Requirements:
1. Anaconda

## Installation Instructions
`conda install -c conda-forge gxx_linux-64 arrow-cpp=0.15 cmake gfortran_linux-64 slepc`
## Usage

`-inputFilePath <input path>` <br/><br/>
`-outputFilePath <output path>` <br/><br/>
`-sym ` <br/><br/>
`-alpha <smoothing exponent>`  <br/><br/>
`-svd_nsv <number of singular vectors>` <br/><br/>
