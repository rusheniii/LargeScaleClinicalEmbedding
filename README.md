# Large Scale Clinical Embeddings
This pipeline for creating clinical embeddings contains two steps. 
1. Count the frequency of cooccurring medical concepts using a sliding 30 day window. 
2. Perform singular value decomposition on the concept-concept matrix.

![PipelineArchitecture][https://github.com/rusheniii/LargeScaleClinicalEmbedding/CodeEmbeddingArchitecture.png]

## Requirements:
1. Anaconda
2. Linux

## Installation Instructions
`conda install -c conda-forge gxx_linux-64 arrow-cpp=0.15 cmake gfortran_linux-64 slepc=3.12 tbb-devel=2020.0` <br/><br/>
`mkdir build` <br/><br/>
`cd build` <br/><br/>
`cmake ..` <br/><br/>
`make -j 5` <br/><br/>
These steps should compile both applications. The binaries are in a directory named `bin`.
## Usage
### `enumeratePairs [outputFile] [numberOfThreads] [input files] ...`
This program using Intel's TBB to count the frequency of cooccurring medical concepts using a sliding 30 day window. 
The program expects the input to be in parquet format and already sorted by Patient and number of days. <br/><br/>

The input schema must conform to the following
`PatientOrder (int64), NumDays(int32), WordIndex(int32), window(int32)` <br/><br/>
### `ppmisvd -inputFilePath <path> -outputFilePath <path>`

This a C++ implementation that performs singular value decomposition of a large sparse matrix using SLEPc and PETSc. 
The input is a word-word cooccurrence matrix in parquet format. The application will calculate the positive pointwise mutual information (PPMI) matrix. 
Then it performs SVD and writes the left singular vectors to a file. The PETSc ecosystem is designed for large sparse matrices. 
The SLEPc eigenvalue problem solver is implemented on top of PETSc. 

`-help` or `-h` : This will display the help menu and provides a full list of options. <br/><br/>
`-inputFilePath <input path>`: This is the filepath to a parquet file with schema: i (int32),j (int32), count (int64) <br/><br/>
`-outputFilePath <output path>`: This is the location to write the left singular vectors as a PETSc Matrix <br/><br/>
`-sym `: For each i,j pair in input file, this flag also adds pair j,i into the matrix. (ie the input is stored as a triangular matrix but should be symmetric).<br/><br/>
`-eigenWeight` : should the left signular vectors be scaled by square root of sigma? <br/><br/>
`-alpha <smoothing parameter>`: This is context distributional smoothing. <br/><br/>
`-svd_nsv <number of singular vectors>`: This specifies the number of singular vectors to find. <br/><br/>
`-svd_type` : SVD solver method (one of) cross cyclic lapack lanczos trlanczos.  <br/><br/>
`-svd_largest` or `-svd_smallest`:  Compute largest or smallest singular values. <br/><br/>
