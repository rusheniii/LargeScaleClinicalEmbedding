# INSTALLATION
## Requirements
- arrow-cpp                 0.13.0
- parquet-cpp               1.5.1
- boost-cpp                 1.67.0
- gcc_linux-64              7.3.0
- gxx_linux-64              7.3.0
- tbb-devel                 2019.8

## Instructions 
### 1. create conda environment using python 3.6 or 3.7 (recommended)
` conda create -n main3 python=3.7`
### 2. activate env
` conda activate main3 `
### 3. conda install g++ and parquet dependencies
` conda install gxx_linux-64 arrow-cpp parquet-cpp boost-cpp tbb-devel`

### 4. Make sure the compilers are loaded into your environment
` conda deactivate`
` conda activate main3`

### 5. Use cmake to build the application
#### Make sure you are in the root folder (you shoudl see folders src, tests, etc..)
` mkdir build`
` cd build`
` cmake ..`
` make all`

# Using Enumerate Pairs
## `enumeratePairs [outputFile] [numberOfThreads] [input files] ...`
This tool expects the input to be in parquet format and already sorted by Patient and number of days

The input schema must conform to the following
`PatientOrder (int64), NumDays(int32), WordIndex(int32), window(int32)`