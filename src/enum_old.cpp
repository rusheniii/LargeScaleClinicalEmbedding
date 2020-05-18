
#define _GLIBCXX_USE_CXX11_ABI 1
#include <iostream>
#include <fstream>
#include <thread>
#include <string>
#include <vector>
#include <iterator>
#include <boost/tokenizer.hpp>
#include <boost/format.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
//#include "tbb/concurrent_queue.h"
#include <zlib.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <algorithm>
#include "enumeratePairs.hpp"
#include <math.h>


using namespace boost;
using std::string;



int readParquetFile(string parquetFilePath, std::vector<IJPair> &pairs) {
    // read in parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(parquetFilePath, arrow::default_memory_pool(), &infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
    // expected schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("i", arrow::int64()),
        arrow::field("j",arrow::int64()),
        arrow::field("count",arrow::int64()),
    };
    auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);
    // verify schema
    //if (!expected_schema->Equals(*table->schema())) {
    //    std::cout << "INVALID SCHEMA:" << (*table->schema()).ToString() << std::endl;
    //    return; 
    //arrow::Status::Invalid("Schema doesnt match");
    //}
    auto i = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->data()->chunk(0));
    auto j = std::static_pointer_cast<arrow::Int64Array>(table->column(1)->data()->chunk(0));
    auto count = std::static_pointer_cast<arrow::Int64Array>(table->column(2)->data()->chunk(0));
    int64_t n = 0;
    for (int64_t c=0; c < table->num_rows(); c++) {
        int64_t i_value = i->Value(c);
        int64_t j_value = j->Value(c);
        //if (i_value > 500 ){continue;}
        //if (j_value > 500 ){continue;}
        int32_t count_value = count->Value(c);
        //pairs.emplace_back( (int)i_value+1, (int)j_value+1, count_value);
        if (i_value!=j_value) {
            pairs.emplace_back( (int)i_value, (int)j_value, count_value);
            pairs.emplace_back( (int)j_value, (int)i_value, count_value);
        } else { 
            pairs.emplace_back( (int)i_value, (int)j_value, count_value*2);
        }
        n = std::max(n,i_value);
        n = std::max(n, j_value);
    }
    // add one because we assume there is a row 0
    return n+1;
}

bool compareIJ(IJPair a, IJPair b) {
    if (a.i<b.i){
        return true;
    }
    if (a.i > b.i) {
        return false;
    }
    if (a.j < b.j){
        return true;
    }
    return false;
}

int buildMatrix(std::vector<IJPair> &pairs, Mat *A, int ndim){
    std::cout << "SORTING" <<std::endl;
    std::sort(pairs.begin(), pairs.end(), compareIJ);
    std::cout << "SORTED" <<std::endl;
    std::cout << pairs.size() << std::endl;
    double * csr_a = (double *) calloc(pairs.size(), sizeof(double));
    int * csr_ia = (int*) calloc(ndim+1, sizeof(int));
    int * csr_ja = (int*) calloc(pairs.size(),sizeof(int));
    int nnz =0 ;
    int a_index = 0;
    std::vector<IJPair>::iterator end = pairs.end();
    long long total_pairs = 0;
    long long * wCounts = (long long *) calloc(ndim+1,sizeof(long long));
    for(std::vector<IJPair>::iterator current_pair=pairs.begin(); current_pair<end; current_pair++) {
        total_pairs+=current_pair->count*2;
        wCounts[current_pair->i]+=current_pair->count;
    }

    for(std::vector<IJPair>::iterator current_pair=pairs.begin(); current_pair<end; current_pair++) {
        //std::cout << current_row << " "<< current_pair->j << " " << current_pair->count << std::endl;
        //exit(0);
        csr_a[a_index] = std::max(0.0,log2(current_pair->count * total_pairs / (wCounts[current_pair->i]*pow(wCounts[current_pair->j],.75))));
        csr_ja[a_index] = current_pair->j;
        nnz++;
        csr_ia[current_pair->i+1] = nnz;
        a_index++;
    }
    for (int a=1; a<ndim; a++){
        if (csr_ia[a]==0) {
            csr_ia[a]= csr_ia[a-1];
        }
        //std::cout<< csr_ia[a] << " ";
    }
    PetscErrorCode ierr;
    PetscInt n = ndim;
    PetscInt Istart, Iend;
    //ierr = MatCreate(PETSC_COMM_WORLD,&A);CHKERRQ(ierr);
    //ierr = MatSetSizes(A,PETSC_DECIDE,PETSC_DECIDE,n,n);CHKERRQ(ierr);
    //ierr = MatSetFromOptions(A);CHKERRQ(ierr);
    //ierr = MatSetUp(A);CHKERRQ(ierr);

    //ierr = MatGetOwnershipRange(A,&Istart,&Iend);CHKERRQ(ierr);
    //std::vector<IJPair>::iterator end = pairs.end();
    //int * i =(int*) calloc(pairs.size(), sizeof(int));
    //int *j = (int*) calloc(pairs.size(),sizeof(int));
    //double *count = (double*) calloc(pairs.size(),sizeof(double));;
    //int index = 0;
    //for(std::vector<IJPair>::iterator current_pair=pairs.begin(); current_pair<end; current_pair++) {
    //    i[index]=current_pair->i;;
    //    j[index]=current_pair->j;
    //    count[index] = (double)current_pair->count;
    //    index++;
    //if (current_pair->i >= Istart && current_pair->i < Iend){
    //ierr = MatSetValue(A,current_pair->i,current_pair->j,current_pair->count,INSERT_VALUES);CHKERRQ(ierr);
    //}
    //}
    std::cout << "INSERTING VALUES" << std::endl;
    ierr = MatCreateSeqAIJWithArrays(PETSC_COMM_WORLD,ndim, ndim, csr_ia, csr_ja, csr_a,A);CHKERRQ(ierr);
    std::cout << "INSERTED VALUES" << std::endl;
    ierr = MatSetOption(*A, MAT_SYMMETRIC, PETSC_TRUE);CHKERRQ(ierr);
    //ierr = MatAssemblyBegin(A,MAT_FINAL_ASSEMBLY);CHKERRQ(ierr);
    //ierr = MatAssemblyEnd(A,MAT_FINAL_ASSEMBLY);CHKERRQ(ierr);
    return 0;
}

// -mat_is_symmetric
// -svd_type <now cross : formerly cross>: SVD solver method (one of) cross cyclic lapack lanczos trlanczos primme (SVDSetType)
// -svd_cross_explicitmatrix FALSE/TRUE
// -svd_cross_eps_type <now krylovschur : formerly krylovschur>: Eigensolver method (one of) krylovschur power subspace arnoldi lanczos gd jd rqcg lobpcg ciss lapack primme (EPSSetType)
/*
  Pick at most one of -------------
    -svd_cross_eps_hermitian: Hermitian eigenvalue problem (EPSSetProblemType)
    -svd_cross_eps_gen_hermitian: Generalized Hermitian eigenvalue problem (EPSSetProblemType)
    -svd_cross_eps_non_hermitian: Non-Hermitian eigenvalue problem (EPSSetProblemType)
    -svd_cross_eps_gen_non_hermitian: Generalized non-Hermitian eigenvalue problem (EPSSetProblemType)
    -svd_cross_eps_pos_gen_non_hermitian: Generalized non-Hermitian eigenvalue problem with positive semi-definite B (EPSSetProblemType)
    -svd_cross_eps_gen_indefinite: Generalized Hermitian-indefinite eigenvalue problem (EPSSetProblemType)
  Pick at most one of -------------
    -svd_cross_eps_ritz: Rayleigh-Ritz extraction (EPSSetExtraction)
    -svd_cross_eps_harmonic: Harmonic Ritz extraction (EPSSetExtraction)
    -svd_cross_eps_harmonic_relative: Relative harmonic Ritz extraction (EPSSetExtraction)
    -svd_cross_eps_harmonic_right: Right harmonic Ritz extraction (EPSSetExtraction)
    -svd_cross_eps_harmonic_largest: Largest harmonic Ritz extraction (EPSSetExtraction)
    -svd_cross_eps_refined: Refined Ritz extraction (EPSSetExtraction)
    -svd_cross_eps_refined_harmonic: Refined harmonic Ritz extraction (EPSSetExtraction)
*/