
#include <iostream>
#include <fstream>
#include <iterator>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <boost/sort/sort.hpp>
#include <algorithm>
#include "ppmisvd.hpp"
#include <math.h>


using std::string;



int readParquetFile(string parquetFilePath, std::vector<IJPair> &pairs, PetscBool isSymmetric=PETSC_FALSE) {
    // read in parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(parquetFilePath, arrow::default_memory_pool(), &infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    reader->set_use_threads(true);
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
    // expected schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("i", arrow::int32()),
        arrow::field("j",arrow::int32()),
        arrow::field("count",arrow::int64()),
    };
    auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);
    // verify schema
    // don't check the metadata though
    if (!expected_schema->Equals(*table->schema(),false)) {
        std::cout << "INVALID SCHEMA:" << (*table->schema()).ToString() << std::endl;
        arrow::Status::Invalid("Schema doesnt match"); 
        return -1;
    }
    auto i = std::static_pointer_cast<arrow::Int32Array>(table->column(0)->chunk(0));
    auto j = std::static_pointer_cast<arrow::Int32Array>(table->column(1)->chunk(0));
    auto count = std::static_pointer_cast<arrow::Int64Array>(table->column(2)->chunk(0));
    int n = 0;
    for (int64_t c=0; c < table->num_rows(); c++) {
        int i_value = (int) i->Value(c);
        int j_value = (int) j->Value(c);
        long long int count_value = (long long int) count->Value(c);
        if (isSymmetric) {
            if (i_value!=j_value) {
                IJPair pair1 = {i_value, j_value, count_value};
                IJPair pair2 = {j_value, i_value, count_value};
                pairs.emplace_back( pair1 );
                pairs.emplace_back( pair2 );
            } else { 
                IJPair pair = {i_value, j_value, count_value*2};
                pairs.emplace_back( pair);
            }
        } else {
            IJPair pair = {i_value, j_value, count_value};
            pairs.emplace_back( pair );
        }
        
        n = std::max(n,i_value);
        n = std::max(n, j_value);
    }
    // add one because we assume there is a row 0
    return n+1;
}

int buildMatrix(std::vector<IJPair> &pairs, Mat *A, int ndim, PetscScalar alpha){
    boost::sort::block_indirect_sort(pairs.begin(), pairs.end());
    double * csr_a = (double *) calloc(pairs.size(), sizeof(double));
    int * csr_ia = (int*) calloc(ndim+1, sizeof(int));
    int * csr_ja = (int*) calloc(pairs.size(),sizeof(int));
    int nnz =0 ;
    int a_index = 0;
    std::vector<IJPair>::iterator end = pairs.end();
    long long total_pairs = 0; // |D|, the number of pairs in D
    long long * wCounts = (long long *) calloc(ndim+1,sizeof(long long)); // SUM(w, c')
    for(std::vector<IJPair>::iterator current_pair=pairs.begin(); current_pair<end; current_pair++) {
        total_pairs+=current_pair->count;
        wCounts[current_pair->i]+=current_pair->count;
    }

    for(std::vector<IJPair>::iterator current_pair=pairs.begin(); current_pair<end; current_pair++) {
        // calculate pmi
        csr_a[a_index] = log2(current_pair->count * pow(total_pairs, alpha) / (wCounts[current_pair->i]*pow(wCounts[current_pair->j], alpha)));
        // calculate ppmi
        csr_a[a_index] = std::max(0.0, csr_a[a_index]);
        csr_ja[a_index] = current_pair->j;
        nnz++;
        csr_ia[current_pair->i+1] = nnz;
        a_index++;
    }
    for (int a=1; a<ndim+1; a++){
        if (csr_ia[a]==0) {
            csr_ia[a]= csr_ia[a-1];
        }
    }
    PetscErrorCode ierr;
    //PetscInt n = ndim;
    //PetscInt Istart, Iend;
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

    ierr = MatCreateSeqAIJWithArrays(PETSC_COMM_SELF,ndim, ndim, csr_ia, csr_ja, csr_a, A);CHKERRQ(ierr);


    //ierr = MatSetOption(*A, MAT_SYMMETRIC, PETSC_TRUE);CHKERRQ(ierr);
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
