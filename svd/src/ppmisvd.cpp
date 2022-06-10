
#include <iostream>
#include <fstream>
#include <thread>
#include <iterator>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <boost/sort/sort.hpp>
#include <algorithm>
#include <math.h>
#include "ppmisvd.hpp"


using std::string;



MatrixInfo readParquetFile(string parquetFilePath, std::vector<IJPair> &pairs, PetscBool isSymmetric, PetscMPIInt worldRank, PetscMPIInt worldSize, long long int * &wCounts, PetscInt n) {
    // read in parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    auto result = arrow::io::ReadableFile::Open(parquetFilePath, arrow::default_memory_pool());
    if (result.ok()){
        infile = result.ValueOrDie();
    }
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    int num_row_groups = reader->num_row_groups();
    reader->set_use_threads(true);
    // expected schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("i", arrow::int32()),
        arrow::field("j",arrow::int32()),
        arrow::field("count",arrow::int64()),
    };
    auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);
    std::shared_ptr<arrow::Schema> actual_schema;
    reader->GetSchema(&actual_schema);
    // verify schema
    // don't check the metadata though
    if (!expected_schema->Equals(*actual_schema,false)) {
        std::cout << "INVALID SCHEMA:" << actual_schema->ToString() << std::endl;
        arrow::Status::Invalid("Schema doesnt match");
        MatrixInfo mi = {-1,-1,-1};
        return mi;
    }
    int step = n / worldSize;
    int remainder = n % step;
    int start = step* worldRank, end = start+step;
    if (worldRank == worldSize-1){
        end+=remainder;
    } 
    wCounts = (long long *) calloc(n, sizeof(long long));
    std::vector<IJPair> allPairs;
    for (int r =0; r < num_row_groups; r++){
        std::shared_ptr<arrow::Table> table;
        reader->ReadRowGroup(r,  &table);
        auto i = std::static_pointer_cast<arrow::Int32Array>(table->column(0)->chunk(0));
        auto j = std::static_pointer_cast<arrow::Int32Array>(table->column(1)->chunk(0));
        auto count = std::static_pointer_cast<arrow::Int64Array>(table->column(2)->chunk(0));

        for (int64_t c=0; c < table->num_rows(); c++) {
            int i_value = (int) i->Value(c);
            int j_value = (int) j->Value(c);
            long long int count_value = (long long int) count->Value(c);
            if (isSymmetric) {
                if (i_value!=j_value) {
                    IJPair pair1 = {i_value, j_value, count_value};
                    IJPair pair2 = {j_value, i_value, count_value};
                    if (pair1.i >= start && pair1.i < end) {
                        pairs.push_back(pair1);
                    }
                    if (pair2.i >= start && pair2.i < end) {
                        pairs.push_back(pair2);
                    }
		    wCounts[pair1.i] += pair1.count;
		    wCounts[pair2.i] += pair2.count;
                } else {
                    IJPair pair = {i_value, j_value, count_value*2};
                    if (pair.i >= start && pair.i < end) {
                        pairs.push_back(pair);
                    }
		    wCounts[pair.i] += pair.count;
                }
            } else {
                IJPair pair = {i_value,j_value,count_value};         
                if (pair.i >= start && pair.i < end) {
                    pairs.push_back(pair);
                }
                wCounts[pair.i] += pair.count;
            }
        }

    }


    MatrixInfo mi = {start, end, n};
    return mi;
}

int buildMatrix(std::vector<IJPair> &pairs, Mat *A, MatrixInfo mi, PetscScalar alpha, long long int * wCounts){
    int ndim = mi.ndim;
    int start = mi.start;
    int stop = mi.end;
    boost::sort::block_indirect_sort(pairs.begin(), pairs.end());
    double * csr_a = (double *) calloc(pairs.size(), sizeof(double));
    PetscInt * csr_ia = (PetscInt*) calloc(stop-start+1, sizeof(PetscInt));
    PetscInt * csr_ja = (PetscInt*) calloc(pairs.size(),sizeof(PetscInt));
    PetscInt nnz =0 ;
    size_t a_index = 0;
    std::vector<IJPair>::iterator end = pairs.end();
    // |D|, the total number of pairs in D when alpha = 1. otherwise sum(#(c)^alpha). 
    double total_pairs = 0; 
    // Context distribution smoothing
    // calculate sum(#(c)^alpha) for every c in V_c
    for(size_t i = 0; i < ndim+1; i++){
        total_pairs+= pow( (double)wCounts[i],alpha);
    }

    for(std::vector<IJPair>::iterator current_pair=pairs.begin(); current_pair<end; current_pair++) {
        // calculate pmi
        csr_a[a_index] = log2(current_pair->count * total_pairs / (wCounts[current_pair->i]*pow( (double)wCounts[current_pair->j], alpha)));
        // calculate ppmi
        csr_a[a_index] = std::max(0.0, csr_a[a_index]);
        csr_ja[a_index] = (PetscInt) current_pair->j;
        nnz++;
        csr_ia[(current_pair->i) - start + 1] = nnz;
        a_index++;
    }
    for (int a=1; a<stop-start+1; a++){
        if (csr_ia[a]==0) {
            csr_ia[a]= csr_ia[a-1];
        }
    }
    PetscErrorCode ierr;
    //free(wCounts);
    ierr = MatCreateMPIAIJWithArrays(PETSC_COMM_WORLD, stop-start, stop-start, ndim, ndim, csr_ia, csr_ja, csr_a, A);CHKERRQ(ierr);
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
