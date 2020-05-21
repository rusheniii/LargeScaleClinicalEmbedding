#include <iostream>
#include <fstream>
#include <thread>
#include <string>
#include <vector>
#include <iterator>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "enumeratePairs.hpp"


using std::string;



void readParquetFile(string parquetFilePath, std::vector<MedicalEvent> &events) {
    // read in parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(parquetFilePath, arrow::default_memory_pool(), &infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
    // expected schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("PatientOrder", arrow::int64()),
        arrow::field("NumDays",arrow::int32()),
        arrow::field("WordIndex",arrow::int32()),
        arrow::field("window",arrow::int32())
    };
    auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);
    // verify schema don't check metadata
    if (!expected_schema->Equals(*table->schema(), false)) {
        std::cout << "INVALID SCHEMA:" << (*table->schema()).ToString() << std::endl;
        std::cout << "DOESN'T MATCH TARGET SCHEMA:" << (*expected_schema).ToString() << std::endl;
        return; 
        //arrow::Status::Invalid("Schema doesnt match");
    }
    auto patientOrder = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    auto numDays = std::static_pointer_cast<arrow::Int32Array>(table->column(1)->chunk(0));
    auto wordIndex = std::static_pointer_cast<arrow::Int32Array>(table->column(2)->chunk(0));
    for (int64_t i =0; i < table->num_rows(); i++) {
        int64_t patient_order = patientOrder->Value(i);
        int32_t num_days = numDays->Value(i);
        int32_t word_index = wordIndex->Value(i);
        events.emplace_back( (int)patient_order, num_days, word_index);
    }
    return;
}

void generatePairs( Table &pairs, std::vector<MedicalEvent> &events, int threadId, int totalThreads, int window, std::atomic<int> &rowCounter) {
    std::vector<MedicalEvent>::iterator end = events.end()--;
    for (std::vector<MedicalEvent>::iterator i=events.begin()+threadId; i<end; i+=totalThreads) {
        int psid = i->psid;
        int numDays = i->numDays;
        int word = i->word;
        Table::accessor a;
        for (std::vector<MedicalEvent>::iterator j = i+1; j< events.end(); j++) {
            if (psid != j->psid) { break;}
            if (j->numDays - numDays > window) { break;}
            KeyType num;
            if (word < j->word) {
            num = KeyType(word, j->word);
            } else { num = KeyType(j->word, word); }
            bool missing = pairs.insert(a, num);
            if (missing) {
                a->second=0;
            }
            a->second+=1;
            a.release();
        }
        rowCounter++;
    }
}

void dumpTable( Table &pairs, string outPath){
    arrow::Int32Builder i,j;
    arrow::Int64Builder count;
    for (auto it = pairs.begin(); it!=pairs.end(); it++) {
        KeyType key = it->first;
        long long int value = it->second;
        i.Append(key.first);j.Append(key.second);count.Append(value);
    }
    std::shared_ptr<arrow::Array> iArray,jArray, countArray;
    i.Finish(&iArray);
    j.Finish(&jArray);
    count.Finish(&countArray);
    std::shared_ptr<arrow::Schema> schema = arrow::schema( {arrow::field("i",arrow::int32()), 
    arrow::field("j",arrow::int32()), 
    arrow::field("count",arrow::int64())} );
    std::shared_ptr<arrow::Table> arrowTable = arrow::Table::Make(schema, {iArray, jArray, countArray});
    std::shared_ptr<arrow::io::FileOutputStream> outFile;
    arrow::io::FileOutputStream::Open(outPath, &outFile);
    parquet::arrow::WriteTable(*arrowTable, arrow::default_memory_pool(), outFile, 10000);
}
