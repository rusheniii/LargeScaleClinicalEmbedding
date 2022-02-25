
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
#include <tbb/concurrent_hash_map.h>
#include <boost/functional/hash.hpp>


typedef std::pair<int, int> KeyType;

struct BoostHashCompare {
    static size_t hash( const KeyType &key ) {
        return static_cast<size_t>(boost::hash_value(key));
    }
    static bool equal( const KeyType &key1, const KeyType &key2) {
        return key1.first==key2.first && key1.second==key2.second;
    }
};

typedef tbb::concurrent_hash_map<KeyType, long long int, BoostHashCompare> Table;

class CodeWord{
public:
    int word_index, source;
    std::string code;

    CodeWord(int word_index, int source, std::string code) {
        this->word_index = word_index;
        this->source = source;
        this->code = code;
    }
};

void readCodesFile(std::string parquetFilePath, std::vector<CodeWord> &codeWords) {
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
        arrow::field("WordIndex",arrow::int64()),
        arrow::field("Source",arrow::int64()),
        arrow::field("Code", arrow::utf8())
    };
    auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);
    // verify schema don't check metadata
    if (!expected_schema->Equals(*table->schema(), false)) {
        std::cout << "INVALID SCHEMA:" << (*table->schema()).ToString() << std::endl;
        std::cout << "DOESN'T MATCH TARGET SCHEMA:" << (*expected_schema).ToString() << std::endl;
        //arrow::Status::Invalid("Schema doesnt match");
    }
    auto wordIndex = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    auto source = std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0));
    auto codeChunk = std::static_pointer_cast<arrow::StringArray>(table->column(2)->chunk(0));

    for (int64_t i =0; i < table->num_rows(); i++) {
        int32_t word_index = (int32_t) wordIndex->Value(i);
        int32_t source_id = (int32_t)source->Value(i);
        std::string code = codeChunk->GetString(i);
        codeWords.emplace_back( word_index, source_id, code);
    }
    return;
}


void readPairsFile(std::string parquetFilePath,
std::unordered_map<int, std::string>& index2code,
std::unordered_map<int, unsigned char> index2src,
std::unordered_map<std::string, int>& code2newindex,
Table& reindexed_mat, int threadId, int totalThreads) {
    // read in parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(parquetFilePath, arrow::default_memory_pool(), &infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::OpenFile(parquetFilePath, false);
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    int num_row_groups = file_metadata->num_row_groups();
    parquet_reader->Close();
    reader->set_use_threads(false);
    for (int rg=threadId; rg<num_row_groups; rg+=totalThreads){
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->RowGroup(rg)->ReadTable(&table));
        // expected schema
        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("i",arrow::int32()),
            arrow::field("j",arrow::int32()),
            arrow::field("count", arrow::int64())
        };
        auto i = std::static_pointer_cast<arrow::Int32Array>(table->column(0)->chunk(0));
        auto j = std::static_pointer_cast<arrow::Int32Array>(table->column(1)->chunk(0));
        auto count = std::static_pointer_cast<arrow::Int64Array>(table->column(2)->chunk(0));
        Table::accessor a;
        for (int64_t c=0; c < table->num_rows(); c++) {
            int i_value = (int) i->Value(c);
            int j_value = (int) j->Value(c);
            long long int count_value = (long long int) count->Value(c);
            auto i_code = index2code.find(i_value);
            auto j_code = index2code.find(j_value);
            if (i_code == index2code.end() || j_code == index2code.end()){
                continue;
            }
            auto i_src = index2src.find(i_value);
            auto j_src = index2src.find(j_value);
            if (i_src == index2src.end() || j_src == index2src.end()){
                continue;
            }
            int is = i_src->second;
            int js = j_src->second;
            if ( (is == 1 || is==2 || is==5) && (js == 1 || js == 2 || js == 5) ){
                // pass
            } else {
                continue;
            }

            KeyType num;
            int newi= code2newindex[i_code->second];
            int newj = code2newindex[j_code->second];
            if (newi < newj) {
                num = KeyType(newi, newj);
            } else { num = KeyType(newj, newi); }

            bool missing = reindexed_mat.insert(a, num);
            if (missing) {
                a->second=0;
            }
            a->second+=count_value;
            a.release();
        }
    }

    return;
}
void writeRowGroup(std::vector<int32_t> &iVector, std::vector<int32_t> &jVector, std::vector<int64_t> &countVector, std::unique_ptr<parquet::arrow::FileWriter> &fileWriter) {
    arrow::Int32Builder i, j;
    arrow::Int64Builder count;
    std::shared_ptr<arrow::Array> iArray, jArray, countArray;
    i.AppendValues(iVector);
    i.Finish(&iArray);
    j.AppendValues(jVector);
    j.Finish(&jArray);
    count.AppendValues(countVector);
    count.Finish(&countArray);
    fileWriter->NewRowGroup(iVector.size());
    fileWriter->WriteColumnChunk(*iArray);
    fileWriter->WriteColumnChunk(*jArray);
    fileWriter->WriteColumnChunk(*countArray);
}

void dumpTable( Table &pairs, std::string outPath){
    std::shared_ptr<arrow::io::FileOutputStream> outFile;
    arrow::io::FileOutputStream::Open(outPath, &outFile);

    std::shared_ptr<arrow::Schema> schema = arrow::schema( {arrow::field("i", arrow::int32()),
        arrow::field("j", arrow::int32()),
        arrow::field("count", arrow::int64())
    });
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    std::unique_ptr<parquet::arrow::FileWriter> fileWriter;
    parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), outFile, props, &fileWriter);
    std::vector<int32_t> i, j;
    std::vector<int64_t> count;
    for (auto it=pairs.begin(); it!=pairs.end(); it++){
        KeyType key = it->first;
        long long int value = it->second;
        i.push_back( (int32_t) key.first );
        j.push_back( (int32_t) key.second);
        count.push_back( (int64_t) value);
        if (i.size()>=30000){
            writeRowGroup(i,j,count,fileWriter);
            i.clear();
            j.clear();
            count.clear();
        }
    }
    if (i.size() > 0) {
        writeRowGroup(i, j, count, fileWriter);
    }
    fileWriter->Close();
}


int main(void){
    std::vector<CodeWord> codes;
    readCodesFile("word_mapping.parquet",codes);
    std::unordered_map<int, std::string> index2code;
    std::unordered_map<int, unsigned char> index2src;
    std::set<std::string> codeset;
    for (auto it = codes.begin(); it != codes.end(); it++){
        index2code[it->word_index] = it->code;
        index2src[it->word_index] = (unsigned char) it->source;
        codeset.insert(it->code);
    }
    std::cout << "CODES:" << index2code.size() << " CODESET:" << codeset.size() << std::endl;
    std::unordered_map<std::string, int> code2newindex;
    int i =0;
    for (auto it = codeset.begin(); it!=codeset.end(); it++){
        code2newindex[*it] = i;
        i++;
    }
    Table reindexed_mat;
    // read code mapping file
    std::vector<std::thread> threads;
    int thread_num = 10;
    for (int i =0; i<thread_num; i++) {
        // read parquet and remap each pair and increase count
        threads.emplace_back(readPairsFile, "COOCCURRENCE_MATRIX_SYMMETRIC_12_17.parquet", std::ref(index2code), \
        std::ref(index2src), std::ref(code2newindex), std::ref(reindexed_mat), i, thread_num);
    }
    for (int i=0; i<thread_num; i++) {
        threads[i].join();
    }
    dumpTable(reindexed_mat, "COOCCURRENCE_MATRIX_REDUCED.parquet");
    return 0;

}
