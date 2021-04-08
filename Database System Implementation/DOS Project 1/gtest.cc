#include "DBFile.h"
#include "Schema.h"
#include <string.h>
#include <gtest/gtest.h>


class FilePath{
    public:
    //file path where all dbfiles(.bin) are stored , Update the path accordingly as per machine setup
    const std::string dbBinFilesPath = "/cise/tmp/dbfiles/"; 
    //file path where all tpch-dbgen table files (.tbl) are stored , Update the path accordingly as per machine setup
    const std::string tpchTblFilesPath ="/cise/tmp/tpch-dbgen/"; 
    //file path where catalog file is present
    const std::string catalogFilePath = "catalog"; 
};


TEST(DBFile_CreateMethod, TestCase1) {
    // TestCase1: Create a file which does not exist , on successful creation create function returns 1
    FilePath filepath;
    DBFile dbfile;
    const string heapFileSuffix = "region.bin";
    const string dbFilePath = filepath.dbBinFilesPath + heapFileSuffix;
    ASSERT_EQ(1,dbfile.Create(dbFilePath.c_str(),heap,NULL));
    dbfile.Close();
}

TEST(DBFile_CreateMethod, TestCase2) {
    // TestCase2: try to Create a heap file which already exists in bin file director , create function in this case does not create any file and returns 0
    FilePath filepath;
    DBFile dbfile;
    const string heapFileSuffix = "region.bin";
    const string dbFilePath = filepath.dbBinFilesPath + heapFileSuffix;
    ASSERT_EQ(0,dbfile.Create(dbFilePath.c_str(),heap,NULL));
}

TEST(DBFile_OpenMethod, TestCase1) {
    // TestCase1: try to open a heap bin file which does not exists in db bin file directory , open function in this case will not open any file returns 0
    FilePath filepath;
    DBFile dbfile;
    const string existentFile = "region.bin";
    const string nonExistentFile = "xyz.bin";
    const string existentPath = filepath.dbBinFilesPath + existentFile;
    const string nonExistentPath = filepath.dbBinFilesPath + nonExistentFile;
    ASSERT_EQ(0,dbfile.Open(nonExistentPath.c_str()));

}

TEST(DBFile_OpenMethod, TestCase2) {
    // TestCase2: try to open a heap bin file which exists in db bin file directory , open function in this case will open  file and return 1
    FilePath filepath;
    DBFile dbfile;
    const string existentFile = "region.bin";
    const string nonExistentFile = "xyz.bin";
    const string existentPath = filepath.dbBinFilesPath + existentFile;
    const string nonExistentPath = filepath.dbBinFilesPath + nonExistentFile;
    ASSERT_EQ(1,dbfile.Open(existentPath.c_str()));

}

TEST(DBFile_OpenMethod, TestCase3) {
    // TestCase3: try to open a heap bin file which exists and already open , open function in this case returns 1
    FilePath filepath;
    DBFile dbfile;
    const string existentFile = "region.bin";
    const string nonExistentFile = "xyz.bin";
    const string existentPath = filepath.dbBinFilesPath + existentFile;
    const string nonExistentPath = filepath.dbBinFilesPath + nonExistentFile;
    ASSERT_EQ(1,dbfile.Open(existentPath.c_str()));
    dbfile.Close();
}

TEST(DBFile_CloseMethod, TestCase1) {
    // scenario 1: try to close a file which is not open n in that case close function will return 0
    FilePath filepath;
    DBFile dbfile;
    const string heapFileSuffix = "region.bin";
    const string completePath = filepath.dbBinFilesPath + heapFileSuffix;
    ASSERT_EQ(0,dbfile.Close());
    // // scenario 2: try to close a file which is open and in that case close function will return 1
    dbfile.Open(completePath.c_str());
    ASSERT_EQ(1,dbfile.Close());    
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}