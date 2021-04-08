#include "test.h"
#include "BigQ.h"
#include "Utilities.h"
#include <string.h>
#include <gtest/gtest.h>


// -------------------------------------------------------------------------------------------------------------

//Utility class with methods used in below test cases
class TestUtilities{
    public:
    const std::string dbfile_dir = "dbfiles/"; 
    const std::string tpch_dir ="tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
    static OrderMaker sortorder;
    void getSortOrderFromCNF (OrderMaker &sortorder) {
         cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
        if (yyparse() != 0) {
            cout << "Can't parse your sort CNF.\n";
            exit (1);
        }
        cout << " \n";
        Record recordLiteral;
        CNF cnfObj;
        cnfObj.GrowFromParseTree (final, new Schema ("catalog","nation"), recordLiteral);
        OrderMaker orderMaker;
        cnfObj.GetSortOrders (sortorder, orderMaker);
        // TestUtilities::sortorder=sortorder;
    }

    void deleteFileOnFilePath(string filePathDir) {
             if(Utilities::checkfileExist(filePathDir.c_str())) {
                if( remove(filePathDir.c_str())!= 0) {
                    cerr<< "Error deleting file";   
                }
            }
    }
};
// -------------------------------------------------------------------------------------------------------------
TEST(WriteRunToFile,WriteRunToFile) {
        TestUtilities tu;
        DBFile dbFileObj;
        Record recordObj;
        const string binFile = "nation.bin";
        const string dbFilePathDir = tu.dbfile_dir + binFile;
        File file;
        OrderMaker sortorder;
        string tpchpath(tpch_dir);
        Schema nation ("catalog", "nation");
        if(!Utilities::checkfileExist(dbFilePathDir.c_str())) {
            dbFileObj.Create(dbFilePathDir.c_str(),heap,NULL);
        }
        dbFileObj.Open(dbFilePathDir.c_str());
        dbFileObj.Load(nation,dbFilePathDir.c_str());
        tu.getSortOrderFromCNF(sortorder);
        run trun(1,&sortorder);
        long long int pageCount=0;
        trun.AddPage();
        while(dbFileObj.GetNext(recordObj)) {
                if(!trun.addRecordAtPage(pageCount, &recordObj)) {
                    if(trun.checkRunFull()) {
                    break;
                }
            }
        }
        trun.writeRunToFile(&file);
        file.Close();dbFileObj.Close();
        // th.deleteFileOnFilePath("temp.xbin");
        tu.deleteFileOnFilePath(dbFilePathDir.c_str());
        string prefloc = tu.dbfile_dir+"nation.pref";
        tu.deleteFileOnFilePath(prefloc.c_str());    
}
TEST(RunManager, GetPages) {
    RunManager rm(1,"temp.xbin");
    vector<Page *> pages;
    rm.getPages(&pages);
    ASSERT_GT(pages.size(),0);
}
TEST(RunManager, getNextPage) {
    RunManager rm(1,"temp.xbin");
    Page *page;
    ASSERT_FALSE(rm.getNextPageOfRun(page,1));
}

TEST(customRecordComparator, customRecordComparator) {
    TestUtilities th;
    RunManager rm(1,"temp.xbin");
    ASSERT_GE(rm.getNoOfRuns()*rm.getRunLength(),rm.getTotalPages());
    th.deleteFileOnFilePath("temp.xbin");
}
// -------------------------------------------------------------------------------------------------------------
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}