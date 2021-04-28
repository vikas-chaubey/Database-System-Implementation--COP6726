#include <string.h>
#include <gtest/gtest.h>
#include "test.h"
#include "Utilities.h"
#include "BigQ.h"

// -------------------------------------------------------------------------------------------------------------
//Utility class with methods used in below test cases
class TestHelper{
    public:
    const std::string dbfile_dir = "dbfiles/"; 
    const std::string tpch_dir ="tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
    static OrderMaker sortorder;
    void get_sort_order (OrderMaker &sortorder) {
        cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
        if (yyparse() != 0) {
            cout << "Can't parse your sort CNF.\n";
            exit (1);
        }
        cout << " \n";
        Record literal;
        CNF sort_pred;
        sort_pred.GrowFromParseTree (final, new Schema ("catalog","nation"), literal);
        OrderMaker dummy;
        sort_pred.GetSortOrders (sortorder, dummy);
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
TEST(testingLoadSortedFile,testingLoadSortedFile) {
        TestHelper helperObj;
        OrderMaker sortorder;
        string tpchpath(tpch_dir);
        Schema nation ("catalog", "nation");
        DBFile dbFileSorted;
        Record recordTemp;
        const string binFileSuffix = "nation.bin";
        const string dbFileDir = helperObj.dbfile_dir + binFileSuffix;

        // create new DBFile of type 'sorted'
        if(!Utilities::checkfileExist(dbFileDir.c_str())) {
            dbFileSorted.Create(dbFileDir.c_str(),sorted,NULL);
        }

        // open dbfile
        dbFileSorted.Open(dbFileDir.c_str());
        dbFileSorted.Load(nation,dbFileDir.c_str());
        dbFileSorted.Close();

        helperObj.deleteFileOnFilePath("dbfiles/nation.bin");helperObj.deleteFileOnFilePath(dbFileDir.c_str());
        string prefloc = helperObj.dbfile_dir+"nation.pref";helperObj.deleteFileOnFilePath(prefloc.c_str());    
}
// -------------------------------------------------------------------------------------------------------------
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}