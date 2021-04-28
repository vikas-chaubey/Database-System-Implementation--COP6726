#include <string>
#include <fstream>
#include "DBFile.h"
#include "Statistics.h"
#include "Utilities.h"
#include <gtest/gtest.h>
#include "SQL.h"

extern "C" int yyparse(void);

extern int queryType;
extern char *outputVar;
extern char *tableName;
extern char *fileToInsert;
extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;
extern struct AttrList *attsToCreate;
extern struct NameList *attsToSort;

// Holder for important variables
class FilePath{
    public:
        const std::string dbfile_dir = "bin/"; 
        const std::string tpch_dir ="tpch/"; 
        const std::string catalog_path = "catalog";
};

TEST(QueryTesting, CheckDBFileCreate) {

    // This is a utility function that creates a DBFile
    // Then checks if it exists using the Utility Function
    // And then Delete the DBFile and check again

    // remove if file already exists
    remove("bin/test.bin");
    remove("bin/test.pref");
    
    FilePath filepath;
    const string schemaSuffix = "test.bin";
    const string dbFilePath = filepath.dbfile_dir + schemaSuffix;
    DBFile dbfile;
    dbfile.Create(dbFilePath.c_str(),heap,NULL);
    dbfile.Close();

    ASSERT_EQ(1, Utilities::checkfileExist("bin/test.bin"));
    ASSERT_EQ(1, Utilities::checkfileExist("bin/test.pref"));

}

TEST(QueryTesting, CheckDropTable) {
    yyparse();
    SQL sql;
    sql.ParseAndExecuteDrop();
    if (!Utilities::checkfileExist("bin/test.bin")) {
        ASSERT_EQ(1,1);
    }
    else {
        ASSERT_EQ(1,0);
    }
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
