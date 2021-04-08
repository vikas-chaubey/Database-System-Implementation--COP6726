#include <string>
#include <fstream>
#include "DBFile.h"
#include "Record.h"
#include "File.h"
#include "Schema.h"
#include "Utilities.h"
#include "Statistics.h"
#include "RelOp.h"
#include "BigQ.h"
#include <pthread.h>
#include <gtest/gtest.h>

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

class FilePath{
    public:
    const std::string dbfile_dir = ""; 
    const std::string tpch_dir ="tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
};

using namespace std;
Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		int ival = 0; double dval = 0;
		func.Apply (rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}
int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
SelectPipe SP_ps, SP_p, SP_s, SP_o, SP_li, SP_c;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz);
//CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
Function func_ps, func_p, func_s, func_o, func_li, func_c;

int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;

TEST (Statistics,AddAtt)
{
	 DBFile dbfile;
    
    string dbfile_dir = ""; // dir where binary heap files should be stored
    string tpch_dir = "tpch-dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
    string catalog_path = "catalog"; // full path of the catalog file
    string tablename = "";
	
	char *pred_ps = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
	int val = 22, cnt=23;

	cnt =cnt+val;
	EXPECT_NO_THROW(SF_p.Use_n_Pages (30));
}
TEST (Statistics,CopyRel)
{
	 DBFile dbfile;
    
    string dbfile_dir = ""; // dir where binary heap files should be stored
    string tpch_dir = "tpch-dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
    string catalog_path = "catalog"; // full path of the catalog file
    string tablename = "";
	
	char *pred_ps = "(s_suppkey = s_suppkey)";
	int val = 22, cnt=23;
	Join J;
		// left _s
		// right _ps
		Pipe _s_ps (pipesz);
		CNF cnf_p_ps;
		Record lit_p_ps;
	
	cnt =cnt+val;
	EXPECT_NO_THROW(SP_li.Use_n_Pages (30));
}

TEST(QueryTesting, estimate) {
    Statistics statObj;
    char *relNameArray[] = {"orders"};
	statObj.AddRel(relNameArray[0],5057879);
	statObj.AddAtt(relNameArray[0], "order_flag",30);
	statObj.AddAtt(relNameArray[0], "order_distatObjcount",15);
	statObj.AddAtt(relNameArray[0], "order_delivery_type",70);
	char *cnf = "(order_flag = 'D') AND (order_discount < 0.04 OR order_delivery_type = 'UPS')";
	yy_scan_string(cnf);
	yyparse();
	double result = statObj.Estimate(final, relNameArray, 1);
    ASSERT_NEAR(57804.3, result, 0.1);
}


TEST(QueryTesting, apply) {
    Statistics statObj;
    // joining 3 relations
    char *relNameArray[] = {"t1","t2","t3"};
	statObj.AddRel(relNameArray[0],1700000);
	statObj.AddAtt(relNameArray[0], "t1_k1",170000);
	statObj.AddRel(relNameArray[1],180000);
	statObj.AddAtt(relNameArray[1], "t2_k1",180000);
	statObj.AddAtt(relNameArray[1], "t2_k2",50);
	
	statObj.AddRel(relNameArray[2],100);
	statObj.AddAtt(relNameArray[2], "t3_k1",7);

	char *cnf = "(t1_k1 = t2_k1)";
	yy_scan_string(cnf);
	yyparse();

	// Join the first two relations in relNameArray
	statObj.Apply(final, relNameArray, 2);
	
	cnf = " (t2_k2 = t3_k1)";
	yy_scan_string(cnf);
	yyparse();
	
	double result = statObj.Estimate(final, relNameArray, 3);
	ASSERT_NEAR(3400000,result,0.1);
}

//This method tests the utility method which generates new file name with given extension
TEST(QueryTesting, GettingUniqueFilePath) {
    
    char *fileName1 = Utilities::newRandomFileName(".bin");
    char *fileName2 = Utilities::newRandomFileName(".bin");
    ASSERT_TRUE(fileName1!=fileName2);
}

//This method tests the utility method which generates new file name with given extension
TEST(QueryTesting, CheckIfFileExist) {
    
    int response = Utilities::checkfileExist("BigQ.cc");
    ASSERT_EQ(1, response);
}

//This Utility method checks whether there are already file present with a given name
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}