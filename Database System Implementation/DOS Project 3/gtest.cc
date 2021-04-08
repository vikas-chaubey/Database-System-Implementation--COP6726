#include "DBFile.h"
#include "Schema.h"
#include "Utilities.h"
#include "RelOp.h"
#include <string.h>
#include <gtest/gtest.h>
#include "test.h"

// Integer type attribute
Attribute IA = {"int", Int};
// String type Attribute
Attribute SA = {"string", String};
// Double type Attribute
Attribute DA = {"double", Double};


// This is a utility method which clears the given pipe and prints the records if the print flag is set to true
int clear_pipe (Pipe &inputPipe, Schema *schema, bool print) {
    //variable to hold record
	Record rec;
    //record counter
	int cnt = 0;
    //remove records from input pipe one by one
	while (inputPipe.Remove (&rec)) {
		if (print) {
            //print records
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

// This is a Overloaded utility method , takes a function argument ,which clears the given pipe and prints the records if the print flag is set to true
int clear_pipe (Pipe &inputPipe, Schema *schema, Function &func, bool print) {
    //variable to hold record
	Record rec;
    //record counter
	int cnt = 0;
    //initial sum set to 0
	double sum = 0;
	while (inputPipe.Remove (&rec)) {
		if (print) {
            //print records
			rec.Print (schema);
		}
        //apply function
		int ival = 0; double dval = 0;
		func.Apply (rec, ival, dval);
        //add interval to the sum value
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}

// The buffer for each pipe is set to 100
int pipesz = 100; 
// This variable declares number of pages of memory allowed for operations
int buffsz = 100; 

//valraibles for selected files of different tables
SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
//dbfile for different tables
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
//variables defining separate pipes for different tables
Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz);
//CNF variables to process CNF statements for different tables
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
//Record variables for different tables
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
//Function values for differen tables
Function func_ps, func_p, func_s, func_o, func_li, func_c;

//Attribute run lenth for different tables
//Attribute run lenth for parts table
int pAtts = 9;
//Attribute run lenth for partsupp table
int psAtts = 5;
//Attribute run lenth for lineItem table
int liAtts = 16;
//Attribute run lenth for orders table
int oAtts = 9;
//Attribute run lenth for supplier table
int sAtts = 7;
//Attribute run lenth for customer table
int cAtts = 8;
//Attribute run lenth for nation table
int nAtts = 4;
//Attribute run lenth for region table
int rAtts = 3;

//initiate predicate for partsupp table
void init_SF_ps (char *pred_str, int numpgs) {
	dbf_ps.Open (ps->path());
	get_cnf (pred_str, ps->schema (), cnf_ps, lit_ps);
	SF_ps.Use_n_Pages (numpgs);
}

//initiate predicate for parts table
void init_SF_p (char *pred_str, int numpgs) {
	dbf_p.Open (p->path());
	get_cnf (pred_str, p->schema (), cnf_p, lit_p);
	SF_p.Use_n_Pages (numpgs);
}

//initiate predicate for supplier table
void init_SF_s (char *pred_str, int numpgs) {
	dbf_s.Open (s->path());
	get_cnf (pred_str, s->schema (), cnf_s, lit_s);
	SF_s.Use_n_Pages (numpgs);
}

//initiate predicate for orders table
void init_SF_o (char *pred_str, int numpgs) {
	dbf_o.Open (o->path());
	get_cnf (pred_str, o->schema (), cnf_o, lit_o);
	SF_o.Use_n_Pages (numpgs);
}

//initiate predicate for lineitem table
void init_SF_li (char *pred_str, int numpgs) {
	dbf_li.Open (li->path());
	get_cnf (pred_str, li->schema (), cnf_li, lit_li);
	SF_li.Use_n_Pages (numpgs);
}

//initiate predicate for customer table
void init_SF_c (char *pred_str, int numpgs) {
	dbf_c.Open (c->path());
	get_cnf (pred_str, c->schema (), cnf_c, lit_c);
	SF_c.Use_n_Pages (numpgs);
}

//This method tests the utility method which generates new file name with given extension
TEST(QueryTesting, GettingUniqueFilePath) {
    
    char *fileName1 = Utilities::newRandomFileName(".bin");
    char *fileName2 = Utilities::newRandomFileName(".bin");
    ASSERT_TRUE(fileName1!=fileName2);
}

//This method tests the sum method 
TEST(QueryTesting, sum) {
    setup();
    char *pred_s = "(s_suppkey = s_suppkey)";
    init_SF_s (pred_s, 100);

    Sum T;
    // _s (input pipe)
    Pipe _out (1);
    Function func;
    char *str_sum = "(s_acctbal)/10000000";
    get_cnf (str_sum, s->schema (), func);
    func.Print ();
    T.Use_n_Pages (1);
    SF_s.Run (dbf_s, _s, cnf_s, lit_s);
    T.Run (_s, _out, func);

    SF_s.WaitUntilDone ();
    T.WaitUntilDone ();

    Schema out_sch ("out_sch", 1, &DA);
    
    Record t;
    _out.Remove(&t);
    int pointer = ((int *) t.bits)[1];
    double *md = (double *) &(t.bits[pointer]);
    double test = *md;
    dbf_s.Close ();
    EXPECT_NEAR(4.51035,test, 1);
}

//This method tests the writeout method
TEST(QueryTesting, WriteOutTesting) {

    DBFile s;
    s.Open("dbfiles/supplier.bin");
    s.MoveFirst();
    char *fwpath = "wo.txt";
	FILE *writefile = fopen (fwpath, "w");
    WriteOut w;
    Pipe ip(PAGE_SIZE);
    Record temp;
    Schema supp("catalog","supplier");
    w.Run(ip, writefile, supp);
    int c = 0;
    while(s.GetNext(temp)) {
        ++c;
        ip.Insert(&temp);
    }
    s.Close();
    ip.ShutDown();
    w.WaitUntilDone();

    int numLines = 0;
    ifstream in("wo.txt");
    std::string unused;
    while ( std::getline(in, unused) )
        ++numLines;
    in.close();

    if(Utilities::checkfileExist("wo.txt")) {
        if( remove("wo.txt") != 0 )
        cerr<< "Error deleting file" ;
    }
    ASSERT_EQ(c, numLines);
}

//This method tests the Duplicate Removal method
TEST(QueryTesting, DuplicateRemovalTesting) {
    DBFile s;
    s.Open("dbfiles/supplier.bin");
    s.MoveFirst();
   
    DuplicateRemoval w;
    Pipe ip(PAGE_SIZE);
    Pipe op(PAGE_SIZE);
    Record temp;
    Schema supp("catalog","supplier");
    w.Run(ip,op,supp);
    int c = 0;
    while(s.GetNext(temp)) {
        ++c;
        ip.Insert(&temp);
    }
    s.Close();
    ip.ShutDown();
    w.WaitUntilDone();

    ASSERT_EQ(10000,clear_pipe(op, &supp, false));
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}