#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

/*==============================================================================================*/
class RelationalOp {

	public:
	// this function holds the calling entity till the executing relational operator has run to completion
	virtual void WaitUntilDone () = 0;

	// This function provides information regarding internal memory usage
	virtual void Use_n_Pages (int n) = 0;
};

/*==============================================================================================*/
class SelectFile : public RelationalOp { 
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	//Variable to hold record object
	Record *literal;
	//DBfile instance
	DBFile *inFile;
	//output Pipe Object
	Pipe *outPipe;
	//CNF class variable for CNF statement processing
	CNF *selOp;
	//all auxillary functions for this class
	public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	// This function provides information regarding internal memory usage
	void Use_n_Pages (int n);
	//this function is a static caller method
	static void* caller(void*);
	void *operation();
};

/*==============================================================================================*/
class SelectPipe : public RelationalOp {
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	//input Pipe Object
	Pipe *inPipe;
	//output Pipe Object
	Pipe *outPipe;
	//CNF class variable for CNF statement processing
	CNF *selOp;
	//Variable to hold record object
	Record *literal;
	//all auxillary functions for this class
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	// This function provides information regarding internal memory usage
	void Use_n_Pages (int n);
	//this function is a static caller method
	static void* caller(void*);
	void *operation();
};

/*==============================================================================================*/
class Project : public RelationalOp { 
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	//input Pipe Object
	Pipe *inPipe;
	//output Pipe Object
	Pipe *outPipe;
	//variable to keep counter
	int *keepMe;
	//variable to hold integer number at TS input
	int numAttsInput;
	//variable to hold integer number at TS output
	int numAttsOutput;
	//all auxillary functions for this class
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	// This function provides information regarding internal memory usage
	void Use_n_Pages (int n);
	//this function is a static caller method
	static void* caller(void*);
	void *operation();
};

/*==============================================================================================*/
class Join : public RelationalOp {
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	//input Pipe Object left
	Pipe *inPipeL;
	//input Pipe Object right
	Pipe *inPipeR;
	//output Pipe Object
	Pipe *outPipe;
	//CNF class variable for CNF statement processing
	CNF *selOp;
	//Variable to hold record object
	Record *literal;
	int rl, mc=0, lrc=0, rrc=0;
	//all auxillary functions for this class
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	//main join method
	void *operation();
	//method to merge record pages
	void MergePages(vector<Record*> lrvec, Page *rp, OrderMaker &lom, OrderMaker &rom);
	//method to merge records
	void MergeRecord(Record *lr, Record *rr);
	//method which defines  sited merge join : This algorithm is used in case a proper OrderMaker object is not formed using given CNF
	void sortMergeJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom);
	//method which defines block nested join : This algorithm is run otherwise and helps apply the CNF without using any OrderMaker
    //objects
	void blockNestedJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom);
};

/*==============================================================================================*/
class DuplicateRemoval : public RelationalOp {
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	// input Pipe Object variable
	Pipe *inPipe;
	// output Pipe Object variable
	Pipe *outPipe;
	// Variable to hold object of Schema Class
	Schema *mySchema;
	int rl;
	// all auxillary functions for this class
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	// This function provides information regarding internal memory usage
	void Use_n_Pages (int n);
	//this function is a static caller method
	static void* caller(void*);
	void *operation();
};

/*==============================================================================================*/
class Sum : public RelationalOp {
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	// input Pipe Object variable
	Pipe *inPipe;
	// output Pipe Object variable
	Pipe *outPipe;
	// Variable to hold Function class object
	Function *computeMe;
	public:
	// all auxillary functions for this class
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	// This function provides information regarding internal memory usage
	void Use_n_Pages (int n);
	//this function is a static caller method
	static void* caller(void*);
	void *operation();
};

/*==============================================================================================*/
class GroupBy : public RelationalOp {
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	// input Pipe Object variable
	Pipe *inPipe;
	// output Pipe Object variable
	Pipe *outPipe;
	// variable to hold object of ordermaker clas providing group attribiutes
	OrderMaker *groupAtts;
	//Function class object variable
	Function *computeMe;
	int rl;
	public:
	// all auxillary functions for this class
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	// This function provides information regarding internal memory usage
	void Use_n_Pages (int n);
	//this function is a static caller method
	static void* caller(void*);
	void *operation();
};

/*==============================================================================================*/
class WriteOut : public RelationalOp {
	private:
	//Pthread object variable
	pthread_t pthreadObj;
	// input Pipe Object variable
	Pipe *inPipe;
	// output File Object variable
	FILE *outFile;
	//variable to store schema class instance
	Schema *mySchema;
	public:
	// all auxillary functions for this class
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	// this function holds the calling entity till the executing relational operator has run to completion
	void WaitUntilDone ();
	// This function provides information regarding internal memory usage
	void Use_n_Pages (int n);
	//this function is a static caller method
	static void* caller(void*);
	void *operation();
};
#endif
/*==============================================================================================*/
