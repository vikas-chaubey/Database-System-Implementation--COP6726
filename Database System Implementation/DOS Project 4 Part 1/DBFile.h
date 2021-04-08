#ifndef DBFILE_H
#define DBFILE_H

#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Pipe.h"

typedef enum {heap, sorted, tree,undefined} fType;
typedef enum {READ, WRITE,IDLE} BufferMode;

typedef struct {
    OrderMaker *o;
    int l;
} SortedStartUp;

// This class handles the metada for different tpcgn tables 
class Preference{
public:
    // This is an Indicator for type of DBFile
    fType fileType;
    
    //this variable stores page buffer state , it can take following Possible values : WRITE,READ & IDLE
    // when the file is just created then the default state is IDLE 
    BufferMode pageBufferState;

    // This Variable tracks the current page , from where records are read and written with the File Class. 
    // It can have different types of values and that depends on the mode.
    off_t currentPageIndex;

    // This Variable tracks the current record , from where records are read and written with the File Class.
    int currentRecordState;

    // This is a Boolean valued variable flag to indicate if the page isFull or not.
    bool isPageFull;

    //  This variable stores the file path  to the preference file
    char * prefFileDir;

    // This variable indicates whether all the records from the page are written to the file or not
    bool areAllRecordsWitten;

    // Boolean indicating if the page needs to be rewritten or not.
    bool reWritePage;
    
    // Sorted FIle takes an argument of ordermaker as input.
    
    char orderMakerBits[sizeof(OrderMaker)];
    
    // OrderMaker as an Input For Sorted File.
    OrderMaker * orderMaker;
    
    // This variable defines the Run Length For Sorting
    int SortRunLength;

};


class GenericDBFile{
protected:
    //  This file variable is used to conatin pages , page read write operations are done using file
    File fileObj;
    //  Page varibales contains records data
    Page pageObj;
    // this Pointer points to preference file
    Preference * prefPointer;
    // Comparison Engine object variable
    ComparisonEngine comparisonEngineObj;
public:
    GenericDBFile();
    // this Function gets the location to write next page
    int GetPageLocationToWrite();
     // This Function gets the page location from where disk read operation has to be done , 
    //Buffer Mode argument - Will give different locations according to current state of the DBFile.
    int GetPageLocationToRead(BufferMode mode);
    // this function gets the same page location which has to be rewritten
    int GetPageLocationToReWrite();
    void Create (char * f_path,fType fileType, void *startup);
    int Open (char * f_path);
    
   //  These are virtual function for GenericDBFile
    virtual ~GenericDBFile();
    virtual void MoveFirst ();
    virtual void Add (Record &addme);
    virtual void Load (Schema &myschema, const char *loadpath);
    virtual int GetNext (Record &fetchme);
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    virtual int Close();
};

//This is Heap DB FIle  Class which inherits and overrides virtual functions from GenericDBFile class
class HeapDBFile: public virtual GenericDBFile{
public:
    HeapDBFile(Preference * preference);
    ~HeapDBFile();
    void MoveFirst ();
    void Add (Record &addme);
    void Load (Schema &myschema, const char *loadpath);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();

};

class SortedDBFile: public  GenericDBFile{
    Pipe * inputPipePtr;
    Pipe * outputPipePtr;
    BigQ * bigQPtr;
    File newFile;
    Page outputBufferForNewFile;
    OrderMaker * queryOrderMaker;
    bool doBinarySearch;
    
public:
    SortedDBFile(Preference * preference);
    ~SortedDBFile();
    void MoveFirst ();
    void Add (Record &addme);
    void Load (Schema &myschema, const char *loadpath);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();
    void MergeSortedInputWithFile();
    int BinarySearch(Record &fetchme, Record &literal,off_t low, off_t high);
};


// stub DBFile header..replace it with your own DBFile.h

class DBFile {
private:
//  Generic DBFile Pointer
    GenericDBFile * fileObjPtr;
//  Used to keep track of the state.
    Preference myPreference;
public:
	// Constructor and Destructor
	DBFile ();
    ~DBFile ();
	
	// Function to load preference from the disk. This is needed to make read and writes persistent.
	// @input - the file path of the table to be created or opened. Each tbl file has a corresponding .pref
	void LoadPreference(char*f_path,fType fileType);

	// Function to dumpn the preference to the disk. This is needed to make read and writes persistent.
	void DumpPreference();

	//  Function to directly load data from the tbl files.
	/**
		The Load function bulk loads the DBFile instance from a text file, appending new data
		to it using the SuckNextRecord function from Record.h. The character string passed to Load is
		the name of the data file to bulk load.
	**/
	void Load (Schema &myschema, const char *loadpath);
	bool isFileOpen;
	
	/**
	        Echa Dbfile consititutes a pointer which points to the first record in the file.
	        This pointer can move  with respect to retreivals of records from the file.
	        This function Forces the pointer to reset its position to the first record of the file
	**/
	void MoveFirst ();

	/**
	    This function "Add" adds records to end of the the file, There are then two functions that allow
		for record retrieval from a DBFile instance; all are
		called GetNext.
	**/
	void Add (Record &addme);

	/**
     	This GenNext method version fetches the next record from the file.
     	The nexrt record's position is defined with respect to current position
     	of the file pointer where it is currently pointing.This method will 
     	fetch the subsequent Record with respct to the pointer position.After 
     	the function call returns, the pointer into the file is incremented, 
     	so a subsequent call to GetNext won’t return the same record twice. 
     	If next record is found and returnned an integer value 1 is returnned
     	otherwise an integer value 0 is returnned.
	**/
	int GetNext (Record &fetchme);

	/**
		The next version of GetNext also accepts a selection predicate
		(this is a conjunctive normal form expression). It returns the
		next record in the file that is accepted by the selection predicate.
		The literal record is used to check the selection predicate, and is
		created when the parse tree for the CNF is processed.
	**/
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	/**
	        This function creates the binary files.This function takes three arguments the first one is
	        the file path , second one is the file type which tells what type of file it is , three possible
			values are : heap, sorted, and tree. in this assignment we are dealing with heap files only.he 
			actual database data using the File class from File.h.The metadat file is saved using .pref extension.
			The last parameter to Create is a dummy parameter that you won’t use for this assignment,
			but you will use for assignment two. The return value from Create is a 1 on success
			and a zero on failure.
	**/
    int Create (const char *fpath, fType file_type, void *startup);

	/**
	        This function open opens the already existing Dbfiles ,it assumes that the DBFile already exists and has previously
	        been created and then closed. The one parameter to this function is simply the physical location
	        of the file. The return value is a 1 on success and a zero on failure.
	**/
    int Open (const char *fpath);
	/**
		Next, Close simply closes the file. The return value is a 1 on success and a zero on failure.
	**/
    int Close ();
};
#endif
