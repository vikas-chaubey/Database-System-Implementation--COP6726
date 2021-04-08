#ifndef DBFILE_H
#define DBFILE_H

#include "Record.h"
#include "TwoWayList.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Schema.h"
#include <string>

typedef enum {heap, sorted, tree} fType;
typedef enum {READ, WRITE,IDLE} BufferMode;

// This class handles the metada for different tpcgn tables 
class Preference{
public:

	// Variable to track current record
	int currentRecordState; 

	//this variable stores page buffer state
	BufferMode pageBufferState; 

	//tracking current page variable
	off_t currentPageInd; 

	//  variable preference file path
	char * prefFilePathDir; 

	// variable to check if page is full
	bool isPageFull;

	// variable to track rewriting of page
	bool rewritePageFlag; 

	// vraible to track if all records have been written
	bool recordsWritingCompleted; 

};


// stub DBFile header..replace it with your own DBFile.h

class DBFile {
private:
//  file var to keep pages
    File file; 
//  page var to keep records
    Page page; 

//  Used to keep track of the state.
    Preference preference; 
//  Used to keep track of the state.
	ComparisonEngine comparisonEngine; 

public:
	// Constructor and Destructor
	DBFile ();
    ~DBFile ();
	
	// this Function gets the location to write next page
	int GetNextPageLocationToWrite();

	// This Function gets the page location from where disk read operation has to be done , Buffer Mode argument - Will give different locations according to current state of the DBFile.
	int GetNextPageLocationToRead(BufferMode mode);

	// this function gets the same page location which has to be rewritten
	int GetPageLocationToReWrite();

	// This function loads the preference from the disk , each table file has a corresponding .pref , ,f_path is the table to be created or opened.
	void LoadPreferenceFromDisk(char*f_path);

	// This functions saves the preference to the disk.
	void SavePreferenceToDisk();



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

	
};
#endif
