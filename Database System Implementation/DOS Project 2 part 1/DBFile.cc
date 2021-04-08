#include "Comparison.h"
#include "File.h"
#include "ComparisonEngine.h"
#include "Schema.h"
#include "DBFile.h"
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Utilities.h"
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include "fstream"

// stub file .. replace it with your own DBFile.cc
DBFile::DBFile () {
    isFileOpen = false;
}

DBFile::~DBFile () {
}


/**
        This function creates the binary files.This function takes three arguments the first one is
        the file path , second one is the file type which tells what type of file it is , three possible
		values are : heap, sorted, and tree. in this assignment we are dealing with heap files only.he 
		actual database data using the File class from File.h.The metadat file is saved using .pref extension.
		The last parameter to Create is a dummy parameter that you won’t use for this assignment,
		but you will use for assignment two. The return value from Create is a 1 on success
		and a zero on failure.
**/

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if (Utilities::checkfileExist(f_path)) {
        cout << "file you are about to create already exists!"<<endl;
        return 0;    
    }
    if (f_type == heap){
            string s(f_path);
            string news = s.substr(0,s.find_last_of('.'))+".pref";
            char* finalString = new char[news.length()+1];
            strcpy(finalString, news.c_str());
            file.Open(0,(char *)f_path);
            LoadPreferenceFromDisk(finalString);            
            isFileOpen = true;
            return 1;
    }
    return 0;
}

// this Function gets the location to write next page
int DBFile::GetNextPageLocationToWrite() {
    int pageLocation = file.GetLength();
    return !pageLocation ? 0 : pageLocation-1;
}

// This Function gets the page location from where disk read operation has to be done , Buffer Mode argument - Will give different locations according to current state of the DBFile.
int DBFile::GetNextPageLocationToRead(BufferMode mode) {
    if (mode == WRITE){
        return preference.currentPageInd-2;
    }
    else if (mode == READ){
        return preference.currentPageInd;
    }
}

// this function gets the same page location which has to be rewritten
int DBFile::GetPageLocationToReWrite(){
    int pageLocation = file.GetLength();
    return pageLocation == 2 ? 0 : pageLocation-1; 

}

/**
    This function "Add" adds records to end of the the file, There are then two functions that allow
	for record retrieval from a DBFile instance; all are
	called GetNext.
**/
void DBFile::Add (Record &rec) {
    
    if (!isFileOpen){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }
     if (preference.pageBufferState == READ ) {
            if( page.getNumRecs() > 0){
                page.EmptyItOut();
            }
            // open page the last written page to start rewriting
            file.GetPage(&page,GetPageLocationToReWrite());
            preference.currentPageInd = GetPageLocationToReWrite();
            preference.currentRecordState = page.getNumRecs();
            preference.rewritePageFlag = true;

    }

    // set DBFile in write mode
    preference.pageBufferState = WRITE;
    
    // add record to current page 
    // check if the page is full
    if(!this->page.Append(&rec)) {
        
        // if page is full, then write page to disk. Check if the date needs to rewritten or not
        if (preference.rewritePageFlag){
            this->file.AddPage(&this->page,GetPageLocationToReWrite());
            preference.rewritePageFlag = false;
        }
        else{
            this->file.AddPage(&this->page,GetNextPageLocationToWrite());
        }
        this->page.EmptyItOut();
        this->page.Append(&rec);
    }
    preference.recordsWritingCompleted=false;
}


/**
 * the Load function bulk loads the DBFile instance from a text file,
 * appending new data to it using the SuckNextRecord function from 
 * Record.h. The character string passed to Load is the name of the 
 * data file to bulk load. 
**/
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    
    if (!isFileOpen){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }

    Record temp;
    // Flush the page data from which you are reading and load the last page to start appending records.
    if (preference.pageBufferState == READ ) {
        if( page.getNumRecs() > 0){
            page.EmptyItOut();
        }
        // open page for write
        file.GetPage(&page,GetPageLocationToReWrite());
        preference.currentPageInd = GetPageLocationToReWrite();
        preference.currentRecordState = page.getNumRecs();
        preference.rewritePageFlag = true;
    }
    // set DBFile in WRITE Mode
    preference.pageBufferState = WRITE;
    FILE *tableFile = fopen (loadpath, "r"); 
    // while there are records, keep adding them to the DBFile. Reuse Add function.
    while(temp.SuckNextRecord(&f_schema, tableFile)==1) {
        Add(temp);
    }
    
}


/**
        This function open opens the already existing Dbfiles ,it assumes that the DBFile already exists and has previously
        been created and then closed. The one parameter to this function is simply the physical location
        of the file. The return value is a 1 on success and a zero on failure.
**/
int DBFile::Open (const char *f_path) {
    
    if (!Utilities::checkfileExist(f_path)) {
        cout << "Trying to open a file which is not created yet!"<<endl;
        return 0;
    }
    // changing .bin extension to .pref for storing preferences.
    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    file.Open(1,(char *)f_path);
    LoadPreferenceFromDisk(finalString);            
    if(file.IsFileOpen()){
        isFileOpen = true;
        // Load the last saved state from preference.
        if( preference.pageBufferState == READ){
            file.GetPage(&page,GetNextPageLocationToRead(preference.pageBufferState));
            Record myRecord;
            for (int i = 0 ; i < preference.currentRecordState; i++){
                page.GetFirst(&myRecord);
            }
            preference.currentPageInd++;
        }
        else if(preference.pageBufferState == WRITE && !preference.isPageFull){
            file.GetPage(&page,GetNextPageLocationToRead(preference.pageBufferState));
        }
        return 1;
    }
    isFileOpen = false;
    return 0;
}

/**
        Echa Dbfile consititutes a pointer which points to the first record in the file.
        This pointer can move  with respect to retreivals of records from the file.
        This function Forces the pointer to reset its position to the first record of the file
**/
void DBFile::MoveFirst () {
    if (file.IsFileOpen()){
        isFileOpen = true;
        if (preference.pageBufferState == WRITE && page.getNumRecs() > 0){
            if(!preference.recordsWritingCompleted){
                file.AddPage(&page,GetPageLocationToReWrite());
            }
        }
        page.EmptyItOut();
        preference.pageBufferState = READ;
        file.MoveToFirst();
        preference.currentPageInd = 0;
        preference.currentRecordState = 0;
    }
}

/**
	Next, Close simply closes the file. The return value is a 1
    on success and a zero on failure.
**/ 
int DBFile::Close () {
    if (!isFileOpen) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    if(preference.pageBufferState == WRITE && page.getNumRecs() > 0){
            if(!preference.recordsWritingCompleted){
                if (preference.rewritePageFlag){
                    file.AddPage(&this->page,GetPageLocationToReWrite());
                    preference.rewritePageFlag = false;
                }
                else{
                    file.AddPage(&this->page,GetNextPageLocationToWrite());
                }
            }
            preference.isPageFull = false;
            preference.currentPageInd = file.Close();
            preference.recordsWritingCompleted = true;
            preference.currentRecordState = page.getNumRecs();
    }
    else{
        if(preference.pageBufferState == READ){
            preference.currentPageInd--;
        }
        file.Close();
    }
    
    isFileOpen = false;
    SavePreferenceToDisk();
    return 1;
}

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
int DBFile::GetNext (Record &fetchme) {
    if (file.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if (preference.pageBufferState == WRITE && page.getNumRecs() > 0){
            if(!preference.recordsWritingCompleted){
                file.AddPage(&page,GetPageLocationToReWrite());
            }
            //  Only Write Records if new records were added.
            page.EmptyItOut();
            preference.currentPageInd = file.GetLength();
            preference.currentRecordState = page.getNumRecs();
            return 0;
        }
        preference.pageBufferState = READ;
        // loop till the page is empty and if empty load the next page to read
        if (!page.GetFirst(&fetchme)) {
            // check if all records are read.
            if (preference.currentPageInd+1 >= file.GetLength()){
               return 0;
            }
            else{
                // load new page and get its first record.
                file.GetPage(&page,GetNextPageLocationToRead(preference.pageBufferState));
                page.GetFirst(&fetchme);
                preference.currentPageInd++;
                preference.currentRecordState = 0;
            }
        }
        // increament counter for each read.
        preference.currentRecordState++;
        return 1;
    }
}

/**
        The next version of GetNext also accepts a selection predicate
        (this is a conjunctive normal form expression). It returns the
        next record in the file that is accepted by the selection predicate.
        The literal record is used to check the selection predicate, and is
        created when the parse tree for the CNF is processed.
**/
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
        // Flush the Page Buffer if the WRITE mode was active.
        if (preference.pageBufferState == WRITE && page.getNumRecs() > 0){
            //  Only Write Records if new records were added.
            if(!preference.recordsWritingCompleted){
                file.AddPage(&page,GetPageLocationToReWrite());
            }
            page.EmptyItOut();
            preference.currentPageInd = file.GetLength();
            preference.currentRecordState = page.getNumRecs();
            return 0;
        }
        bool readFlag ;
        bool compareFlag;
        // loop until all records are read or if some record maches the filter CNF
        do{
            readFlag = GetNext(fetchme);
            compareFlag = comparisonEngine.Compare (&fetchme, &literal, &cnf);
        }
        while(readFlag && !compareFlag);
        if(!readFlag){
            return 0;
        }
        return 1;

}


// This function loads the preference from the disk , each table file has a corresponding .pref , ,f_path is the table to be created or opened.
void DBFile::LoadPreferenceFromDisk(char * newFilePath) {
    ifstream file;
    if (Utilities::checkfileExist(newFilePath)) {
        file.open(newFilePath,ios::in); 
        if(!file){
            cerr<<"Error in opening file..";
            exit(1);
        }
        file.read((char*)&preference,sizeof(Preference));
        preference.prefFilePathDir = (char*)malloc(strlen(newFilePath) + 1); 
        strcpy(preference.prefFilePathDir,newFilePath);
    }
    else {
        preference.prefFilePathDir = (char*) malloc(strlen(newFilePath) + 1); 
        strcpy(preference.prefFilePathDir,newFilePath);
        preference.currentPageInd = 0;
        preference.currentRecordState = 0;
        preference.isPageFull = false;
        preference.pageBufferState = IDLE;
        preference.rewritePageFlag= false;
        preference.recordsWritingCompleted = true;
    }
}

// This functions saves the preference to the disk.
void DBFile::SavePreferenceToDisk(){
    ofstream file;
    file.open(preference.prefFilePathDir,ios::out);
    if(!file) {
        cerr<<"Error in opening file for writing.."<<endl;
        exit(1);
    }
    file.write((char*)&preference,sizeof(Preference));
    file.close();
}
