#include "DBFile.h"
#include "Utilities.h"

/*===================== Generic DBFile Class Functions Definitions====================*/
GenericDBFile::GenericDBFile(){
    
}

GenericDBFile::~GenericDBFile(){

}

// this Function gets the location to write next page
int GenericDBFile::GetPageLocationToWrite() {
    int pageLocation = fileObj.GetLength();
    return !pageLocation ? 0 : pageLocation-1;
}

// This Function gets the page location from where disk read operation has to be done , Buffer Mode argument - Will give different locations according to current state of the DBFile.
int GenericDBFile::GetPageLocationToRead(BufferMode mode) {
    if (mode == WRITE){
        return prefPointer->currentPageIndex-2;
    }
    else if (mode == READ){
        return prefPointer->currentPageIndex;
    }
}

// this function gets the same page location which has to be rewritten
int GenericDBFile::GetPageLocationToReWrite(){
    int pageLocation = fileObj.GetLength();
    return pageLocation == 2 ? 0 : pageLocation-2;

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
void GenericDBFile::Create(char * f_path,fType fileType, void *startup){
    // opening file with given file extension
    fileObj.Open(0,(char *)f_path);
    if (startup!=NULL and fileType==sorted){
        prefPointer->orderMaker = ((SortedStartUp *)startup)->o;
        prefPointer->SortRunLength  = ((SortedStartUp *)startup)->l;
    }
}

/**
    This function open opens the already existing Dbfiles ,it assumes that the DBFile already exists and has previously
    been created and then closed. The one parameter to this function is simply the physical location
    of the file. The return value is a 1 on success and a zero on failure.
**/
int GenericDBFile::Open(char * f_path){
    // opening file with given file extension
    fileObj.Open(1,(char *)f_path);
    if(fileObj.IsFileOpen()){
        // Load the last saved state from preference.
        if( prefPointer->pageBufferState == READ){
            fileObj.GetPage(&pageObj,GetPageLocationToRead(prefPointer->pageBufferState));
            Record myRecord;
            for (int i = 0 ; i < prefPointer->currentRecordState; i++){
                pageObj.GetFirst(&myRecord);
            }
            prefPointer->currentPageIndex++;
        }
        else if(prefPointer->pageBufferState == WRITE && !prefPointer->isPageFull){
            fileObj.GetPage(&pageObj,GetPageLocationToRead(prefPointer->pageBufferState));
        }
        return 1;
    }
    return 0;
}

void GenericDBFile::MoveFirst () {}

void GenericDBFile::Add(Record &addme){}

void GenericDBFile::Load(Schema &myschema, const char *loadpath){}

int GenericDBFile::GetNext(Record &fetchme){}

int GenericDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal){}

int GenericDBFile::Close(){}

/*===================================================================================*/


/*===================== Heap DBFile Class Functions Definitions=======================*/
HeapDBFile :: HeapDBFile(Preference * preference){
    prefPointer = preference;
}

HeapDBFile::~HeapDBFile(){

}

/**
    Echa Dbfile consititutes a pointer which points to the first record in the file.
    This pointer can move  with respect to retreivals of records from the file.
    This function Forces the pointer to reset its position to the first record of the file
**/
void HeapDBFile::MoveFirst () {
    if (fileObj.IsFileOpen()){
        if (prefPointer->pageBufferState == WRITE && pageObj.getNumRecs() > 0){
            if(!prefPointer->areAllRecordsWitten){
                fileObj.AddPage(&pageObj,GetPageLocationToReWrite());
            }
        }
        pageObj.EmptyItOut();
        prefPointer->pageBufferState = READ;
        fileObj.MoveToFirst();
        prefPointer->currentPageIndex = 0;
        prefPointer->currentRecordState = 0;
    }
}

/**
    This function "Add" adds records to end of the the file, There are then two functions that allow
    for record retrieval from a DBFile instance; all are
    called GetNext.
**/
void HeapDBFile :: Add (Record &rec) {

    if (!fileObj.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }

     // Flush the page data from which you are reading and load the last page to start appending records.
     if (prefPointer->pageBufferState == READ ) {
            if( pageObj.getNumRecs() > 0){
                pageObj.EmptyItOut();
            }
            // open page the last written page to start rewriting
            fileObj.GetPage(&pageObj,GetPageLocationToReWrite());
            prefPointer->currentPageIndex = GetPageLocationToReWrite();
            prefPointer->currentRecordState = pageObj.getNumRecs();
            prefPointer->reWritePage = true;
    }

    // set DBFile in write mode
    prefPointer->pageBufferState = WRITE;

    if(pageObj.getNumRecs()>0 && prefPointer->areAllRecordsWitten){
                    prefPointer->reWritePage = true;
    }

    // add record to current page
    // check if the page is full
    if(!this->pageObj.Append(&rec)) {

        //cout << "DBFile page full, writing to disk ...." << fileObj.GetLength() << endl;

        // if page is full, then write page to disk. Check if the date needs to rewritten or not
        if (prefPointer->reWritePage){
            this->fileObj.AddPage(&this->pageObj,GetPageLocationToReWrite());
            prefPointer->reWritePage = false;
        }
        else{
            this->fileObj.AddPage(&this->pageObj,GetPageLocationToWrite());
        }

        // empty page
        this->pageObj.EmptyItOut();

        // add again to page
        this->pageObj.Append(&rec);
    }
    prefPointer->areAllRecordsWitten=false;
}

//  Function to directly load data from the tbl files.
/**
    The Load function bulk loads the DBFile instance from a text file, appending new data
    to it using the SuckNextRecord function from Record.h. The character string passed to Load is
    the name of the data file to bulk load.
**/
void HeapDBFile :: Load (Schema &f_schema, const char *loadpath) {

    if (!fileObj.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }

    Record temp;
    // Flush the page data from which you are reading and load the last page to start appending records.
    if (prefPointer->pageBufferState == READ ) {
        if( pageObj.getNumRecs() > 0){
            pageObj.EmptyItOut();
        }
        // open page for write
        fileObj.GetPage(&pageObj,GetPageLocationToReWrite());
        prefPointer->currentPageIndex = GetPageLocationToReWrite();
        prefPointer->currentRecordState = pageObj.getNumRecs();
        prefPointer->reWritePage = true;
    }
    // set DBFile in WRITE Mode
    prefPointer->pageBufferState = WRITE;
    FILE *tableFile = fopen (loadpath, "r");
    // while there are records, keep adding them to the DBFile. Reuse Add function.
    while(temp.SuckNextRecord(&f_schema, tableFile)==1) {
        Add(temp);
    }

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

int HeapDBFile :: GetNext (Record &fetchme) {
    if (fileObj.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if (prefPointer->pageBufferState == WRITE && pageObj.getNumRecs() > 0){
            if(!prefPointer->areAllRecordsWitten){
                fileObj.AddPage(&pageObj,GetPageLocationToReWrite());
            }
            //  Only Write Records if new records were added.
            pageObj.EmptyItOut();
            prefPointer->currentPageIndex = fileObj.GetLength();
            prefPointer->currentRecordState = pageObj.getNumRecs();
            return 0;
        }
        prefPointer->pageBufferState = READ;
        // loop till the page is empty and if empty load the next page to read
        if (!pageObj.GetFirst(&fetchme)) {
            // check if all records are read.
            if (prefPointer->currentPageIndex+1 >= fileObj.GetLength()){
               return 0;
            }
            else{
                // load new page and get its first record.
                fileObj.GetPage(&pageObj,GetPageLocationToRead(prefPointer->pageBufferState));
                pageObj.GetFirst(&fetchme);
                prefPointer->currentPageIndex++;
                prefPointer->currentRecordState = 0;
            }
        }
        // increament counter for each read.
        prefPointer->currentRecordState++;
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
int HeapDBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
        // Flush the Page Buffer if the WRITE mode was active.
        if (prefPointer->pageBufferState == WRITE && pageObj.getNumRecs() > 0){
            //  Only Write Records if new records were added.
            if(!prefPointer->areAllRecordsWitten){
                fileObj.AddPage(&pageObj,GetPageLocationToReWrite());
            }
            pageObj.EmptyItOut();
            prefPointer->currentPageIndex = fileObj.GetLength();
            prefPointer->currentRecordState = pageObj.getNumRecs();
            return 0;
        }
        bool readFlag ;
        bool compareFlag;
        // loop until all records are read or if some record maches the filter CNF
        do{
            readFlag = GetNext(fetchme);
            compareFlag = comparisonEngineObj.Compare (&fetchme, &literal, &cnf);
        }
        while(readFlag && !compareFlag);
        if(!readFlag){
            return 0;
        }
        return 1;

}

/**
    Next, Close simply closes the file. The return value is a 1 on success and a zero on failure.
**/
int HeapDBFile :: Close () {
    if (!fileObj.IsFileOpen()) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    if(prefPointer->pageBufferState == WRITE && pageObj.getNumRecs() > 0){
            if(!prefPointer->areAllRecordsWitten){
                if (prefPointer->reWritePage){
                    fileObj.AddPage(&this->pageObj,GetPageLocationToReWrite());
                    prefPointer->reWritePage = false;
                }
                else{
                    fileObj.AddPage(&this->pageObj,GetPageLocationToWrite());
                }
            }
            prefPointer->isPageFull = false;
            prefPointer->currentPageIndex = fileObj.Close();
            prefPointer->areAllRecordsWitten = true;
            prefPointer->currentRecordState = pageObj.getNumRecs();
    }
    else{
        if(prefPointer->pageBufferState == READ){
            prefPointer->currentPageIndex--;
        }
        fileObj.Close();
    }
    return 1;
}

/*===================================================================================*/


/*===================== Sorted DBFile Class Functions Definitions=======================*/
SortedDBFile :: SortedDBFile(Preference * preference){
    prefPointer = preference;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
    queryOrderMaker= NULL;
    doBinarySearch=true;
}

SortedDBFile::~SortedDBFile(){

}

/* MoveFirst () method implementation in DBFile is same as of heap file. In the heap file this function 
Forces the pointer to reset its position to the first record of the file. Each DBfile constitutes a pointer
 which points to the first record in the file. This pointer can move with respect to retrievals of records 
 from the file. In a situation where the the page buffer is not empty and the DBFIle is in write mode .
 Then in that case the non-empty page buffer is first written into dbfile , the first page is read and 
 the first record from the page is fetched.in other conditions the pointer is moved towards first page 
 and it is read.*/
void SortedDBFile::MoveFirst () {
    if (fileObj.IsFileOpen()){
         // Flush the Page Buffer if the WRITE mode was active.
        if(prefPointer->pageBufferState == WRITE && !prefPointer->areAllRecordsWitten){
                  MergeSortedInputWithFile();
        }
        if( pageObj.getNumRecs() > 0){
            pageObj.EmptyItOut();
        }
        prefPointer->pageBufferState = READ;
        fileObj.MoveToFirst();
        prefPointer->currentPageIndex = 0;
        prefPointer->currentRecordState = 0;
        doBinarySearch=true;
    }
}

/*The class contains an attribute which is an input pipe which is used to write records into BigQ class objects. 
This Add function is used to write records in the input pipe. Incase if the buffer is set to read mode then 
before the records are written into pipe the buffer is set to the write mode. Once the buffer is set to write 
mode then sorted records form the output pipe of BigQ class objects are written into input pipe.*/
void SortedDBFile :: Add(Record &addme){
    if (!fileObj.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }
    
    
    // assign new pipe instance for input pipe if null
    if (inputPipePtr == NULL){
        inputPipePtr = new Pipe(10);
    }
    if(outputPipePtr == NULL){
        outputPipePtr = new Pipe(10);
    }
    if(bigQPtr == NULL){
        bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(prefPointer->orderMaker), prefPointer->SortRunLength);
    }

    
     // Flush the page data from which you are reading and load the last page to start appending records.
     if (prefPointer->pageBufferState == READ ) {
        if( pageObj.getNumRecs() > 0){
            pageObj.EmptyItOut();
        }
        // assign new pipe instance for input pipe if null
        if (inputPipePtr == NULL){
            inputPipePtr = new Pipe(10);
        }
        if(outputPipePtr == NULL){
            outputPipePtr = new Pipe(10);
        }
        if(bigQPtr == NULL){
            bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(prefPointer->orderMaker), prefPointer->SortRunLength);
        }
    }
    
    // set DBFile in write mode
    prefPointer->pageBufferState = WRITE;
    
    // add record to input pipe
    inputPipePtr->Insert(&addme);
    
    // set allrecords written as false
    prefPointer->areAllRecordsWitten=false;
}

/*The load function can add records into input pipe in bulk. Same as add function load function implements the 
same functionality the only difference is that the load function incorporates a loop to add records in bulk over 
add function which can only add one record at a time. the loop sucks records from the raw.tbl file, and till all 
records are not written load function while loop keeps on running.*/
void SortedDBFile :: Load(Schema &myschema, const char *loadpath){
    if (!fileObj.IsFileOpen()){
          cerr << "Trying to load a file which is not open!";
          exit(1);
      }

      Record temp;
      // Flush the page data from which you are reading and load the last page to start appending records.
       if (prefPointer->pageBufferState == READ ) {
              if( pageObj.getNumRecs() > 0){
                  pageObj.EmptyItOut();
              }
              // assign new pipe instance for input pipe if null
               if (inputPipePtr == NULL){
                    inputPipePtr = new Pipe(10);
               }
      }
    
      // set DBFile in WRITE Mode
      prefPointer->pageBufferState = WRITE;
      FILE *tableFile = fopen (loadpath, "r");
      // while there are records, keep adding them to the DBFile. Reuse Add function.
      while(temp.SuckNextRecord(&myschema, tableFile)==1) {
          Add(temp);
      }
}

/*This method GetNext() gets records from the dbfiles iteratively. The persistent dbfile (.bin file) contains 
various pages which in turn contain records. the GetNext() method reads the pages present in the persistant 
file one by one and fetches all the records present in these pages iteratively.when all the records are read 
from the file then this function returns 0.*/
int SortedDBFile :: GetNext(Record &fetchme){
    if (fileObj.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if(prefPointer->pageBufferState == WRITE && !prefPointer->areAllRecordsWitten){
                   MergeSortedInputWithFile();
        }
        prefPointer->pageBufferState = READ;
        // loop till the page is empty and if empty load the next page to read
        if (!pageObj.GetFirst(&fetchme)) {
            // check if all records are read.
            if (prefPointer->currentPageIndex+1 >= fileObj.GetLength()){
               return 0;
            }
            else{
                // load new page and get its first record.
                fileObj.GetPage(&pageObj,GetPageLocationToRead(prefPointer->pageBufferState));
                pageObj.GetFirst(&fetchme);
                prefPointer->currentPageIndex++;
                prefPointer->currentRecordState = 0;
            }
        }
        // increament counter for each read.
        prefPointer->currentRecordState++;
        return 1;
    }
}

/*This version of GetNext method takes three arguments. One of the arguments is the CNF predicate. using a 
function like GetSortOrders of CNF class this given CNF is converted to an OrderMaker object. Then this given 
CNF is compared with the existing sorting order of dbfile. The attributes of ordermakers can be accommodated 
with the sort order of DBfile then in that case BinarySearch function is used to find any record that satisfy 
that CNF. We keep getting such records for each of the subsequent calls to this function.*/
int SortedDBFile :: GetNext(Record &fetchme, CNF &cnf, Record &literal){
    
    if (fileObj.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if(prefPointer->pageBufferState == WRITE && !prefPointer->areAllRecordsWitten){
            MergeSortedInputWithFile();
        }
        
        // set page mode to READ
        prefPointer->pageBufferState = READ;


        if(doBinarySearch){
            // read the current page for simplicity.
            off_t startPage = !prefPointer->currentPageIndex?0:prefPointer->currentPageIndex-1;
            // loop till the current page is fully read and checked record by record. Once the current page changes or the file ends break from the loop
            while(GetNext(fetchme)){
                if (startPage != prefPointer->currentPageIndex-1){
                    break;
                }
                if(comparisonEngineObj.Compare(&fetchme, &literal, &cnf)){
                    return 1;
                }
            }
            // has the value of the next page{
            if(prefPointer->currentPageIndex == fileObj.GetLength()-1){
                startPage = prefPointer->currentPageIndex;
            }
            else{
                startPage= (prefPointer->currentPageIndex-1);
            }
            
            // fetch the queryOrderMaker using the cnf given and the sort ordermaker stored in the preference.
            queryOrderMaker= cnf.GetQueryOrderMaker(*prefPointer->orderMaker);

            // if the queryOrderMaker is available do a binary search to find the first matching record which is equal to literal using queryOrderMaker.
            if(queryOrderMaker!=NULL){
                off_t endPage = (fileObj.GetLength())-2;
                int retstatus;
                if(endPage > 0){
                    retstatus = BinarySearch(fetchme,literal,startPage,endPage);
                }
                else{
                    retstatus = GetNext(fetchme);
                }
                if(retstatus){
                    doBinarySearch = false;
                    if (retstatus == 1){
                        if(comparisonEngineObj.Compare(&fetchme, &literal, &cnf))
                            return 1;
                    }

                }
            }
            else
            {
                doBinarySearch = false;
            }

            //If the first record of binary search gives a match we cannot be sure that the previous page is not a match. So go Back from the returned page from the binary search untill you find a page which doesnot match.
            // remove page and make space for the correct page to be read in.
            pageObj.EmptyItOut();
            int previousPage = prefPointer->currentPageIndex-1;
            Page prevPage;
            bool brkflag=false;
            while(previousPage>=startPage && queryOrderMaker!=NULL)
            {
                fileObj.GetPage(&prevPage,previousPage);
                prevPage.GetFirst(&fetchme);
                
                if(comparisonEngineObj.Compare(&literal, queryOrderMaker, &fetchme, prefPointer->orderMaker)==0)
                {
                    previousPage--;
                }
                else
                {
                    while(prevPage.GetFirst(&fetchme))
                    {
                        if(comparisonEngineObj.Compare (&fetchme, &literal, &cnf))
                        {
                            char *bits = new (std::nothrow) char[PAGE_SIZE];
                            pageObj.EmptyItOut();
                            prevPage.ToBinary (bits);
                            pageObj.FromBinary(bits);
                            prefPointer->currentPageIndex = previousPage+1;
                            return 1;
                        }
                    }
                    brkflag=true;
                }
                if(brkflag)
                {
                    prefPointer->currentPageIndex = previousPage+1;
                    break;
                }
            }
            //spcal case
            if(brkflag==true)
            {
            }

        }

        // Doing a linear Scan from the Record from where the seach stopped.
        while (GetNext(fetchme))
        {
            // if queryOrderMaker exists and the literal is smaller than the fetched record stop the seach as all other records are greter incase of the search.
            if (queryOrderMaker!=NULL && comparisonEngineObj.Compare(&literal,queryOrderMaker,&fetchme, prefPointer->orderMaker) < 0){
                return 0;
            }
            // if the record passes the CNF compare return.
            if(comparisonEngineObj.Compare (&fetchme, &literal, &cnf))
            {
                return 1;
            }
        }
        // if nothing matches and we read all the file.
        return 0;
            
    }
}

/*This function Close () simply closes the dbfile (.bin file). when the program has completely written the state 
of dbfile in a .pref file (preference file). the order maker object is also written in the state in the case of 
sorted DB File. it is done so that the sort order of the file could be known when it is read.*/
int SortedDBFile :: Close(){
    if (!fileObj.IsFileOpen()) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    
    if(prefPointer->pageBufferState == WRITE && !prefPointer->areAllRecordsWitten){
            MergeSortedInputWithFile();
            prefPointer->isPageFull = false;
            prefPointer->currentPageIndex = fileObj.Close();
            prefPointer->areAllRecordsWitten = true;
            prefPointer->currentRecordState = pageObj.getNumRecs();
    }
    else{
        if(prefPointer->pageBufferState == READ){
            prefPointer->currentPageIndex--;
        }
        fileObj.Close();
    }
    return 1;
    
}

/*When the program switches from write mode to the read mode then this function is used. In this state when the 
mode is switched then the records wich are already present in the BigQ class object have to be merged with the 
already sorted file. This function is more of a merge step which merges two sorted sources together. A temporary 
file is used for writing the merge stream.this temporary file is renamed to original dbfile.*/
void SortedDBFile::MergeSortedInputWithFile(){
    // setup for new file
    string fileName(prefPointer->prefFileDir);
    string newFileName = fileName.substr(0,fileName.find_last_of('.'))+".nbin";
    char* new_f_path = new char[newFileName.length()+1];
    strcpy(new_f_path, newFileName.c_str());
    
    // open new file in which the data will be written.
    newFile.Open(0,new_f_path);
    Page page;
    int newFilePageCounter = 0;
    
    // shut down input pipe;
    inputPipePtr->ShutDown();
    
    // flush the read buffer
    if(pageObj.getNumRecs() > 0){
        pageObj.EmptyItOut();
    }
    
    // page counts;
    int totalPages = fileObj.GetLength()-1;
    int currentPageIndex = 0;
    // set flags to initiate merge
    bool fileReadFlag = true;
    bool outputPipeReadFlag = true;
    
    
    // merge data from file and output pipe
    bool getNextFileRecord = true;
    bool getNextOutputPipeRecord = true;
    Record * fileRecordptr = NULL;
    Record * outputPipeRecordPtr = NULL;
    
    // loop until data is there in either the pipe or file.
    while(fileReadFlag || outputPipeReadFlag){
        
        if (getNextOutputPipeRecord){
            outputPipeRecordPtr = new Record();
            getNextOutputPipeRecord = false;
            if(!outputPipePtr->Remove(outputPipeRecordPtr)){
                outputPipeReadFlag= false;
            }
        }
        
        if (getNextFileRecord){
            fileRecordptr=new Record();
            getNextFileRecord = false;
            if(!pageObj.GetFirst(fileRecordptr)){
                if(currentPageIndex<totalPages){
                    fileObj.GetPage(&pageObj,currentPageIndex);
                    pageObj.GetFirst(fileRecordptr);
                    currentPageIndex++;
                }
                else{
                    fileReadFlag= false;
                }
            }
        }
        
        // select record to be written
        Record * writeRecordPtr;
        bool consumeFlag = false;
        if(fileReadFlag and outputPipeReadFlag){
            if (comparisonEngineObj.Compare(fileRecordptr,outputPipeRecordPtr,prefPointer->orderMaker)<=0){
                writeRecordPtr = fileRecordptr;
                fileRecordptr=NULL;
                consumeFlag=true;
                getNextFileRecord=true;
            }
            else{
                writeRecordPtr = outputPipeRecordPtr;
                outputPipeRecordPtr=NULL;
                consumeFlag=true;
                getNextOutputPipeRecord=true;
            }
        }
        else if (fileReadFlag){
            writeRecordPtr = fileRecordptr;
            fileRecordptr=NULL;
            consumeFlag=true;
            getNextFileRecord=true;

        }
        else if (outputPipeReadFlag){
            writeRecordPtr = outputPipeRecordPtr;
            outputPipeRecordPtr=NULL;
            consumeFlag=true;
            getNextOutputPipeRecord=true;
        }
        
        
        if(consumeFlag && !page.Append(writeRecordPtr)) {
            // Add the page to new File
            newFile.AddPage(&page,newFilePageCounter);
            newFilePageCounter++;
            
            // empty page if not consumed
            if(page.getNumRecs() > 0){
                page.EmptyItOut();
            }
            // add again to page
            page.Append(writeRecordPtr);
        }
        if(writeRecordPtr!=NULL){
            delete writeRecordPtr;
        }
    }
    
    // write the last page which might not be full.
    if (page.getNumRecs() > 0){
        newFile.AddPage(&page,newFilePageCounter);
    }
    
    // set that all records in the input pipe buffer are written
    prefPointer->areAllRecordsWitten=true;
    
    // clean up the input pipe, output pipe and bigQ after use.
    delete inputPipePtr;
    delete outputPipePtr;
    delete bigQPtr;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
    
    // close the files and swap the files
    fileObj.Close();
    newFile.Close();
    
    string oldFileName = fileName.substr(0,fileName.find_last_of('.'))+".bin";
    char* old_f_path = new char[oldFileName.length()+1];
    strcpy(old_f_path, oldFileName.c_str());
    
    // deleting old file
    if(Utilities::checkfileExist(old_f_path)) {
        if( remove(old_f_path) != 0 )
        cerr<< "Error deleting file" ;
    }
    // renaming new file to old file.
    rename(new_f_path,old_f_path);
    
    //opening the new file.
    fileObj.Open(1,old_f_path);
}

/*This function is a function which is used in GetNext() method which also accepts a CNF as an argument. Binary 
search is applied on the pages of DBFile.The given CNF is compared with the existing sorting order of dbfile. 
The attributes of ordermakers can be accommodated with the sort order of DBfile then in that case BinarySearch 
function is used to find any record that satisfy that CNF. This function returns a 0 if no record is found 
satisfying the CNF.*/
int SortedDBFile::BinarySearch(Record &fetchme, Record &literal,off_t low, off_t high){
    off_t mid = low + (high - low)/2;
    while(low <= high){
           if(mid != 0)
                  fileObj.GetPage(&pageObj,mid);
           else
                  fileObj.GetPage(&pageObj,0);
           pageObj.GetFirst(&fetchme);
           int comparisonResult = comparisonEngineObj.Compare(&literal, queryOrderMaker, &fetchme, prefPointer->orderMaker);

           if (comparisonResult == 0 ){
               prefPointer->currentPageIndex = mid+1;
               return 2;
           }
           else if (comparisonResult<0){
               high = mid - 1;
           }
           if (comparisonResult > 0){
                while(pageObj.GetFirst(&fetchme)){
                    int comparisonResultInsidePage = comparisonEngineObj.Compare(&literal, queryOrderMaker,&fetchme, prefPointer->orderMaker);
                    if(comparisonResultInsidePage<0){
                        return 0;
                    }
                    else if(comparisonResultInsidePage == 0 ){
                        prefPointer->currentPageIndex = mid+1;
                        return 1;
                    }
                }
                low = mid+1;
           }
          // calculate new mid using new low and high
           mid = low+(high - low)/2;
       }
       return 0;
}

/*=====================================================================================*/


/*===================== DBFile Class Functions Definitions=============================*/

// Constructor and Destructor
DBFile::DBFile () {
    fileObjPtr = NULL;
}

DBFile::~DBFile () {
    delete fileObjPtr;
    fileObjPtr = NULL;
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
int DBFile::Create (const char *f_path, fType fileType, void *startup) {
    if (Utilities::checkfileExist(f_path)) {
        cout << "file you are about to create already exists!"<<endl;
        return 0;
    }
    // changing .bin extension to .pref for storing preferences.
    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    // loading preferences
    LoadPreference(finalString,fileType);
    
    // check if the file type is correct
    if (fileType == heap){
        fileObjPtr = new HeapDBFile(&myPreference);
        fileObjPtr->Create((char *)f_path,fileType,startup);
        return 1;
    }
    else if(fileType == sorted){
        fileObjPtr = new SortedDBFile(&myPreference);
        fileObjPtr->Create((char *)f_path,fileType,startup);
        return 1;
    }
    return 0;
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

    // loading preferences
    LoadPreference(finalString,undefined);
    
    
    // check if the file type is correct
    if (myPreference.fileType == heap){
        fileObjPtr = new HeapDBFile(&myPreference);
    }
    else if(myPreference.fileType == sorted){
        fileObjPtr = new SortedDBFile(&myPreference);
    }
    // opening file using given path
    return fileObjPtr->Open((char *)f_path);
}

/**
    This function "Add" adds records to end of the the file, There are then two functions that allow
    for record retrieval from a DBFile instance; all are
    called GetNext.
**/
void DBFile::Add (Record &rec) {
    if (fileObjPtr!=NULL){
        fileObjPtr->Add(rec);
    }
}

//  Function to directly load data from the tbl files.
/**
    The Load function bulk loads the DBFile instance from a text file, appending new data
    to it using the SuckNextRecord function from Record.h. The character string passed to Load is
    the name of the data file to bulk load.
**/
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    if (fileObjPtr!=NULL){
           fileObjPtr->Load(f_schema,loadpath);
    }
}

/**
    Echa Dbfile consititutes a pointer which points to the first record in the file.
    This pointer can move  with respect to retreivals of records from the file.
    This function Forces the pointer to reset its position to the first record of the file
**/
void DBFile::MoveFirst () {
    if (fileObjPtr!=NULL){
           fileObjPtr->MoveFirst();
    }
}

/**
    Next, Close simply closes the file. The return value is a 1 on success and a zero on failure.
**/

int DBFile::Close () {
    if (fileObjPtr!=NULL){
        if (fileObjPtr->Close()){
            DumpPreference();
            return 1;

        }
        else{
            return 0;
        };
    }
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
    if (fileObjPtr != NULL){
        return fileObjPtr->GetNext(fetchme);
    }
    return 0;
}

/**
    The next version of GetNext also accepts a selection predicate
    (this is a conjunctive normal form expression). It returns the
    next record in the file that is accepted by the selection predicate.
    The literal record is used to check the selection predicate, and is
    created when the parse tree for the CNF is processed.
**/
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if (fileObjPtr != NULL){
        return fileObjPtr->GetNext(fetchme,cnf,literal);
    }
    return 0;
}

// Function to load preference from the disk. This is needed to make read and writes persistent.
// @input - the file path of the table to be created or opened. Each tbl file has a corresponding .pref
void DBFile::LoadPreference(char * newFilePath,fType fileType) {
    ifstream file;
    if (Utilities::checkfileExist(newFilePath)) {
        file.open(newFilePath,ios::in);
        if(!file){
            cerr<<"Error in opening file..";
            exit(1);
        }
        file.read((char*)&myPreference,sizeof(Preference));
        myPreference.prefFileDir = (char*)malloc(strlen(newFilePath) + 1);
        strcpy(myPreference.prefFileDir,newFilePath);
        if (myPreference.fileType == sorted){

        myPreference.orderMaker = new OrderMaker();
        file.read((char*)myPreference.orderMaker,sizeof(OrderMaker));
        }
    }
    else {
        myPreference.fileType = fileType;
        myPreference.prefFileDir = (char*) malloc(strlen(newFilePath) + 1);
        strcpy(myPreference.prefFileDir,newFilePath);
        myPreference.currentPageIndex = 0;
        myPreference.currentRecordState = 0;
        myPreference.isPageFull = false;
        myPreference.pageBufferState = IDLE;
        myPreference.reWritePage= false;
        myPreference.areAllRecordsWitten = true;
        myPreference.orderMaker = NULL;
        myPreference.SortRunLength = 0;
    }
}

// Function to dumpn the preference to the disk. This is needed to make read and writes persistent.
void DBFile::DumpPreference(){
    ofstream file;
    file.open(myPreference.prefFileDir,ios::out);
    if(!file) {
        cerr<<"Error in opening file for writing.."<<endl;
        exit(1);
    }
    file.write((char*)&myPreference,sizeof(Preference));
    if (myPreference.fileType == sorted){
        file.write((char*)myPreference.orderMaker,sizeof(OrderMaker));
    }
    file.close();
}

/*-----------------------------------END--------------------------------------------*/
