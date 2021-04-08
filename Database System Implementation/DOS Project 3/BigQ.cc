#include "BigQ.h"
#include "Utilities.h"
using namespace std;




// Class used to sort the heap binary files.
// ------------------------------------------------------------------
void* BigQ :: Driver(void *p){
  BigQ * ptr = (BigQ*) p;
  //   function to implement phase1 of TPMMS algorithm
  ptr->TPMMSPhase1();
  //   function to implement phase2 of TPMMS algorithm
  ptr->TPMMSPhase2();
  ptr->ThreadDataObj.out->ShutDown();
}

//   function to implement phase1 of TPMMS algorithm
void BigQ :: TPMMSPhase1()
{
    Record tRec;
    run tRun(this->ThreadDataObj.runlen,this->ThreadDataObj.sortorder,this->f_path);

    // add 1 page for adding records
    long long int pageCount=0;
    long long int runCount=1;
    tRun.AddPage();
    bool diff = false;

    // read data from in pipe sort them into runlen pages

    while(this->ThreadDataObj.in->Remove(&tRec)) {
        if(!tRun.addRecordAtPage(pageCount, &tRec)) {
            if (tRun.checkRunFull()) {
                sortCompleteRun(&tRun, this->ThreadDataObj.sortorder);
                diff = tRun.writeRunToFile(&this->myFile);
                if (diff){
                    pageCount= 0;
                    runCount++;
                    tRun.addRecordAtPage(pageCount, &tRec);
                }
                else{
                    tRun.clearPages();
                    tRun.AddPage();
                    pageCount = 0;
                    tRun.addRecordAtPage(pageCount, &tRec);
                }
            }
            else{
                tRun.AddPage();
                pageCount++;
                tRun.addRecordAtPage(pageCount, &tRec);
            }
        }
    }
    if(tRun.getRunSize()!=0) {
        sortCompleteRun(&tRun, this->ThreadDataObj.sortorder);
        tRun.writeRunToFile(&this->myFile);
        tRun.clearPages();
    }
    this->f_path = "temp.xbin";
    this->totalNumberOfRuns = runCount;
}

// sort runs from file using Run Manager
void BigQ :: TPMMSPhase2()
{
    RunManager runManager(this->ThreadDataObj.runlen,this->f_path);
    TreeObj = new TournamentTree(&runManager,this->ThreadDataObj.sortorder);
    Page * tempPage;
    while(TreeObj->GetSortedPageAndRefill(&tempPage)){
        Record tempRecord;
        while(tempPage->GetFirst(&tempRecord)){
            this->ThreadDataObj.out->Insert(&tempRecord);
        }
        TreeObj->RefillOutputBufferUsingRunManager();
    }
}

void BigQ::sortCompleteRun(run *run, OrderMaker *sortorder) {
    TreeObj = new TournamentTree(run,sortorder);
    Page * tempPage;
    // as run was swapped by tournament tree
    // we need to allocate space for pages again
    // these pages will be part of complete sorted run
    // add 1 page for adding records
    while(TreeObj->GetSortedPageForRunAndRefill(&tempPage)){
        Record tempRecord;
        Page * pushPage = new Page();
        while(tempPage->GetFirst(&tempRecord)){
            pushPage->Append(&tempRecord);
        }
        run->AddPage(pushPage);
        TreeObj->RefillOutputBuffer();
    }
    delete TreeObj;TreeObj=NULL;
}

// BigQ constructor
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    ThreadDataObj.in = &in;
    ThreadDataObj.out = &out;
    ThreadDataObj.sortorder = &sortorder;
    ThreadDataObj.runlen = runlen;
    TreeObj=NULL;f_path=NULL;
    pthread_create(&pThreadObj, NULL, BigQ::Driver,this);
    //pthread_join(myThread, NULL);
    //out.ShutDown ();
}

// BigQ destructor
BigQ::~BigQ () {
    if(Utilities::checkfileExist("temp.xbin")) {
        if( remove( "temp.xbin" ) != 0 )
        cerr<< "Error deleting file" ;
    }
}
// ------------------------------------------------------------------

// ------------------------------------------------------------------

//constructor for run class
run::run(int runlen ,char * f_path) {
    this->f_path = f_path;
    this->runSequenceLength = runlen;
    this->sortorder = NULL;
}

//constructor for run class
run::run(int runlen,OrderMaker * sortorder,char * f_path) {
    this->f_path = f_path;
    this->runSequenceLength = runlen;
    this->sortorder = sortorder;
}

void run::AddPage() {
    this->pages.push_back(new Page());
}

//this function is method to add page
void run::AddPage(Page *p) {
    this->pages.push_back(p);
}

//this function is getter method to vector of pages For Priority Queue.
vector<Page*> run::getPages() {
    return this->pages;
}

//this function is getter method to getPages For Priority Queue.
void run::getPages(vector<Page*> * PageVectorObj) {
    PageVectorObj->swap(this->pages);
}

//this function is method to check if the run if full.
bool run::checkRunFull() {
    return this->pages.size() == this->runSequenceLength;
}

//this function is method to clear pages.
bool run::clearPages() {
    this->pages.clear();
}

//This function is getter method to get runSize.
int run::getRunSize() {
    return this->pages.size();
}

//this function is method to add record to the page.
int run::addRecordAtPage(long long int pageCount, Record *rec) {
    return this->pages.at(pageCount)->Append(rec);
}


//this function is method to writeRun to File after Sorting.
int run::writeRunToFile(File *file) {
    int writeLocation=0;
    if(!Utilities::checkfileExist("temp.xbin")) {
        file->Open(0,"temp.xbin");
    }
    else{
        file->Open(1,"temp.xbin");
        writeLocation=file->GetLength()-1;
    }
    int loopend = pages.size()>runSequenceLength ? runSequenceLength:pages.size();
    bool difference = false;
    for(int i=0;i<loopend;i++) {
        Record tempRecord;
        Page tempPage;
        while(pages.at(i)->GetFirst(&tempRecord)) {
            tempPage.Append(&tempRecord);
        }
        //write this page to file
        file->AddPage(&tempPage,writeLocation);writeLocation++;
    }
    if(pages.size()>runSequenceLength){
        Page *lastPage = new Page();
        Record temp;
        while(pages.back()->GetFirst(&temp)) {
            lastPage->Append(&temp);
        }
        this->clearPages();
        this->AddPage(lastPage);
        difference=true;
    }
    file->Close();
    return difference;
}
// ------------------------------------------------------------------


// RUn Manager Class for managing runs
// ------------------------------------------------------------------

RunManager :: RunManager(int runSequenceLength,char * f_path){
    this->runSequenceLength = runSequenceLength;
    this->f_path = f_path;
    this->file.Open(1,this->f_path);
    int totalNumberOfPages = file.GetLength()-1;
    this->numberOfRuns = totalNumberOfPages/runSequenceLength + (totalNumberOfPages % runSequenceLength != 0);
    this->totalNumberOfPages = totalNumberOfPages;
    int pageOffset = 0;
    for(int i = 0; i<numberOfRuns;i++){
        RunFileObj fileObject;
        fileObject.runIdValue = i;
        fileObject.startPageInd = pageOffset;
        fileObject.currentPageInd = fileObject.startPageInd;
        pageOffset+=runSequenceLength;
        if (pageOffset <=totalNumberOfPages){
            fileObject.endPageInd = pageOffset-1;
        }
        else{
            fileObject.endPageInd =  fileObject.startPageInd + (totalNumberOfPages-fileObject.startPageInd-1);
        }
        runLocation.insert(make_pair(i,fileObject));
    }

}

//  This Function is a getter method to get Inital Set of Pages
void  RunManager:: getPages(vector<Page*> * PageVectorObj){
    for(int i = 0; i< numberOfRuns ; i++){
        unordered_map<int,RunFileObj>::iterator runGetter = runLocation.find(i);
        if(!(runGetter == runLocation.end())){
            Page * pagePtr = new Page();
            this->file.GetPage(pagePtr,runGetter->second.currentPageInd);
            runGetter->second.currentPageInd+=1;
            if(runGetter->second.currentPageInd>runGetter->second.endPageInd){
                runLocation.erase(i);
            }
            PageVectorObj->push_back(pagePtr);
        }
    }
}

//  This Function is a getter method to get Next Page for a particular Run
bool RunManager :: getNextPageOfRun(Page * page,int runNo){
    unordered_map<int,RunFileObj>::iterator runGetter = runLocation.find(runNo);
    if(!(runGetter == runLocation.end())){
        this->file.GetPage(page,runGetter->second.currentPageInd);
        runGetter->second.currentPageInd+=1;
        if(runGetter->second.currentPageInd>runGetter->second.endPageInd){
            runLocation.erase(runNo);
        }
        return true;
    }
    return false;
}

RunManager :: ~RunManager(){
    file.Close();
}


//getter for number of runs
int RunManager :: getNoOfRuns()
{
  return numberOfRuns;
}


//getter for run length
int RunManager ::getRunLength()
{
  return runSequenceLength;
}

//getter for total pages
int RunManager :: getTotalPages()
{
  return totalNumberOfPages;
}


// ------------------------------------------------------------------


// Class used to sort the records within a run or across runs using priority queue.
// ------------------------------------------------------------------
TournamentTree :: TournamentTree(run * run,OrderMaker * sortorder){
    OrderMakerObj = sortorder;
    myQueue = new priority_queue<QueueObj,vector<QueueObj>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = false;
    run->getPages(&PageVectorObj);
    InititateQueueForRun();
}

//  This Function initiates and processes the queue with records pulled from run
void TournamentTree :: InititateQueueForRun(){
    if (!PageVectorObj.empty()){
        int runIdValue = 0;
        for(vector<Page*>::iterator i = PageVectorObj.begin() ; i!=PageVectorObj.end() ; ++i){
            Page * page = *(i);
            while(1)
            {
                QueueObj object;
                object.record = new Record();
                object.runIdValue = runIdValue;
                if(!page->GetFirst(object.record)){
                    break;
                }
                myQueue->push(object);
            }
            runIdValue++;
        }

        while(!myQueue->empty()){
            QueueObj topObject = myQueue->top();
            bool OutputBufferFull = !(PageOutputBuffer.Append(topObject.record));
            if (OutputBufferFull){
                break;
            }
            myQueue->pop();
        }

    }
}

//  this function to get sorted output buffer and refill buffer again
bool TournamentTree :: GetSortedPageForRunAndRefill(Page ** page){
    if (PageOutputBuffer.getNumRecs()>0){
        *(page) = &PageOutputBuffer;
        return 1;
    }
    return 0;
}


//  this function is a getter for refill output buffer using Run.
void TournamentTree :: RefillOutputBuffer(){
     if (!PageVectorObj.empty()){
         while(!myQueue->empty()){
             QueueObj topObject = myQueue->top();
             bool OutputBufferFull = !(PageOutputBuffer.Append(topObject.record));
             if (OutputBufferFull){
                 break;
             }
             myQueue->pop();
         }
     }
}


//constructor to create tournamentTree
TournamentTree :: TournamentTree(RunManager * manager,OrderMaker * sortorder){
    OrderMakerObj = sortorder;
    RunManagerObj = manager;
    myQueue = new priority_queue<QueueObj,vector<QueueObj>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = true;
    RunManagerObj->getPages(&PageVectorObj);
    InititateQueue();
}



//  This Function initiates and processes the queue with records pulled from runManager
void TournamentTree :: InititateQueue(){
    if (!PageVectorObj.empty()){
        int runIdValue = 0;
        for(vector<Page*>::iterator i = PageVectorObj.begin() ; i!=PageVectorObj.end() ; ++i){
            Page * page = *(i);
            QueueObj object;
            object.record = new Record();
            object.runIdValue = runIdValue++;
            if(page->GetFirst(object.record)){
                myQueue->push(object);
            }
        }

        while(!myQueue->empty()){
            QueueObj topObject = myQueue->top();
            if(isRunManagerAvailable){
            }

            bool OutputBufferFull = !(PageOutputBuffer.Append(topObject.record));
            if (OutputBufferFull){
                break;
            }
            myQueue->pop();
            Page * topPage = PageVectorObj.at(topObject.runIdValue);
            if (!(topPage->GetFirst(topObject.record))){
                if (isRunManagerAvailable&&RunManagerObj->getNextPageOfRun(topPage,topObject.runIdValue)){
                    if(topPage->GetFirst(topObject.record)){
                        myQueue->push(topObject);
                    }
                }
            }
            else{
                myQueue->push(topObject);
            }
        }

    }
}



//  This method refills the output buffer using RunManger.
void TournamentTree :: RefillOutputBufferUsingRunManager(){
    if (!PageVectorObj.empty()){
        int runIdValue = 0;
        while(!myQueue->empty()){
            QueueObj topObject = myQueue->top();
            bool OutputBufferFull = !(PageOutputBuffer.Append(topObject.record));
            if (OutputBufferFull){
                break;
            }
            myQueue->pop();

            Page * topPage = PageVectorObj.at(topObject.runIdValue);
            if (!(topPage->GetFirst(topObject.record))){
                if (isRunManagerAvailable&&RunManagerObj->getNextPageOfRun(topPage,topObject.runIdValue)){
                    if(topPage->GetFirst(topObject.record)){
                        myQueue->push(topObject);
                    }
                }
            }
            else{
                myQueue->push(topObject);
            }

        }
    }
}

//  this function is a getter for sorted output buffer and this method also refills buffer again
bool TournamentTree :: GetSortedPageAndRefill(Page ** page){
    if (PageOutputBuffer.getNumRecs()>0){
        *(page) = &PageOutputBuffer;
        return 1;
    }
    return 0;
}






// ------------------------------------------------------------------




// Class to implement custom comparator for vector sorting and priority queue sorting.
// ------------------------------------------------------------------
CustomComparator :: CustomComparator(OrderMaker * sortorder){
    this->OrderMakerObj = sortorder;
}

//this method sorts the vector of records 
bool CustomComparator :: operator ()( QueueObj lhs, QueueObj rhs){
    int val = ComparisonEngineObj.Compare(lhs.record,rhs.record,OrderMakerObj);
    return (val <=0)? false : true;
}

//this function sorts the priority queue
bool CustomComparator :: operator ()( Record* lhs, Record* rhs){
    int val = ComparisonEngineObj.Compare(lhs,rhs,OrderMakerObj);
    return (val <0)? true : false;
}
// ------------------------------------------------------------------





