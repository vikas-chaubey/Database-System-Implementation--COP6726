#ifndef BIGQ_H
#define BIGQ_H
#include <iostream>
#include <pthread.h>
#include <queue>
#include <vector>
#include<unordered_map>
#include <algorithm>
#include "File.h"
#include "Pipe.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include "Comparison.h"
using namespace std;


// ------------------------------------------------------------------

// This structure is created to encapsulate Data for Runmanager
typedef struct{
    //variable  to save start page index
    int startPageInd;
    //variable  to save current page index
    int currentPageInd;
    //variable  to save end page index
    int endPageInd;
    //variable to save runID
    int runIdValue;
} RunFileObj;

// This structure is created to encapsulate Data for Priority Queue
typedef struct{
	//variable for runId
    int runIdValue;
    //Variable to save records
    Record * record;
} QueueObj;

// This structure is created to encapsulate Data Passed to BigQ's Constructor
typedef struct {
    Pipe * in;
    Pipe * out;
    OrderMaker * sortorder;
    int runlen;
} ThreadData;
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class run {
    //file path
    char * f_path;
	//variable to save run length
    int runSequenceLength;
    //orderMaker variable
    OrderMaker * sortorder;
    //vector to save pages
    vector <Page *> pages;
    public:
    	//constructor to instantiate run class 
        run(int runSequenceLength , char * f_path);
        //constructor to instantiate run class 
        run(int runSequenceLength,OrderMaker * sortorder , char * f_path);
        void AddPage();
        //this function is method to add page
        void AddPage(Page *p);
        //this function is method to add record to the page.
        int addRecordAtPage(long long int pageCount, Record *rec);
        //this function is method to check if the run if full.
        bool checkRunFull();
        //this function is method to clear pages.
        bool clearPages();
		//This function is getter method to get runSize.
        int getRunSize();
        vector<Page*> getPages();
		//this function is gtter method to getPages For Priority Queue.
        void getPages(vector<Page*> * pagevector);
        bool customRecordComparator(Record &left, Record &right);
        //this function is method to writeRun to File after Sorting.
        int writeRunToFile(File *file);

       


};
// ------------------------------------------------------------------


// ------------------------------------------------------------------
// Class to implement custom comparator for vector sorting and priority queue sorting.
class CustomComparator{
    //variable to save comparisonenginer object
    ComparisonEngine ComparisonEngineObj;
    //variable to save ordermaker
    OrderMaker * OrderMakerObj;
public:
    //constructor to create custom comparator class
    CustomComparator(OrderMaker * sortorder);
    //this method sorts the vector of records 
    bool operator()( Record* lhs,   Record* rhs);
    //this function sorts the priority queue
    bool operator()(QueueObj lhs,  QueueObj rhs);

};
// ------------------------------------------------------------------


// ------------------------------------------------------------------
// Class for managing runs
class RunManager{
	//variable to save number of runs
    int numberOfRuns;
    //variable to save run length
    int runSequenceLength;
    //variable to save total pages
    int totalNumberOfPages;
    //variable to save file object
    File file;
    //variable to save file path
    char * f_path;
    //unordered_map
    unordered_map<int,RunFileObj> runLocation;
public:
	//RunManager class cinstructor
    RunManager(int runSequenceLength,char * f_path);
//  This Function is a getter method to get Inital Set of Pages
    void getPages(vector<Page*> * PageVectorObj);
//  This Function is a getter method to get Next Page for a particular Run
    bool getNextPageOfRun(Page * page,int runNo);

    ~RunManager();
    int getNoOfRuns();
    int getRunLength();
    int getTotalPages();


};
// ------------------------------------------------------------------






// ------------------------------------------------------------------
// Class used to sort the records within a run or across runs using priority queue.
class TournamentTree{
	//variable to save ordermaker object
    OrderMaker * OrderMakerObj;
    //variable to save run manager object
    RunManager * RunManagerObj;
    //variable to save vector of pages
    vector<Page*> PageVectorObj;
    //variable to save pages
    Page PageOutputBuffer;
    // RUnManager availability checker
    bool isRunManagerAvailable;
    priority_queue<QueueObj,vector<QueueObj>,CustomComparator> * myQueue;
//  This Function initiates and processes the queue with records pulled from runManager
    void InititateQueue();
//  This Function initiates and processes the queue with records pulled from run
    void InititateQueueForRun();
public:
	//constructor to create tournamentTree
    TournamentTree(run * run,OrderMaker * sortorder);
    TournamentTree(RunManager * manager,OrderMaker * sortorder);
//  This method refills the output buffer using RunManger.
    void RefillOutputBufferUsingRunManager();
//  this function is a getter for sorted output buffer and this method also refills buffer again
    bool GetSortedPageAndRefill(Page * *p);
//  this function is a getter for refill output buffer using Run.
    void RefillOutputBuffer();
//  this function to get sorted output buffer and refill buffer again
    bool GetSortedPageForRunAndRefill(Page * *p);
};
// ------------------------------------------------------------------


// ------------------------------------------------------------------
// Class used to sort the heap binary files.
class BigQ {
	//variable to save thread data
     ThreadData ThreadDataObj;
     //variable to save tournament tree object
     TournamentTree * TreeObj;
     //variable to save pthread obj
     pthread_t pThreadObj;
     //variable to save total number of threads
     int totalNumberOfRuns;
     File myFile;
     char * f_path;
//   function to implement phase1 of TPMMS algorithm
     void TPMMSPhase1();
//   function to implement phase2 of TPMMS algorithm
     void TPMMSPhase2();

public:
    void sortCompleteRun(run *run, OrderMaker *sortorder);
    //   public static function to drive the TPMMS algorithm.
     static void* Driver(void*);
    //   constructor
     BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    //   destructor
     ~BigQ ();
};
// ------------------------------------------------------------------
#endif
