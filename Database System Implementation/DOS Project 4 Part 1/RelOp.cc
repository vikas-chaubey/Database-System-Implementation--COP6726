#include<sstream>
#include "RelOp.h"
#include "Utilities.h"

// this code segment defines a vector to erase vector memory.
#define CLEANUPVECTOR(v) \
	({ for(vector<Record *>::iterator it = v.begin(); it!=v.end(); it++) { \
		if(!*it) { delete *it; } }\
		v.clear();\
})

/*==============================================================================================*/
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {	
	// initialize all the class object variables with args
	//initializing DBFile object
	this->inFile = &inFile; 
	//initialize output pipe object
	this->outPipe = &outPipe;
	//initialize CNF object from CNF class using arguments
	this->selOp = &selOp; 	
	//initialize record object with given literal
	this->literal = &literal;
	// this function creates the executing thread
	pthread_create(&this->pthreadObj, NULL, caller, (void*)this);
}

// below are given auxiliary functions

// this function holds the calling entity till the executing relational operator has run to completion
void SelectFile::WaitUntilDone () { pthread_join (pthreadObj, NULL); }
// This function provides information regarding internal memory usage
void SelectFile::Use_n_Pages (int runlen) { return; }
void* SelectFile::caller(void *args) { ((SelectFile*)args)->operation(); }

// The operation function is called and executed by the thread.
void* SelectFile::operation() {
	int count=0;Record rec;ComparisonEngine cmp;
	inFile->MoveFirst();
	while(inFile->GetNext(rec)) {
		if (cmp.Compare(&rec, literal, selOp)) { outPipe->Insert(&rec);++count; }
	}
	outPipe->ShutDown();
}

/*==============================================================================================*/
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	//initialize input pipe object
	this->inPipe = &inPipe;	
	//initialize output pipe object
	this->outPipe = &outPipe;
	//initialize CNF object from CNF class using arguments
	this->selOp = &selOp;	
	//initialize record object with given literal
	this->literal = &literal;
	// this function creates the executing thread
	pthread_create(&this->pthreadObj, NULL, caller, (void *)this);
}

// below are given auxiliary functions

// this function holds the calling entity till the executing relational operator has run to completion
void SelectPipe::WaitUntilDone () { pthread_join (pthreadObj, NULL); }

// This function provides information regarding internal memory usage
void SelectPipe::Use_n_Pages (int n) { return; }
void* SelectPipe::caller(void *args) { ((SelectPipe*)args)->operation(); }

// The operation function is called and executed by the thread.
void* SelectPipe::operation() {
	Record rec;
	ComparisonEngine cmp;
	while(inPipe->Remove(&rec)) {
		if (cmp.Compare(&rec, literal, selOp)) { outPipe->Insert(&rec); }
	}
	outPipe->ShutDown();
}

/*==============================================================================================*/
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	//initialize input pipe object
	this->inPipe = &inPipe;	
	//initialize output pipe object
	this->outPipe = &outPipe;
	//initialize variable to keep counter
	this->keepMe = keepMe;	
	//initialize variable to hold integer number at TS input
	this->numAttsInput = numAttsInput;
	//initialize variable to hold integer number at TS output
	this->numAttsOutput = numAttsOutput;
	// this function creates the executing thread
	pthread_create(&this->pthreadObj, NULL, caller, (void *)this);
}

// below are given auxiliary functions
// this function holds the calling entity till the executing relational operator has run to completion
void Project::WaitUntilDone () { pthread_join (pthreadObj, NULL); }
// This function provides information regarding internal memory usage
void Project::Use_n_Pages (int n) { return; }
void* Project::caller(void *args) { ((Project*)args)->operation(); }

// function is called by the thread
void* Project::operation() {
	Record rec;
	while (inPipe->Remove(&rec)) { 
		rec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&rec);
	}
	outPipe->ShutDown();
}

/*==============================================================================================*/

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	//initiate input Pipe Object left
	this->inPipeL = &inPipeL;
	//initiate input Pipe Object right
	this->inPipeR = &inPipeR;
	//initiate output Pipe Object
	this->outPipe = &outPipe;
	//initiate CNF class variable for CNF statement processing
	this->selOp = &selOp;
	//initiate Variable to hold record literal
	this->literal = &literal;
	// this function creates the executing thread
	pthread_create(&pthreadObj, NULL, caller, (void *)this);

}
// this function holds the calling entity till the executing relational operator has run to completion
void Join::WaitUntilDone () { pthread_join (pthreadObj, NULL); }
// This function provides information regarding internal memory usage
void Join::Use_n_Pages (int n) { rl = n; }

//this function is a static caller method
void* Join::caller(void *args) { ((Join*)args)->operation(); }

//This method defines the main join operation in the class
void* Join::operation() {

	//initialize all the variables
	Record lr, rr, m; OrderMaker lom, rom;
	//obtain getsortorders values and create BigQ class object 
	selOp->GetSortOrders(lom,rom);
	// this if-else block chooses among sorted-merge join and  block-nested join as per Numnber of attributes
	 if(lom.getNumAtts()==rom.getNumAtts()!=0) { sortMergeJoin(lr,rr,m,lom,rom); }
	 else {
		blockNestedJoin(lr,rr,m,lom,rom); 
	 }
	//Pipe shutdown on completion
	outPipe->ShutDown();
}

//This method defines the Sort-Merge join operation in the class
void Join::sortMergeJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom) {
	
	//initialize all the variables
	Pipe spl(500), spr(500); ComparisonEngine ce;
	BigQ lq(*inPipeL, spl, lom, rl); 
	BigQ rq(*inPipeR, spr, rom, rl);

	// perform the sort and merge operations
	bool le=spl.Remove(&lr); bool re=spr.Remove(&rr);int c=0;int lc=1; int rc=1;
	while(le&&re) {
		int v = ce.Compare(&lr,&lom, &rr, &rom);
		if (v==-1)		{ le=spl.Remove(&lr);lc++;}
		else if	(v==1)	{ re=spr.Remove(&rr);rc++;}
		else {
			c++;
			vector<Record *> vl; vector <Record *> vr;
			Record *pushlr  = new Record();  pushlr->Consume(&lr);  vl.push_back(pushlr);
			Record *pushrr  = new Record();  pushrr->Consume(&rr);  vr.push_back(pushrr);

			le=spl.Remove(&lr);lc++;
			while(le && (!ce.Compare(&lr,pushlr,&lom))) {
				Record *tr = new Record(); tr->Consume(&lr); vl.push_back(tr);le=spl.Remove(&lr);lc++;
			}

			re=spr.Remove(&rr);rc++;
			while(re &&(!ce.Compare(&rr,pushrr,&rom))) {
				Record *tr = new Record(); tr->Consume(&rr); vr.push_back(tr);re=spr.Remove(&rr);rc++;
			}
			
			for(int i=0; i < vl.size(); i++){ for(int j=0; j< vr.size(); j++) { MergeRecord(vl[i], vr[j]); }}
			CLEANUPVECTOR(vl);CLEANUPVECTOR(vr);
		}
	}

	// drain the pipes and remove the records from it.
	while(spl.Remove(&lr)); while(spr.Remove(&rr));
}

//This method defines the block-Nested join operation in the class
void Join::blockNestedJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom) {

	//initialize all the variables
	ComparisonEngine ce; Record r, *trl, *trr; int c=0, lc=0, rc=0; Page *lp, *rp; 

	// Create Dbfile object and fill it with records and move to the first record
	DBFile dbf;
    char * fileName = Utilities::newRandomFileName(".bin");
    dbf.Create(fileName, heap, NULL);
	while(inPipeR->Remove(&r)) {
        ++c;
        dbf.Add(r);
    }
    dbf.Close();

    // Open the file with given filename and perform read operation
    dbf.Open(fileName);
    // move the pointer to the first record
    dbf.MoveFirst();
	if (!c) {
        while(inPipeL->Remove(&lr));
        return;
    }
    
// These are variables to perform block merge operation
	lp = new Page();
    rp = new Page();
	trl = new Record();
    trr = new Record();
	while(inPipeL->Remove(&lr)) {
		++lc;
		if (!lp->Append(&lr)) {
            
			Record * lpr; vector <Record *> lrvec;
            lpr = new Record();
            
            while(lp->GetFirst(lpr)){
				lrvec.push_back(lpr);
                lpr = new Record();
			}
            
            if (sizeof(lr.bits)) {
                lrvec.push_back(&lr);
            }
            
            lrc += lrvec.size();
            
            Record * rpr; vector <Record *> rrvec;
            rpr = new Record();
            
            while (dbf.GetNext(rr)) {

                ++rrc;
                if(!rp->Append(&rr)) {
                    while(rp->GetFirst(rpr)){
                        rrvec.push_back(rpr);
                        rpr = new Record();
                    }
                    if (sizeof(rr.bits)) {
                        rrvec.push_back(&rr);
                    }
                    for (int i=0; i < lrvec.size(); i++) {
                        for ( int j=0; j < rrvec.size();j++){
                            if (ce.Compare(lrvec[i], rrvec[j],literal, selOp)) {
                                ++mc;
                                MergeRecord(lrvec[i], rrvec[j]);
                            }
                        }
                    }
                    CLEANUPVECTOR(rrvec);
                }
            }
           
            dbf.MoveFirst();
            CLEANUPVECTOR(lrvec);
		}


	}
    if(lp->getNumRecs()){
        dbf.MoveFirst();
        Record * lpr; vector <Record *> lrvec;
        lpr = new Record();
        while(lp->GetFirst(lpr)){
            lrvec.push_back(lpr);
            lpr = new Record();
        }
        lrc += lrvec.size();
        Record * rpr; vector <Record *> rrvec;
        rpr = new Record();
        while (dbf.GetNext(rr)) {
            ++rrc;
            if(!rp->Append(&rr)) {
                 while(rp->GetFirst(rpr)){
                     rrvec.push_back(rpr);
                     rpr = new Record();
                 }
                
                 if (sizeof(rr.bits)) {
                     rrvec.push_back(&rr);
                 }
                for (int i=0; i < lrvec.size(); i++) {
                    for ( int j=0; j < rrvec.size();j++){
                            if (ce.Compare(lrvec[i], rrvec[j],literal, selOp)) {
                                ++mc;
                                MergeRecord(lrvec[i], rrvec[j]);
                            }
                    }
                }
                CLEANUPVECTOR(rrvec);
                
            }
        }
        dbf.MoveFirst();
        CLEANUPVECTOR(lrvec);
        
    }
    
    dbf.Close();
    
    if(Utilities::checkfileExist(fileName)) {
        if( remove(fileName) != 0 )
        cerr<< "Error deleting file" ;
    }
    string s(fileName);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char * finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    
    if(Utilities::checkfileExist(finalString)) {
        if( remove(finalString) != 0 )
        cerr<< "Error deleting file" ;
    }
	while(inPipeL->Remove(&lr));
	while(inPipeR->Remove(&rr));
}

//method to merge record pages
void Join::MergePages(vector <Record *> lrvec, Page *rp, OrderMaker &lom, OrderMaker &rom) {
	
	rrc += rp->getNumRecs();
	
	Record rpr;
	ComparisonEngine ce;

	for (int i=0; i < lrvec.size(); i++) {
		while(rp->GetFirst(&rpr)) {
			if (!ce.Compare(lrvec[i], &lom, &rpr, &rom)) {
				++mc;
				MergeRecord(lrvec[i], &rpr);
			}
		}
	
	}
}

//method to merge records
void Join::MergeRecord(Record *lr, Record *rr) {
	
	int nal=lr->getNumAtts(), nar=rr->getNumAtts();
	int *atts = new int[nal+nar];
	for (int k=0;k<nal;k++) atts[k]=k;
	for (int k=0;k<nar;k++) atts[k+nal]=k;
	Record *m = new 
	Record();
	m->MergeRecords(lr, rr, nal, nar, atts, nal+nar, nal);
	outPipe->Insert(m);
	delete atts;
	return;
}


/*==============================================================================================*/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
	// initialize input Pipe Object variable
	this->inPipe = &inPipe; 
	// initialize output Pipe Object variable
	this->outPipe = &outPipe;
	// initialize Schema Class object variable
	this->mySchema = &mySchema;
	// this function creates the executing thread
	pthread_create(&this->pthreadObj,NULL, caller, (void*)this);
}

// auxiliary functions
// this function holds the calling entity till the executing relational operator has run to completion
void DuplicateRemoval::WaitUntilDone () { pthread_join (pthreadObj, NULL); }
// This function provides information regarding internal memory usage
void DuplicateRemoval::Use_n_Pages (int n) { this->rl = n; }
//this function is a static caller method
void* DuplicateRemoval::caller(void *args) { ((DuplicateRemoval*)args)->operation(); }

// function is called by the thread
void* DuplicateRemoval::operation() {

	// initialize all the vraibles
	OrderMaker om(mySchema); ComparisonEngine ce;
	Record pr, cr;Pipe *sp = new Pipe(100);
	BigQ sq(*inPipe, *sp, om, rl);
	
	// This method remove the duplicates 
	sp->Remove(&pr);
	while(sp->Remove(&cr)) {
		if(!ce.Compare(&pr, &cr, &om)) continue;
		outPipe->Insert(&pr);pr.Consume(&cr);
	}

	// perform insert operation in output pipe and after insertion shut it down
	outPipe->Insert(&pr);outPipe->ShutDown();
}

/*==============================================================================================*/
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

	// initialize input Pipe Object variable
	this->inPipe = &inPipe; 
	// initialize output Pipe Object variable
	this->outPipe = &outPipe; 
	//Instantiate Function class  and initialize variable
	this->computeMe = &computeMe;

	// this function creates the executing thread
	pthread_create(&this->pthreadObj, NULL, caller, (void *)this);
}

// auxiliary functions
// this function holds the calling entity till the executing relational operator has run to completion
void Sum::WaitUntilDone () { pthread_join (pthreadObj, NULL); }
// This function provides information regarding internal memory usage
void Sum::Use_n_Pages (int n) { return; }
//this function is a static caller method
void* Sum::caller(void *args) { ((Sum*)args)->operation(); }
// function is called by the thread
void* Sum::operation() {
	Record t; Record rec; Type rt;
	int si = 0; double sd = 0.0f;

	while(inPipe->Remove(&rec)) {
		int ti = 0; double td = 0.0f;
		rt = this->computeMe->Apply(rec, ti, td);
		rt == Int ? si += ti : sd += td;
	}

	char result[30];
	if (rt == Int) sprintf(result, "%d|", si); else sprintf(result, "%f|", sd);
	Type myType = (rt==Int)?Int:Double;
	Attribute sum = {(char *)"sum",myType};
	Schema os("something",1,&sum);
	t.ComposeRecord(&os,result);
	outPipe->Insert(&t);outPipe->ShutDown();
}

/*==============================================================================================*/
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 
	
	// initialize input Pipe Object variable
	this->inPipe = &inPipe;	
	// initialize output Pipe Object variable
	this->outPipe = &outPipe;
	//Initialize OrderMaker class variable with group attributes
	this->groupAtts = &groupAtts; 
	//Instantiate Function class  and initialize variable
	this->computeMe = &computeMe;

	// this function creates the executing thread
	pthread_create(&this->pthreadObj, NULL, caller, (void *)this);
}

// auxiliary functions
// this function holds the calling entity till the executing relational operator has run to completion
void GroupBy::WaitUntilDone () { pthread_join (pthreadObj, NULL); }
// This function provides information regarding internal memory usage
void GroupBy::Use_n_Pages (int n) { rl=n; }
//this function is a static caller method
void* GroupBy::caller(void *args) { ((GroupBy*)args)->operation(); }
// function is called by the thread
void* GroupBy::operation() {

	Pipe sp(1000);BigQ bq(*inPipe,sp,*groupAtts,rl);
	Record *gr = new Record(),*pr=new Record(),*cr=NULL;
	int ir,si;double dr,sd;Type rt; ComparisonEngine ce;
	bool grchng=false,pe = false;

	// defensive check
	if(!sp.Remove(pr)) { outPipe->ShutDown(); return 0; }
 
	while(!pe) {
		si =0;sd=0;ir=0;dr=0;
		cr = new Record();
		grchng = false;

		while(!pe && !grchng) {
			sp.Remove(cr);
			// defensive check in case record is empty
			if(cr->bits!=NULL) { 
			if(ce.Compare(cr,pr,groupAtts)!=0){gr->Copy(pr);grchng=true;}}
			else { pe = true; }
			rt = computeMe->Apply(*pr,ir,dr);
			if(rt==Int) { si += ir; } else if(rt==Double) { sd += dr; }
			pr->Consume(cr);
		}

		Record *op = new Record();
		
		if(rt==Double) {
			Attribute a = {(char*)"sum", Double};Schema ss((char*)"somefile",1,&a);
			char sstr[30];sprintf(sstr, "%f|", sd); op->ComposeRecord(&ss,sstr);
		}

		if (rt==Int) {
			Attribute att = {(char*)"sum", Int};Schema ss((char*)"somefile",1,&att);
			char sstr[30];sprintf(sstr, "%d|", si); op->ComposeRecord(&ss,sstr);
		}

		Record rr;
		int nsatt = groupAtts->numAtts+1; int satt[nsatt]; satt[0]=0;
		for(int i=1;i<nsatt;i++) { satt[i]=groupAtts->whichAtts[i-1]; }
		rr.MergeRecords(op,gr,1,nsatt-1,satt,nsatt,1);
		outPipe->Insert(&rr);
	}
	outPipe->ShutDown();
}

/*==============================================================================================*/
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	// initialize input Pipe Object variable
	this->inPipe = &inPipe;
	// Initialize Scehma class variable
	this->mySchema = &mySchema;
	// output File Object variable initialization
	this->outFile = outFile;
	pthread_create(&this->pthreadObj, NULL, caller, (void *)this);
}

// auxiliary
// this function holds the calling entity till the executing relational operator has run to completion
void WriteOut::WaitUntilDone () { pthread_join (pthreadObj, NULL);}
// This function provides information regarding internal memory usage
void WriteOut::Use_n_Pages (int n) { return; }
//this function is a static caller method
void* WriteOut::caller(void *args) { ((WriteOut*)args)->operation(); }
// function similar to record.Print()
void* WriteOut::operation() {
    Record t;
    long rc=0;
    // loop through all of the attributes
    int n = mySchema->GetNumAtts();
    Attribute *atts = mySchema->GetAtts();
    while (inPipe->Remove(&t) )
    {
        rc++;
        // loop through all of the attributes
        for (int i = 0; i < n; i++) {

            fprintf(outFile,"%s: ",atts[i].name);
            int pointer = ((int *) t.bits)[i + 1];
            
            if (atts[i].myType == Int)
            {
                int *mi = (int *) &(t.bits[pointer]);
                fprintf(outFile,"%d",*mi);

            }
            else if (atts[i].myType == Double)
            {
                double *md = (double *) &(t.bits[pointer]);
                fprintf(outFile,"%f",*md);
            }
            else if (atts[i].myType == String)
            {
                char *ms = (char *) &(t.bits[pointer]);
                fprintf(outFile,"%s",ms);
            }

            if (i != n - 1) {
                fprintf(outFile,"%c",'|');
            }
        }
        fprintf(outFile,"%c",'\n');
    }
    fclose(outFile);
    cout<<"\n Number of records written to output file : "<<rc<<"\n";
}
/*==============================================================================================*/

