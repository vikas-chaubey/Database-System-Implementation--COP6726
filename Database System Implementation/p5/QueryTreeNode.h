
#ifndef QUERYTREENODE_h
#define QUERYTREENODE_h

#include <unordered_map>
const int BUFFSIZE = 100;


unordered_map<int, Pipe *> pipeMap;

enum RelOpType {
    G, SF, SP, P, D, S, GB, J, W
};

class RelOpNode {
public:
    int pid;  // Pipe ID
    RelOpType t;
    Schema schema;  // Ouput Schema
    RelationalOp *relOp;
    RelOpNode ();
    RelOpNode (RelOpType type) : t (type) {}
    ~RelOpNode () {}
    virtual void PrintNode () {};
    virtual void ExecuteNode (unordered_map<int, Pipe *> &pipeMap){};
    virtual void Wait () {
        relOp->WaitUntilDone ();
    }
};

class JoinOpNode : public RelOpNode {
public:
    RelOpNode *left;
    RelOpNode *right;
    CNF cnf;
    Record literal;
    JoinOpNode () : RelOpNode (J) {}
    ~JoinOpNode () {
        if (left) delete left;
        if (right) delete right;
    }
    void PrintNode () {
        left->PrintNode ();
        right->PrintNode ();
        cout << "-----------------------------------" << endl;
        cout << " JOIN OPERATION" << endl;
        cout << " Left Input Pipe ID : " << left->pid << endl;
        cout << " Right Input Pipe ID : " << right->pid << endl;
        cout << " Output Pipe ID : " << pid << endl;
        cout << " Output Schema : " << endl;
        schema.Print ();
        cout << " Join CNF : " << endl;
        cnf.PrintWithSchema(&left->schema,&right->schema,&literal);
        cout << "-----------------------------------" << endl;
    }
    
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
        pipeMap[pid] = new Pipe (BUFFSIZE);
        relOp = new Join ();
        left->ExecuteNode (pipeMap);
        right->ExecuteNode (pipeMap);
        ((Join *)relOp)->Run (*(pipeMap[left->pid]), *(pipeMap[right->pid]), *(pipeMap[pid]), cnf, literal);
        left->Wait ();
        right->Wait ();
    }
};

class ProjectOpNode : public RelOpNode {
public:
    int numIn;
    int numOut;
    int *attsToKeep;
    RelOpNode *from;
    ProjectOpNode () : RelOpNode (P) {}
    ~ProjectOpNode () {
    if (attsToKeep) delete[] attsToKeep;
    }
    void PrintNode () {
     from->PrintNode ();
           
           cout << "-----------------------------------" << endl;
           cout << " PROJECT OPERATION" << endl;
           cout << " Input Pipe ID : " << from->pid << endl;
           cout << " Output Pipe ID " << pid << endl;
           cout << " Number Attrs Input : " << numIn << endl;
           cout << " Number Attrs Output : " << numOut << endl;
           cout << " Attrs To Keep : [";
           for (int i = 0; i < numOut; i++) {
               cout << attsToKeep[i];
               if (i!=numOut-1){
                   cout<<",";
               }
           }
           cout<< "]"<< endl;
           cout << " Output Schema:" << endl;
           schema.Print ();
           cout << "-----------------------------------" << endl;

    }
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
        pipeMap[pid] = new Pipe (BUFFSIZE);
        relOp = new Project ();
        from->ExecuteNode (pipeMap);
        ((Project *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), attsToKeep, numIn, numOut);
        from->Wait ();
    }
};

class SelectFileOpNode : public RelOpNode {
public:
    bool opened;
    CNF cnf;
    DBFile file;
    Record literal;
    SelectFileOpNode () : RelOpNode (SF) {}
    ~SelectFileOpNode () {
        if (opened) {
            file.Close ();
        }
    }
    void PrintNode () {
           cout << "-----------------------------------" << endl;
           cout << " SELECT FILE OPERATION" << endl;
           cout << " Output Pipe ID " << pid << endl;
           cout << " Output Schema:" << endl;
           schema.Print ();
           cout << " Select CNF:" << endl;
           cnf.PrintWithSchema(&schema,&schema,&literal);
           cout << "-----------------------------------" << endl;

    }
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
        pipeMap[pid] = new Pipe (BUFFSIZE);
        relOp = new SelectFile ();
        ((SelectFile *)relOp)->Run (file, *(pipeMap[pid]), cnf, literal);
    }
};


class SelectPipeOpNode : public RelOpNode {
public:
    CNF cnf;
    Record literal;
    RelOpNode *from;
    SelectPipeOpNode () : RelOpNode (SP) {}
    ~SelectPipeOpNode () {
        if (from) delete from;
    }
    void PrintNode () {
        from->PrintNode ();
        cout << "-----------------------------------" << endl;
        cout << " SELECT PIPE OPERATION" << endl;
        cout << " Input Pipe ID : " << from->pid << endl;
        cout << " Output Pipe ID : " << pid << endl;
        cout << " Output Schema:" << endl;
        schema.Print ();
        cout << " Select CNF:" << endl;
        cnf.PrintWithSchema(&schema,&schema,&literal);
        cout << "-----------------------------------" << endl;
    }
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
        pipeMap[pid] = new Pipe (BUFFSIZE);
        relOp = new SelectPipe ();
        from->ExecuteNode (pipeMap);
        ((SelectPipe *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), cnf, literal);
        from->Wait ();
    }
};

class SumOpNode : public RelOpNode {
public:
    Function compute;
    RelOpNode *from;
    SumOpNode () : RelOpNode (S) {}
    ~SumOpNode () {
    if (from) delete from;
    }
    void PrintNode () {
        
        from->PrintNode ();
        cout << "-----------------------------------" << endl;
        cout << " SUM OPERATION" << endl;
        cout << " Input Pipe ID : " << from->pid << endl;
        cout << " Output Pipe ID : " << pid << endl;
        cout << " Function :" << endl;
        compute.Print (&from->schema);
        cout << " Output Schema:" << endl;
        schema.Print ();
        cout << "-----------------------------------" << endl;

    }
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
    pipeMap[pid] = new Pipe (BUFFSIZE);
    relOp = new Sum ();
    from->ExecuteNode (pipeMap);
    ((Sum *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), compute);
    from->Wait ();
    }
};

class DistinctOpNode : public RelOpNode {
public:
    RelOpNode *from;
    DistinctOpNode () : RelOpNode (D) {}
    ~DistinctOpNode () {
        if (from) delete from;
    }
    void PrintNode () {
        from->PrintNode ();
        cout << "-----------------------------------" << endl;
        cout << " DISTINCT OPERATION" << endl;
        cout << " Input Pipe ID : " << from->pid << endl;
        cout << " Output Pipe ID : " << pid << endl;
        cout << " Output Schema:" << endl;
        schema.Print ();
        cout << "-----------------------------------" << endl;
    }
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
        pipeMap[pid] = new Pipe (BUFFSIZE);
        relOp = new DuplicateRemoval ();
        from->ExecuteNode (pipeMap);
        ((DuplicateRemoval *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), schema);
        from->Wait ();
    }
};

class GroupByOpNode : public RelOpNode {
public:
    RelOpNode *from;
    Function compute;
    OrderMaker group;
    bool distinctFunc;
    GroupByOpNode () : RelOpNode (GB) {
        distinctFunc=false;
    }
    ~GroupByOpNode () {
        if (from) delete from;
    }
    void PrintNode () {
        from->PrintNode ();
        cout << "-----------------------------------" << endl;
        cout << " GROUP OPERATION" << endl;
        cout << " Input Pipe ID : " << from->pid << endl;
        cout << " Output Pipe ID : " << pid << endl;
        cout << " Output Schema : " << endl;
        schema.Print ();
        cout << " Function : " << endl;
        compute.Print (&from->schema);
        if (distinctFunc){
            cout << " Distinct Function :  True" <<  endl;
        }
        cout << " Grouping On : " << endl;
        group.PrintWithSchema(&from->schema);
        cout << "-----------------------------------" << endl;
    }
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
        pipeMap[pid] = new Pipe (BUFFSIZE);
        relOp = new GroupBy ();
        from->ExecuteNode(pipeMap);
        ((GroupBy *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), group, compute,distinctFunc);
        from->Wait ();
    }
};

class WriteOutOpNode : public RelOpNode {
public:
    RelOpNode *from;
    FILE *output;
    WriteOutOpNode () : RelOpNode (W) {}
    ~WriteOutOpNode () {
        if (output) delete output;
        if (from) delete from;
    }
    void PrintNode () {
        from->PrintNode ();
        cout << "-----------------------------------" << endl;
        cout << " WRITE OPERATION" << endl;
        cout << " Input Pipe ID : " << from->pid << endl;
        cout << "-----------------------------------" << endl;
    }
    void ExecuteNode (unordered_map<int, Pipe *> &pipeMap) {
        relOp = new WriteOut ();
        from->ExecuteNode (pipeMap);
        ((WriteOut *)relOp)->Run (*(pipeMap[from->pid]), output, schema);
        from->Wait ();
    }
};


#endif
