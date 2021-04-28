#ifndef QUERYTREENODE_H
#define QUERYTREENODE_H
enum RelOpType {
    G, SF, SP, P, D, S, GB, J, W
};

class RelOpNode {

public:

    int pid;
    RelOpType t;
    Schema schema;
    RelOpNode ();
    RelOpNode (RelOpType type) : t (type) {}
    ~RelOpNode () {}
    virtual void PrintNode () {};

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

};

class GroupByOpNode : public RelOpNode {

public:

    RelOpNode *from;

    Function compute;
    OrderMaker group;
    bool distinctFuncFlag;
    GroupByOpNode () : RelOpNode (GB) {}
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
        if (distinctFuncFlag){
            cout << " Distinct Function :  True" <<  endl;
        }
        cout << " Grouping On : " << endl;
        group.PrintWithSchema(&from->schema);
        cout << "-----------------------------------" << endl;

    }

};

class WriteOutOpNode : public RelOpNode {

public:

    RelOpNode *from;

    FILE *output;

    WriteOutOpNode () : RelOpNode (W) {}
    ~WriteOutOpNode () {

        if (from) delete from;

    }

    void PrintNode () {

        from->PrintNode ();
        cout << "-----------------------------------" << endl;
        cout << " WRITE OPERATION" << endl;
        cout << " Input Pipe ID : " << from->pid << endl;
        cout << "-----------------------------------" << endl;

    }

};
#endif
