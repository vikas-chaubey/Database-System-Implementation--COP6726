#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"
#include "QueryTreeNode.h"

extern "C" {
    int yyparse (void);   // defined in y.tab.c
}

using namespace std;

// the aggregate function (NULL if no agg)
extern struct FuncOperator *finalFunction;  
// the list of tables and aliases in the query
extern struct TableList *tables;      
// the predicate in the WHERE clause      
extern struct AndList *boolean;  
// grouping atts (NULL if no grouping)           
extern struct NameList *groupingAtts;  
// the set of attributes in the SELECT (NULL if no such atts)    
extern struct NameList *attsToSelect;      
// 1 if there is a DISTINCT in a non-aggregate query
extern int distinctAtts;       
// 1 if there is a DISTINCT in an aggregate query            
extern int distinctFunc;                    

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

const int nrows_customer = 150000;
const int nrows_lineitem = 6001215;
const int nrows_nation = 25;
const int nrows_orders = 1500000;
const int nrows_part = 200000;
const int nrows_partsupp = 800000;
const int nrows_region = 5;
const int nrows_supplier = 10000;

static int pidBuffer = 0;
int GetUniquePipeID () {
    return ++pidBuffer;
}

typedef map<string, Schema> SchMap;
typedef map<string, string> TableAliasMap;
typedef map<string, AndList> booleanMap;
typedef map<string,bool> Loopkup;

class QueryPlanner{
public:
    vector<char *> tableNames;
    vector<char *> TableOrderForJoin;
    vector<char *> memoryBuffer;
    TableAliasMap aliasMap;
    SchMap map;
    Statistics s;
    RelOpNode *root;
    /*The constructor function helps boot up the class object and populates the necessary variables necessary for
    query parsing and optimization plan. The constructor first calls the yyparse () function which populates the extern
    data structure variables used throughout the program. Post the parsing, it populates the string to schema map
    and attribute statistics for the tables present in the catalog.
    void*/
    QueryPlanner();

    /*This function is called when the constructor is invoked by creation of a new QueryPlanner Object. This function
    populates the map SchemaMap variable of the class. Each of the string names of schemas are mapped to their
    corresponding Schema Objects.*/
    void PopulateSchemaMap ();

    /*The Statistics ‘s’ object is populated by this method. As the previous method, this method is invoked by the
    QueryPlanner constructor. The AddRel method is invoked to populate the statistics for each of the attributes
    pertaining to all the relations of the catalog.*/
    void PopulateStatistics ();

    /*Alias map holds the map from the actual table to the alias name being used in the query. After the query is
    parsed, all the aliases are stored in this table for uniformity in table names. Also the statistics of the original name
    is copied to the alias name.*/
    void PopulateAliasMapAndCopyStatistics ();

    /*This is a utility function utilized for converting a NameList data structure to a vector of strings. The vector of strings
is passed by reference and each name is pushed until the nameList is completely read.*/
    void CopyNameList(NameList *nameList, vector<string> &names) ;

    /*This function marks the starting point for the entire logic. This function calls the Optimise and BuildExecutionTree
    which comprises of the main logic of the entire assignment.*/
    void Compile();

    /*Optimise function uses the estimate function from assignment 4.1 to permute the best ordering of joins given
    more than 1 join. Join conditions are first extracted from AndList of WHERE condition. Permutations of these joins
    are then run over a for loop for computing the minimum join cost, the join ordering of which is stored in a variable
    to be used further during node creation.*/
    void Optimise();

    /*This function constructs the query plan tree from the information processed until now. Query nodes correspond to
    7 relational operator implementations from assignment 3. Starting from the root node, each of different types of
    nodes like SelectFile, SelectPipe etc. are constructed and then appended to the corresponding parent pointer.
    Unique Pipe IDs are assigned, and updated schema is passed down to the child node by the parent node.*/
    void BuildExecutionTree();

    /*Print function prints the tree branch with the minimum cost. Structures like CNF, Schema are printed along other
    necessary information.*/
    void Print();

    /*Given a AndList this function identifies the condition which are responsible for the joins and create a map which
    contains all the possible combination of the joins.*/
    booleanMap GetMapFromBoolean(AndList *boolean);
};

QueryPlanner::QueryPlanner():memoryBuffer(2){
    PopulateSchemaMap ();
    PopulateStatistics ();
    yyparse ();
};


/*This function is called when the constructor is invoked by creation of a new QueryPlanner Object. This function
populates the map SchemaMap variable of the class. Each of the string names of schemas are mapped to their
corresponding Schema Objects.*/
void QueryPlanner ::PopulateSchemaMap () {

    map[string(region)] = Schema ("catalog", region);
    map[string(part)] = Schema ("catalog", part);
    map[string(partsupp)] = Schema ("catalog", partsupp);
    map[string(nation)] = Schema ("catalog", nation);
    map[string(customer)] = Schema ("catalog", customer);
    map[string(supplier)] = Schema ("catalog", supplier);
    map[string(lineitem)] = Schema ("catalog", lineitem);
    map[string(orders)] = Schema ("catalog", orders);

}

/*The Statistics ‘s’ object is populated by this method. As the previous method, this method is invoked by the
QueryPlanner constructor. The AddRel method is invoked to populate the statistics for each of the attributes
pertaining to all the relations of the catalog.*/
void QueryPlanner::PopulateStatistics () {

    s.AddRel (region, nrows_region);
    s.AddRel (nation, nrows_nation);
    s.AddRel (part, nrows_part);
    s.AddRel (supplier, nrows_supplier);
    s.AddRel (partsupp, nrows_partsupp);
    s.AddRel (customer, nrows_customer);
    s.AddRel (orders, nrows_orders);
    s.AddRel (lineitem, nrows_lineitem);

    // region
    s.AddAtt (region, "r_regionkey", nrows_region);
    s.AddAtt (region, "r_name", nrows_region);
    s.AddAtt (region, "r_comment", nrows_region);

    // nation
    s.AddAtt (nation, "n_nationkey",  nrows_nation);
    s.AddAtt (nation, "n_name", nrows_nation);
    s.AddAtt (nation, "n_regionkey", nrows_region);
    s.AddAtt (nation, "n_comment", nrows_nation);

    // part
    s.AddAtt (part, "p_partkey", nrows_part);
    s.AddAtt (part, "p_name", nrows_part);
    s.AddAtt (part, "p_mfgr", nrows_part);
    s.AddAtt (part, "p_brand", nrows_part);
    s.AddAtt (part, "p_type", nrows_part);
    s.AddAtt (part, "p_size", nrows_part);
    s.AddAtt (part, "p_container", nrows_part);
    s.AddAtt (part, "p_retailprice", nrows_part);
    s.AddAtt (part, "p_comment", nrows_part);

    // supplier
    s.AddAtt (supplier, "s_suppkey", nrows_supplier);
    s.AddAtt (supplier, "s_name", nrows_supplier);
    s.AddAtt (supplier, "s_address", nrows_supplier);
    s.AddAtt (supplier, "s_nationkey", nrows_nation);
    s.AddAtt (supplier, "s_phone", nrows_supplier);
    s.AddAtt (supplier, "s_acctbal", nrows_supplier);
    s.AddAtt (supplier, "s_comment", nrows_supplier);

    // partsupp
    s.AddAtt (partsupp, "ps_partkey", nrows_part);
    s.AddAtt (partsupp, "ps_suppkey", nrows_supplier);
    s.AddAtt (partsupp, "ps_availqty", nrows_partsupp);
    s.AddAtt (partsupp, "ps_supplycost", nrows_partsupp);
    s.AddAtt (partsupp, "ps_comment", nrows_partsupp);

    // customer
    s.AddAtt (customer, "c_custkey", nrows_customer);
    s.AddAtt (customer, "c_name", nrows_customer);
    s.AddAtt (customer, "c_address", nrows_customer);
    s.AddAtt (customer, "c_nationkey", nrows_nation);
    s.AddAtt (customer, "c_phone", nrows_customer);
    s.AddAtt (customer, "c_acctbal", nrows_customer);
    s.AddAtt (customer, "c_mktsegment", 5);
    s.AddAtt (customer, "c_comment", nrows_customer);

    // orders
    s.AddAtt (orders, "o_orderkey", nrows_orders);
    s.AddAtt (orders, "o_custkey", nrows_customer);
    s.AddAtt (orders, "o_orderstatus", 3);
    s.AddAtt (orders, "o_totalprice", nrows_orders);
    s.AddAtt (orders, "o_orderdate", nrows_orders);
    s.AddAtt (orders, "o_orderpriority", 5);
    s.AddAtt (orders, "o_clerk", nrows_orders);
    s.AddAtt (orders, "o_shippriority", 1);
    s.AddAtt (orders, "o_comment", nrows_orders);

    // lineitem
    s.AddAtt (lineitem, "l_orderkey", nrows_orders);
    s.AddAtt (lineitem, "l_partkey", nrows_part);
    s.AddAtt (lineitem, "l_suppkey", nrows_supplier);
    s.AddAtt (lineitem, "l_linenumber", nrows_lineitem);
    s.AddAtt (lineitem, "l_quantity", nrows_lineitem);
    s.AddAtt (lineitem, "l_extendedprice", nrows_lineitem);
    s.AddAtt (lineitem, "l_discount", nrows_lineitem);
    s.AddAtt (lineitem, "l_tax", nrows_lineitem);
    s.AddAtt (lineitem, "l_returnflag", 3);
    s.AddAtt (lineitem, "l_linestatus", 2);
    s.AddAtt (lineitem, "l_shipdate", nrows_lineitem);
    s.AddAtt (lineitem, "l_commitdate", nrows_lineitem);
    s.AddAtt (lineitem, "l_receiptdate", nrows_lineitem);
    s.AddAtt (lineitem, "l_shipinstruct", nrows_lineitem);
    s.AddAtt (lineitem, "l_shipmode", 7);
    s.AddAtt (lineitem, "l_comment", nrows_lineitem);

}

/*Alias map holds the map from the actual table to the alias name being used in the query. After the query is
parsed, all the aliases are stored in this table for uniformity in table names. Also the statistics of the original name
is copied to the alias name.*/
void QueryPlanner::PopulateAliasMapAndCopyStatistics(){
    while (tables) {
        s.CopyRel (tables->tableName, tables->aliasAs);
        aliasMap[tables->aliasAs] = tables->tableName;
        tableNames.push_back (tables->aliasAs);
        tables = tables->next;
    }
}


/*This is a utility function utilized for converting a NameList data structure to a vector of strings. The vector of strings
is passed by reference and each name is pushed until the nameList is completely read.*/
void QueryPlanner ::CopyNameList(NameList *nameList, vector<string> &names) {
    while (nameList) {
        names.push_back (string (nameList->name));
        nameList = nameList->next;
    }
}


/*This function marks the starting point for the entire logic. This function calls the Optimise and BuildExecutionTree
which comprises of the main logic of the entire assignment.*/
void QueryPlanner::Compile(){
    PopulateAliasMapAndCopyStatistics();
    Optimise();
    BuildExecutionTree();
}


/*Optimise function uses the estimate function from assignment 4.1 to permute the best ordering of joins given
more than 1 join. Join conditions are first extracted from AndList of WHERE condition. Permutations of these joins
are then run over a for loop for computing the minimum join cost, the join ordering of which is stored in a variable
to be used further during node creation.*/
void QueryPlanner::Optimise(){
    
    sort (tableNames.begin (), tableNames.end ());

    int min_join_cost = INT_MAX;
    int curr_join_cost = 0;
    booleanMap b = GetMapFromBoolean(boolean);

    do {
        Statistics temp (s);
        auto iter = tableNames.begin ();
        int biter = 0;
        memoryBuffer[biter] = *iter;
        iter++;
        biter++;

        while (iter != tableNames.end ()) {
            
            memoryBuffer[biter] =  *iter ;
            string key = "";
            for ( int c = 0; c<=biter;c++){
                key += string(memoryBuffer[c]);
            }
            
            if (b.find(key) == b.end()) {
                break;
            }
            
            curr_join_cost += temp.Estimate (&b[key], &memoryBuffer[0], 2);
            temp.Apply (&b[key], &memoryBuffer[0], 2);

            if (curr_join_cost <= 0 || curr_join_cost > min_join_cost) {
                break;
            }

            iter++;
            biter++;
        }

        if (curr_join_cost > 0 && curr_join_cost < min_join_cost) {
            min_join_cost = curr_join_cost;
            TableOrderForJoin = tableNames;
        }
        curr_join_cost = 0;

    } while (next_permutation (tableNames.begin (), tableNames.end ()));

    if (TableOrderForJoin.size()==0){
        TableOrderForJoin = tableNames;
    }
}


/*This function constructs the query plan tree from the information processed until now. Query nodes correspond to
7 relational operator implementations from assignment 3. Starting from the root node, each of different types of
nodes like SelectFile, SelectPipe etc. are constructed and then appended to the corresponding parent pointer.
Unique Pipe IDs are assigned, and updated schema is passed down to the child node by the parent node.*/
void QueryPlanner :: BuildExecutionTree(){

        auto iter = TableOrderForJoin.begin ();
        SelectFileOpNode *selectFileNode = new SelectFileOpNode ();

        char filepath[50];
        selectFileNode->opened = true;
        selectFileNode->pid = GetUniquePipeID ();
        selectFileNode->schema = Schema (map[aliasMap[*iter]]);
        selectFileNode->schema.ResetSchema (*iter);
        selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

        iter++;
        if (iter == TableOrderForJoin.end ()) {
            root = selectFileNode;
        } 
        else {

            JoinOpNode *joinNode = new JoinOpNode ();
            joinNode->pid = GetUniquePipeID ();
            joinNode->left = selectFileNode;

            selectFileNode = new SelectFileOpNode ();
            selectFileNode->opened = true;
            selectFileNode->pid = GetUniquePipeID ();
            selectFileNode->schema = Schema (map[aliasMap[*iter]]);
            selectFileNode->schema.ResetSchema (*iter);
            selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

            joinNode->right = selectFileNode;
            joinNode->schema.GetSchemaForJoin (joinNode->left->schema, joinNode->right->schema);
            joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->left->schema), &(joinNode->right->schema), joinNode->literal);

            iter++;

            while (iter != TableOrderForJoin.end ()) {

                JoinOpNode *p = joinNode;
                selectFileNode = new SelectFileOpNode ();
                selectFileNode->opened = true;
                selectFileNode->pid = GetUniquePipeID ();
                selectFileNode->schema = Schema (map[aliasMap[*iter]]);
                selectFileNode->schema.ResetSchema (*iter);
                selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

                joinNode = new JoinOpNode ();
                joinNode->pid = GetUniquePipeID ();
                joinNode->left = p;
                joinNode->right = selectFileNode;
                joinNode->schema.GetSchemaForJoin (joinNode->left->schema, joinNode->right->schema);
                joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->left->schema), &(joinNode->right->schema), joinNode->literal);

                iter++;
            }
            root = joinNode;
        }

        RelOpNode *temp = root;

        if (groupingAtts) 
        {
            root = new GroupByOpNode ();
            vector<string> groupAtts;
            CopyNameList (groupingAtts, groupAtts);
            root->pid = GetUniquePipeID ();
            ((GroupByOpNode *) root)->compute.GrowFromParseTree (finalFunction, temp->schema);
            vector<string> atts;
            CopyNameList (groupingAtts, atts);
            root->schema.GetSchemaForGroup (temp->schema, ((GroupByOpNode *) root)->compute.ReturnInt (),atts);
            ((GroupByOpNode *) root)->group.growFromParseTree (groupingAtts, &(temp->schema));
            if (distinctFunc) {
                ((GroupByOpNode *) root)->distinctFuncFlag = true;
            }
            ((GroupByOpNode *) root)->from = temp;

        } 
        else if (finalFunction) 
        {
            root = new SumOpNode ();
            root->pid = GetUniquePipeID ();
            ((SumOpNode *) root)->compute.GrowFromParseTree (finalFunction, temp->schema);
            Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
            root->schema = Schema (NULL, 1, ((SumOpNode *) root)->compute.ReturnInt () ? atts[0] : atts[1]);
            ((SumOpNode *) root)->from = temp;
        }
        else if (attsToSelect)
        {
            root = new ProjectOpNode ();
            vector<int> attsToKeep;
            vector<string> atts;
            CopyNameList (attsToSelect, atts);
            root->pid = GetUniquePipeID ();
            root->schema.GetSchemaForProject (temp->schema, atts, attsToKeep);
            ((ProjectOpNode *) root)->attsToKeep = &attsToKeep[0];
            ((ProjectOpNode *) root)->numIn = temp->schema.GetNumAtts ();
            ((ProjectOpNode *) root)->numOut = atts.size ();
            ((ProjectOpNode *) root)->from = temp;
            temp = root;

            if (distinctAtts) {
                root = new DistinctOpNode ();
                root->pid = GetUniquePipeID ();
                root->schema = temp->schema;
                ((DistinctOpNode *) root)->from = temp;
                temp = root;
            }
        }
}


/*Given a AndList this function identifies the condition which are responsible for the joins and create a map which
contains all the possible combination of the joins.*/
booleanMap QueryPlanner::GetMapFromBoolean(AndList *parseTree) {
    booleanMap b;
    string delimiter = ".";
    vector <string> fullkey;
    AndList * head = NULL;
    AndList * root = NULL;

    // now we go through and build the comparison structure
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
        
        // see if we have run off of the end of all of the ANDs
        if (parseTree == NULL) {
            // done
            break;
        }

        // we have not, so copy over all of the ORs hanging off of this AND
        struct OrList *myOr = parseTree->left;
        for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

            // see if we have run off of the end of the ORs
            if (myOr == NULL) {
                // done with parsing
                break;
            }

            // we have not run off the list, so add the current OR in!
            
            // these store the types of the two values that are found
            Type typeLeft;
            Type typeRight;

            // first thing is to deal with the left operand
            // so we check to see if it is an attribute name, and if so,
            // we look it up in the schema
            if (myOr->left->left->code == NAME) {
                if (myOr->left->right->code == NAME) 
                {   
                    string key1,key2;

                    // left table string
                    string lts = myOr->left->left->value;
                    string pushlts = lts.substr(0, lts.find(delimiter));

                    // right table string
                    string rts = myOr->left->right->value;
                    string pushrts = rts.substr(0, rts.find(delimiter));
                    
                    key1= pushlts+pushrts;
                    key2 = pushrts+pushlts;
                    fullkey.push_back(pushlts);
                    fullkey.push_back(pushrts);

                    AndList pushAndList;
                    pushAndList.left=parseTree->left;
                    pushAndList.rightAnd=NULL;
                    
                    if(head==NULL){
                        root = new AndList;
                        root->left=parseTree->left;
                        root->rightAnd=NULL;
                        head = root;
                    }
                    else{
                        root->rightAnd =  new AndList;
                        root = root->rightAnd ;
                        root->left=parseTree->left;
                        root->rightAnd=NULL;
                    }
                    
                    b[key1] = pushAndList;
                    b[key2] = pushAndList;

                } 
                else if (myOr->left->right->code == STRING  || 
                        myOr->left->right->code == INT      ||
                        myOr->left->right->code == DOUBLE) 
                {
                    continue;
                } 
                else 
                {
                    cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                    //return -1;
                }
            } 
			else if (myOr->left->left->code == STRING   || 
                    myOr->left->left->code == INT       || 
                    myOr->left->left->code == DOUBLE)   
            {
                continue;
            }
            // catch-all case
            else 
            {
                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                //return -1;
            }

            // now we check to make sure that there was not a type mismatch
            if (typeLeft != typeRight) {
                cerr<< "ERROR! Type mismatch in Boolean  " 
                << myOr->left->left->value << " and "
                << myOr->left->right->value << " were found to not match.\n";
            }        
        }
    }
        
    if (fullkey.size()>0){
        Loopkup h;
        vector <string> keyf;
        for (int k = 0; k<fullkey.size();k++){
            if (h.find(fullkey[k]) == h.end()){
                keyf.push_back(fullkey[k]);
                h[fullkey[k]]=true;
            }
        }

        sort(keyf.begin(), keyf.end());
        do
        {
            string str="";
            for (int k = 0; k<keyf.size();k++){
                str+=keyf[k];
            }
            b[str] = *head;
        }while(next_permutation(keyf.begin(),keyf.end()));
    }
    return b;
}


/*Print function prints the tree branch with the minimum cost. Structures like CNF, Schema are printed along other
necessary information.*/
void QueryPlanner:: Print(){
    root->PrintNode();
}
#endif
