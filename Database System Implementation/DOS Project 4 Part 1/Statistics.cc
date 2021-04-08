#include "Statistics.h"

/*This is Statistics class no arg constructor*/
Statistics::Statistics()
{
}

/*This is Statistics class destructor*/
Statistics::~Statistics()
{
    //map iterator object
    map<string,RelStats*>::iterator iteratorObj;
    RelStats * relStatsObj = NULL;
    for(iteratorObj=statsMap.begin(); iteratorObj!=statsMap.end(); iteratorObj++)
    {
        relStatsObj = iteratorObj->second;
        delete relStatsObj;
        relStatsObj = NULL;
    }
    statsMap.clear();
}

/*get stats map method to get map which contains Rel statistics*/
map<string,RelStats*>* Statistics::GetStatsMap()
{
    return &statsMap;
}

/*This is Statistics class arg constructor*/
Statistics::Statistics(Statistics &copyMe)
{
    map<string,RelStats*> * mapPointer = copyMe.GetStatsMap();
    map<string,RelStats*>::iterator iteratorObj;
    RelStats * relStatsObj;
    for(iteratorObj=mapPointer->begin(); iteratorObj!=mapPointer->end(); iteratorObj++)
    {
        relStatsObj = new RelStats(*iteratorObj->second);
        statsMap[iteratorObj->first] = relStatsObj;
    }
}


void Statistics::AddRel(char *relName, int numTuples)
{
    map<string,RelStats*>::iterator iteratorObj;
    iteratorObj = statsMap.find(string(relName));
    if (iteratorObj == statsMap.end()){
        RelStats * relStatsObj = new RelStats(numTuples,string(relName));
        statsMap[string(relName)]=relStatsObj;
    }
    else{
        statsMap[string(relName)]->UpdateNoOfTuples(numTuples);
        statsMap[string(relName)]->UpdateGroup(relName,1);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{

  map<string,RelStats*>::iterator iteratorObj;
  iteratorObj = statsMap.find(string(relName));
  if(iteratorObj!=statsMap.end())
  {
      statsMap[string(relName)]->UpdateAttributes(string(attName),numDistincts);
  }

}

void Statistics::CopyRel(char *oldName, char *newName)
{
  string oldRelName=string(oldName);
  string newRelName=string(newName);

  if(strcmp(oldName,newName)==0)  return;

  map<string,RelStats*>::iterator mapIterator;
  mapIterator = statsMap.find(newRelName);
  if(mapIterator!=statsMap.end())
  {
      delete mapIterator->second;
      string temp=mapIterator->first;
      mapIterator++;
      statsMap.erase(temp);

  }

  map<string,RelStats*>::iterator iteratorObj;

  iteratorObj = statsMap.find(oldRelName);
  RelStats * oldrelStatsObj;

  if(iteratorObj!=statsMap.end())
  {
      oldrelStatsObj = statsMap[oldRelName];
      RelStats* newrelStatsObj=new RelStats(oldrelStatsObj->GetNofTuples(),newRelName);

      map<string,int>::iterator attriteratorObj;
      for(attriteratorObj = oldrelStatsObj->GetRelationAttributes()->begin(); attriteratorObj!=oldrelStatsObj->GetRelationAttributes()->end();attriteratorObj++)
      {
          string s = newRelName + "." + attriteratorObj->first;
          newrelStatsObj->UpdateAttributes(s,attriteratorObj->second);
      }
      statsMap[string(newName)] = newrelStatsObj;
  }
  else
  {
      cerr<<"\n No Relation Exist with the name :"<<oldName<<endl;
      exit(1);
  }
}

void Statistics::Read(char *fromWhere)
{
    FILE *filePointer=NULL;
    filePointer = fopen(fromWhere,"r");
    char buffer[200];
    while(filePointer!=NULL && fscanf(filePointer,"%s",buffer)!=EOF)
    {
        if(strcmp(buffer,"BEGIN")==0)
        {
            long tupleCount=0;
            char relNameArray[200];
            int groupCountValue=0;
            char arrayGroupName[200];
            fscanf(filePointer,"%s %ld %s %d",relNameArray,&tupleCount,arrayGroupName,&groupCountValue);
            AddRel(relNameArray  ,tupleCount);
            statsMap[string(relNameArray)]->UpdateGroup(arrayGroupName,groupCountValue);
            char arrayAttName[200];
            int distinctValues=0;
            fscanf(filePointer,"%s",arrayAttName);
            while(strcmp(arrayAttName,"END")!=0)
            {
                fscanf(filePointer,"%d",&distinctValues);
                AddAtt(relNameArray,arrayAttName,distinctValues);
                fscanf(filePointer,"%s",arrayAttName);
            }
        }
    }
}

/*
    This function prints the details as per the relation, numOfTuples, numOfDistinctValues 
    and it also writes the statistics data structure values in the statistics.txt file
*/
void Statistics::Write(char *fromWhere)
{
    map<string,int> *numberMap;
    map<string,RelStats*>::iterator mapIterator;
    map<string,int>::iterator tIterator;
    FILE *filePointer;filePointer = fopen(fromWhere,"w");

    for(mapIterator = statsMap.begin();mapIterator!=statsMap.end();mapIterator++) {
        fprintf(filePointer,"BEGIN\n");

        long int tc = mapIterator->second->GetNofTuples();
        const char * rn = mapIterator->first.c_str();
        const char * gn = strdup(mapIterator->second->GetGroupName().c_str());
        int gz = mapIterator->second->GetGroupSize();
        fprintf(filePointer,"%s %ld %s %d\n",rn,tc,gn,gz);

        numberMap = mapIterator->second->GetRelationAttributes();
        for(tIterator = numberMap->begin();tIterator!=numberMap->end();tIterator++) {
            const char *f = strdup(tIterator->first.c_str());
            int s = tIterator->second;
            fprintf(filePointer,"%s %d\n",tIterator->first.c_str(),tIterator->second);
        }
        fprintf(filePointer,"END\n");      
    }
    fclose(filePointer);  
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  double estimatedDoubleValue = Estimate(parseTree,relNames,numToJoin);
  long tupleCount =(long)((estimatedDoubleValue-floor(estimatedDoubleValue))>=0.5?ceil(estimatedDoubleValue):floor(estimatedDoubleValue));
  string nameOfGroup="";
  int sizeOfgroup = numToJoin;
  for(int i=0;i<sizeOfgroup;i++)
  {
      nameOfGroup = nameOfGroup + "," + relNames[i];
  }
  for(int i=0;i<numToJoin;i++)
  {
      statsMap[relNames[i]]->UpdateGroup(nameOfGroup,sizeOfgroup);
      statsMap[relNames[i]]->UpdateNoOfTuples(tupleCount);
  }
}

double Statistics::Estimate(struct AndList *tree, char **relationNames, int numToJoin)
{
    double estimatedDoubleValue=1;
    map<string,long> longValueMap;
    if(!checkParseTreeAndPartition(tree,relationNames,numToJoin,longValueMap))
    {
        cout<<"\n invalid parameters passed";
        return -1.0;
    }
    else
    {
        string nameofGroup="";
        map<string,long> tValueMap;
        map<string,long>::iterator tIterator;
      
        int sizeOfGroup = numToJoin;
        for(int i=0;i<sizeOfGroup;i++)
        {
            nameofGroup = nameofGroup + "," + relationNames[i];
        }
        for(int i=0;i<numToJoin;i++)
        {
            tValueMap[statsMap[relationNames[i]]->GetGroupName()]=statsMap[relationNames[i]]->GetNofTuples();
        }

        estimatedDoubleValue = 1000.0;
        while(tree!=NULL)
        {
            estimatedDoubleValue*=EstimateTuples(tree->left,longValueMap);
            tree=tree->rightAnd;
        }
        tIterator=tValueMap.begin();
        for(;tIterator!=tValueMap.end();tIterator++)
        {
            estimatedDoubleValue*=tIterator->second;
        }
    }
    estimatedDoubleValue = estimatedDoubleValue/1000.0;
    return estimatedDoubleValue;
}

double Statistics::EstimateTuples(struct OrList *orList, map<string,long> &uniqueValueList)
{

    struct ComparisonOp *compOpStruct;
    map<string,double> valueSelectionMap;

    while(orList!=NULL)
    {
        compOpStruct=orList->left;
        string selectionKey = string(compOpStruct->left->value);
        if(valueSelectionMap.find(selectionKey)==valueSelectionMap.end())
        {
            valueSelectionMap[selectionKey]=0.0;
        }
        if(compOpStruct->code == LESS_THAN || compOpStruct->code == GREATER_THAN)
        {
            valueSelectionMap[selectionKey] = valueSelectionMap[selectionKey]+1.0/3;
        }
        else
        {
            string leftKeyVal = string(compOpStruct->left->value);
            long max_val = uniqueValueList[leftKeyVal];
            if(compOpStruct->right->code==NAME)
            {
               string rightKeyVal = string(compOpStruct->right->value);
               if(max_val<uniqueValueList[rightKeyVal])
                   max_val = uniqueValueList[rightKeyVal];
            }
            valueSelectionMap[selectionKey] =valueSelectionMap[selectionKey] + 1.0/max_val;
        }
        orList=orList->rightOr;
    }

    double selectivity=1.0;
    map<string,double>::iterator iteratorObj = valueSelectionMap.begin();
    for(;iteratorObj!=valueSelectionMap.end();iteratorObj++)
        selectivity*=(1.0-iteratorObj->second);
    return (1.0-selectivity);
}

/*
 *  This Function checks whether the given attributes belong to relations in relation list or not
*/
bool Statistics::CheckForAttribute(char *v,char *relationNames[], int numberOfJoinAttributes,map<string,long> &uniqueValueList)
{
    int i=0;
    while(i<numberOfJoinAttributes)
    {
        map<string,RelStats*>::iterator iteratorObj=statsMap.find(relationNames[i]);
        // if stats are not empty
        if(iteratorObj!=statsMap.end())
        {   
            string relation = string(v);
            if(iteratorObj->second->GetRelationAttributes()->find(relation)!=iteratorObj->second->GetRelationAttributes()->end())
            {
                // update value in uniqueValueList
                uniqueValueList[relation]=iteratorObj->second->GetRelationAttributes()->find(relation)->second;
                return true;
            }
        }
        else {
            // empty stats
            return false;
        }
        i++;
    }
    return false;
}



/*
    ehile estimating checkParseTreeAndPartition function puts an additonal check 
    so that Passed CNF predicate does not use any attributes outside the list of 
    attributes passed in relations parameter.
*/
bool Statistics::checkParseTreeAndPartition(struct AndList *tree, char *relationNames[], int numberOfAttributesJoin,map<string,long> &uniqueValueList)
{
    // boolean for return value
    bool returnValue=true;

    // while tree is not parsed and returnValue is not false
    while(tree!=NULL && returnValue)
    {
        // get the left most orList of parse tree
        struct OrList *orListTop=tree->left;

        // traverse the orList until it becomes null and returnValue is not false
        while(orListTop!=NULL && returnValue)
        {
            // get pointer to the comparison operator
            struct ComparisonOp *cmpPtr = orListTop->left;

            // left of comparison operator should be an attribute and right should be a string
            // check whether the attributes used belong to the relations listed in relationNames
            if(!CheckForAttribute(cmpPtr->left->value,relationNames,numberOfAttributesJoin,uniqueValueList) 
                && cmpPtr->left->code==NAME 
                && cmpPtr->code==STRING) {
                cout<<"\n"<< cmpPtr->left->value<<" Does Not Exist";
                returnValue=false;
            }

            // left of comparison operator should be an attribute and right should be a string
            // check whether the attributes used belong to the relations listed in relationNames
            if(!CheckForAttribute(cmpPtr->right->value,relationNames,numberOfAttributesJoin,uniqueValueList) 
                && cmpPtr->right->code==NAME 
                && cmpPtr->code==STRING) {
                returnValue=false;
            }
            // now move to the right OR after we've seen the left one and keep moving until the end
            orListTop=orListTop->rightOr;
        }
        // after the OR parsing is complete, we'll now go to the right OR until the ANDs end
        tree=tree->rightAnd;
    }

    // if false, return
    if(!returnValue) return returnValue;

    // for number of
    map<string,int> tbl;
    for(int i=0;i<numberOfAttributesJoin;i++)
    {
        string gn = statsMap[string(relationNames[i])]->GetGroupName();
        if(tbl.find(gn)!=tbl.end())
            tbl[gn]--;
        else
            tbl[gn] = statsMap[string(relationNames[i])]->GetGroupSize()-1;
    }

    map<string,int>::iterator ti;
    for( ti=tbl.begin();ti!=tbl.end();ti++)
    {
        if(ti->second!=0)
        {
            returnValue=false;
            break;
        }
    }
    return returnValue;
}

