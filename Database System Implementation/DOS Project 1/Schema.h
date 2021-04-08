
#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdio.h>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

/*
Finally, the fifth header file (Schema.h) encodes a few functions that load up a relation
schema from the database catalog. An example database catalog is given in the file
catalog. In fact, this catalog gives the relation schemas for several of the tables from
the “TPC-H” databases schema, which you will become very familiar with over the
semester. The TPC-H is a very well-known standard database benchmark that we will
make extensive use of in this class. One of the first things that you should do when you
start on this assignment is to check out http://www.tpc.org/tpch, and at least look over the
PDF document describing this schema.
*/

struct Attribute {

	char *name;
	Type myType;
};

class OrderMaker;
class Schema {

	// gives the attributes in the schema
	int numAtts;
	Attribute *myAtts;

	// gives the physical location of the binary file storing the relation
	const char *fileName;

	friend class Record;

public:

	// gets the set of attributes, but be careful with this, since it leads
	// to aliasing!!!
	Attribute *GetAtts ();

	// returns the number of attributes
	int GetNumAtts ();

	// this finds the position of the specified attribute in the schema
	// returns a -1 if the attribute is not present in the schema
	int Find (const char *attName);

	// this finds the type of the given attribute
	Type FindType (const char *attName);

	// this reads the specification for the schema in from a file
	Schema (const char *fName, const char *relName);

	// this constructs a sort order structure that can be used to
	// place a lexicographic ordering on the records using this type of schema
	int GetSortOrder (OrderMaker &order);

	~Schema ();

};

#endif
