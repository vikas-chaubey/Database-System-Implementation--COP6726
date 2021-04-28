#include <string>
#include <fstream>
#include "DBFile.h"
#include "Statistics.h"
#include "QueryPlanner.h"
#include <gtest/gtest.h>

extern "C" int yyparse(void);

QueryPlanner qp;

TEST(QueryTesting, CheckQueryAlias) {
    qp.PopulateAliasMapAndCopyStatistics();
    std::map<std::string, string> tables = {
		{"p","part"},
		{"ps","partsupp"},
        {"s","supplier"}
    };
    auto tableIterator = tables.cbegin();
    for(auto it = qp.aliasMap.cbegin(); it != qp.aliasMap.cend(); ++it)
    {
        ASSERT_STREQ(tableIterator->first.c_str(), it->first.c_str());
        ASSERT_STREQ(tableIterator->second.c_str(), it->second.c_str());
        ++tableIterator;
    }
}

TEST(QueryTesting, TestJoinOptimization) {
    qp.Optimise();
    vector<char *> assertion = {"s","ps","p"};
    for (int i=0; i<qp.TableOrderForJoin.size();i++) {
        ASSERT_STREQ(assertion.at(i), qp.TableOrderForJoin.at(i));
    }
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}