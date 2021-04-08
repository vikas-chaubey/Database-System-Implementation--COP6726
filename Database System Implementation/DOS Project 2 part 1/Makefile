CC = g++ -std=c++11 -O2 -Wno-deprecated  

tag = -i

CURR_DIR = $(shell pwd)
GTEST_DIR = $(CURR_DIR)/googletest/googletest

# command given in gtest Makefile
DD = $(CC) -isystem "$(GTEST_DIR)/include" -Wall -Wextra -pthread 

ifdef linux
tag = -n
endif

loadtables.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o loadtables.o
	$(CC) -o loadtables.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o loadtables.o -lfl

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -lfl -lpthread

gtest.out: libgtest.a Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o 
	$(DD) gtest.cc libgtest.a -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o  -lfl


libgtest.a: 
	g++ -w -isystem "$(GTEST_DIR)/include" -I"$(GTEST_DIR)" -pthread -c "$(GTEST_DIR)/src/gtest-all.cc"
	ar -rv libgtest.a gtest-all.o

	
test.o: test.cc
	$(CC) -g -c test.cc

loadtables.o: loadtables.cc
	$(CC) -g -c loadtables.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f *.xbin
	rm -f dbfiles/*.bigq
	rm -f dbfiles/*.pref
	rm -f output1.txt
