CC = g++ -std=c++11 -O2 -Wno-deprecated  

tag = -i

CURR_DIR = $(shell pwd)
GTEST_DIR = $(CURR_DIR)/googletest/googletest

# command given in gtest Makefile
DD = $(CC) -isystem "$(GTEST_DIR)/include" -Wall -Wextra -pthread 

ifdef linux
tag = -n
endif


a5.out:   y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Statistics.o main.o
	$(CC) -o a5.out y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Statistics.o main.o -lfl -lpthread
	
gtest.out: libgtest.a y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o Statistics.o
	$(DD) gtest.cc libgtest.a -o gtest y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o RelOp.o  Statistics.o -lfl -lpthread

libgtest.a: 
	g++ -w -isystem "$(GTEST_DIR)/include" -I"$(GTEST_DIR)" -pthread -c "$(GTEST_DIR)/src/gtest-all.cc"
	ar -rv libgtest.a gtest-all.o


main.o : main.cc
	$(CC) -g -c main.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

	
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
	
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc
	
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
