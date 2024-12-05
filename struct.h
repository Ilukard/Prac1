#include <string>
#include <vector>
using namespace std;

#ifndef STRUCT_
#define STRUCT_

struct Table {
    string name; //таблица 1
    vector<string> columns;
};

struct Schema {
    string name; // база данных
    int tuples_limit;
    vector<Table> tables;
};
#endif