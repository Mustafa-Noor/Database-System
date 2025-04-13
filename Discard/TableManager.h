#ifndef TABLEMANAGER_H
#define TABLEMANAGER_H

#include <iostream>
#include <vector>
using namespace std;

struct Column {
    string name;
    string type;
    int size;
};

class TableManager {
public:
    static bool createTable(const string& dbName, const string& tableName, const vector<Column>& columns);
    static bool dropTable(const string& dbName, const string& tableName);
    static int getColumnIndex(const string& dbName, const string& tableName, const string& columnName);
};

#endif
