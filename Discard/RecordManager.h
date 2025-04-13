#ifndef RECORDMANAGER_H
#define RECORDMANAGER_H

#include <iostream>
#include <vector>
using namespace std;

class RecordManager {
public:
    static bool insertRecord(const string& dbName, const string& tableName, const vector<string>& values);
    static bool selectAll(const string& dbName, const string& tableName);
    static bool selectWithWhere(const string& dbName, const string& tableName, int columnIndex, const string& condition);
    static bool updateRecord(const string& dbName, const string& tableName, int columnIndex, const string& oldValue, const string& newValue);
    static bool deleteRecord(const string& dbName, const string& tableName, int columnIndex, const string& condition);
};

#endif
