#ifndef DATABASEMANAGER_H
#define DATABASEMANAGER_H

#include <iostream>
using namespace std;

class DatabaseManager {
public:
    static bool createDatabase(const string& dbName);
    static bool dropDatabase(const string& dbName);
    static bool databaseExists(const string& dbName);
};

#endif
