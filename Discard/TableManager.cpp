#include "TableManager.h"
#include <fstream>
#include <sstream>
#include <iostream>

bool TableManager::createTable(const string& dbName, const string& tableName, const vector<Column>& columns) {
    string filePath = "databases/" + dbName + "/" + tableName + ".schema";
    ofstream file(filePath);

    if (!file) {
        cerr << "Error: Could not create table schema file.\n";
        return false;
    }

    // Write each column's details
    for (const auto& col : columns) {
        file << col.name << " " << col.type << " " << col.size << endl;
    }

    file.close();
    return true;
}

bool TableManager::dropTable(const string& dbName, const string& tableName) {
    return remove(("databases/" + dbName + "/" + tableName + ".schema").c_str()) == 0;
}


int TableManager::getColumnIndex(const string& dbName, const string& tableName, const string& columnName) {
    string schemaFile = "databases/" + dbName + "/" + tableName + ".schema";
    ifstream file(schemaFile);
    
    if (!file) {
        cerr << "Error: Could not open schema file for table '" << tableName << "'\n";
        return -1;
    }

    string line;
    int index = 0;

    while (getline(file, line)) {
        istringstream ss(line);
        string colName, colType;
        int colSize;
        ss >> colName >> colType >> colSize;

        if (colName == columnName) {
            file.close();
            return index;
        }

        index++;
    }

    file.close();
    return -1;  // Column not found
}
