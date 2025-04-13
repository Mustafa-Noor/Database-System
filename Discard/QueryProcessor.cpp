#include "QueryProcessor.h"
#include "DatabaseManager.h"
#include "TableManager.h"
#include "RecordManager.h"
#include <iostream>
#include <sstream>
#include <vector>

using namespace std;

string QueryProcessor::currentDatabase = "";  // Initialize empty

void QueryProcessor::executeQuery(const string& query) {
    istringstream ss(query);
    string command;
    ss >> command;

    if (command == "USE") {
        string dbName;
        ss >> dbName;
        if (DatabaseManager::databaseExists(dbName)) {
            currentDatabase = dbName;
            cout << "Switched to database: " << currentDatabase << endl;
        } else {
            cout << "Error: Database '" << dbName << "' does not exist." << endl;
        }
    }

    else if (command == "CREATE") {
        string type, dbName, tableName, columns;
        ss >> type;
        if (type == "DATABASE") {
            ss >> dbName;
            DatabaseManager::createDatabase(dbName);
        } 
        else if (type == "TABLE") {
            if (currentDatabase.empty()) {
                cout << "Error: No database selected. Use 'USE database_name;' first." << endl;
                return;
            }
            ss >> tableName >> columns;
            vector<Column> columnList;
            stringstream colStream(columns);
            string col;
            while (getline(colStream, col, ',')) {
                string name, type;
                istringstream colSS(col);
                colSS >> name >> type;
                columnList.push_back({name, type, 20});
            }
            TableManager::createTable(currentDatabase, tableName, columnList);
        }
    }

    else if (command == "DROP") {
        string type, name;
        ss >> type >> name;
        if (type == "DATABASE") {
            DatabaseManager::dropDatabase(name);
        } else if (type == "TABLE") {
            if (currentDatabase.empty()) {
                cout << "Error: No database selected. Use 'USE database_name;' first." << endl;
                return;
            }
            TableManager::dropTable(currentDatabase, name);
        }
    }

    else if (command == "INSERT") {
        if (currentDatabase.empty()) {
            cout << "Error: No database selected. Use 'USE database_name;' first." << endl;
            return;
        }
        string tableName, values;
        ss >> tableName >> values;
        vector<string> rowValues;
        stringstream valStream(values);
        string value;
        while (getline(valStream, value, ',')) {
            rowValues.push_back(value);
        }
        RecordManager::insertRecord(currentDatabase, tableName, rowValues);
    }

    else if (command == "SELECT") {
        if (currentDatabase.empty()) {
            cout << "Error: No database selected. Use 'USE database_name;' first." << endl;
            return;
        }

        string selectType, tableName, whereKeyword;
        ss >> selectType >> tableName >> whereKeyword;

        if (whereKeyword == "WHERE") {
            string columnName, op, value;
            ss >> columnName >> op;
            getline(ss, value);
            value = value.substr(1); // Remove leading space

            int columnIndex = TableManager::getColumnIndex(currentDatabase, tableName, columnName);
            if (columnIndex == -1) {
                cout << "Error: Column '" << columnName << "' does not exist in table '" << tableName << "'." << endl;
                return;
            }

            RecordManager::selectWithWhere(currentDatabase, tableName, columnIndex, value);
        } else {
            RecordManager::selectAll(currentDatabase, tableName);
        }
    }

    else if (command == "UPDATE") {
        if (currentDatabase.empty()) {
            cout << "Error: No database selected. Use 'USE database_name;' first." << endl;
            return;
        }

        string tableName, setKeyword, columnName, equalSign, newValue, whereKeyword, whereColumn, op, whereValue;
        ss >> tableName >> setKeyword >> columnName >> equalSign;
        getline(ss, newValue, ' ');
        newValue = newValue.substr(1);

        ss >> whereKeyword >> whereColumn >> op;
        getline(ss, whereValue);
        whereValue = whereValue.substr(1);

        if (setKeyword != "SET" || whereKeyword != "WHERE") {
            cout << "Error: Invalid UPDATE syntax." << endl;
            return;
        }

        int columnIndex = TableManager::getColumnIndex(currentDatabase, tableName, columnName);
        int whereColumnIndex = TableManager::getColumnIndex(currentDatabase, tableName, whereColumn);

        if (columnIndex == -1 || whereColumnIndex == -1) {
            cout << "Error: One or more columns do not exist in table '" << tableName << "'." << endl;
            return;
        }

        RecordManager::updateRecord(currentDatabase, tableName, whereColumnIndex, whereValue, newValue);
    }

    else if (command == "DELETE") {
        if (currentDatabase.empty()) {
            cout << "Error: No database selected. Use 'USE database_name;' first." << endl;
            return;
        }

        string fromKeyword, tableName, whereKeyword, columnName, op, value;
        ss >> fromKeyword >> tableName >> whereKeyword >> columnName >> op;
        getline(ss, value);
        value = value.substr(1);

        if (fromKeyword != "FROM" || whereKeyword != "WHERE") {
            cout << "Error: Invalid DELETE syntax." << endl;
            return;
        }

        int columnIndex = TableManager::getColumnIndex(currentDatabase, tableName, columnName);
        if (columnIndex == -1) {
            cout << "Error: Column '" << columnName << "' does not exist in table '" << tableName << "'." << endl;
            return;
        }

        RecordManager::deleteRecord(currentDatabase, tableName, columnIndex, value);
    }

    else {
        cout << "Error: Unrecognized command." << endl;
    }
}
