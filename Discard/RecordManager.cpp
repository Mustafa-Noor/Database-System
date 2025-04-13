#include "RecordManager.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>  

using namespace std;

// Constants
const int COLUMN_SIZE = 50;  
const int NUM_COLUMNS = 4;   
const int RECORD_SIZE = COLUMN_SIZE * NUM_COLUMNS;  

/**
 * Insert a record into the database
 */
bool RecordManager::insertRecord(const string& dbName, const string& tableName, const vector<string>& values) {
    string filePath = "databases/" + dbName + "/" + tableName + ".bin";
    ofstream file(filePath, ios::binary | ios::app); 

    if (!file) {
        cerr << "Error opening binary file for writing.\n";
        return false;
    }

    char buffer[COLUMN_SIZE] = {0};  
    for (const auto& value : values) {
        memset(buffer, 0, COLUMN_SIZE);
        strncpy(buffer, value.c_str(), COLUMN_SIZE - 1);
        file.write(buffer, COLUMN_SIZE);
    }

    file.close();
    return true;
}

/**
 * Select all records from the table
 */
bool RecordManager::selectAll(const string& dbName, const string& tableName) {
    string filePath = "databases/" + dbName + "/" + tableName + ".bin";
    ifstream file(filePath, ios::binary);

    if (!file) {
        cerr << "Error: Could not open file for reading: " << filePath << endl;
        return false;
    }

    char buffer[COLUMN_SIZE];
    int columnCounter = 0;

    while (file.read(buffer, COLUMN_SIZE)) {
        cout << string(buffer) << " ";
        columnCounter++;

        if (columnCounter == NUM_COLUMNS) {
            cout << endl;
            columnCounter = 0;
        }
    }

    file.close();
    return true;
}

/**
 * Select records with a condition (WHERE clause)
 */
bool RecordManager::selectWithWhere(const string& dbName, const string& tableName, int columnIndex, const string& condition) {
    string filePath = "databases/" + dbName + "/" + tableName + ".bin";
    ifstream file(filePath, ios::binary);

    if (!file) {
        cerr << "Error: Could not open file for reading: " << filePath << endl;
        return false;
    }

    char buffer[RECORD_SIZE];

    while (file.read(buffer, RECORD_SIZE)) {
        char columnBuffer[COLUMN_SIZE] = {0};
        memcpy(columnBuffer, buffer + (columnIndex * COLUMN_SIZE), COLUMN_SIZE);
        string columnValue(columnBuffer);

        if (columnValue.find(condition) != string::npos) {
            for (int i = 0; i < NUM_COLUMNS; i++) {
                char col[COLUMN_SIZE] = {0};
                memcpy(col, buffer + (i * COLUMN_SIZE), COLUMN_SIZE);
                cout << string(col) << " ";
            }
            cout << endl;
        }
    }

    file.close();
    return true;
}

/**
 * Update a record in the table
 */
bool RecordManager::updateRecord(const string& dbName, const string& tableName, int columnIndex, const string& oldValue, const string& newValue) {
    string filePath = "databases/" + dbName + "/" + tableName + ".bin";
    fstream file(filePath, ios::binary | ios::in | ios::out);

    if (!file) {
        cerr << "Error: Could not open file for updating: " << filePath << endl;
        return false;
    }

    char buffer[RECORD_SIZE];

    while (file.read(buffer, RECORD_SIZE)) {
        char columnBuffer[COLUMN_SIZE] = {0};
        memcpy(columnBuffer, buffer + (columnIndex * COLUMN_SIZE), COLUMN_SIZE);
        string columnValue(columnBuffer);

        if (columnValue.find(oldValue) != string::npos) {
            file.seekp(-RECORD_SIZE, ios::cur);
            memset(buffer + (columnIndex * COLUMN_SIZE), 0, COLUMN_SIZE);
            strncpy(buffer + (columnIndex * COLUMN_SIZE), newValue.c_str(), COLUMN_SIZE - 1);
            file.write(buffer, RECORD_SIZE);
            file.flush();  
            cout << "Updated record.\n";
        }
    }

    file.close();
    return true;
}

/**
 * Delete a record from the table
 */
bool RecordManager::deleteRecord(const string& dbName, const string& tableName, int columnIndex, const string& condition) {
    string filePath = "databases/" + dbName + "/" + tableName + ".bin";
    string tempFilePath = filePath + ".tmp";

    ifstream file(filePath, ios::binary);
    ofstream tempFile(tempFilePath, ios::binary);

    if (!file || !tempFile) {
        cerr << "Error: Could not open file for deletion.\n";
        return false;
    }

    char buffer[RECORD_SIZE];
    bool recordDeleted = false;

    while (file.read(buffer, RECORD_SIZE)) {
        char columnBuffer[COLUMN_SIZE] = {0};
        memcpy(columnBuffer, buffer + (columnIndex * COLUMN_SIZE), COLUMN_SIZE);
        string columnValue(columnBuffer);

        if (columnValue.find(condition) == string::npos) {
            tempFile.write(buffer, RECORD_SIZE);
        } else {
            recordDeleted = true;
        }
    }

    file.close();
    tempFile.close();

    remove(filePath.c_str());
    rename(tempFilePath.c_str(), filePath.c_str());

    if (recordDeleted) {
        cout << "Record deleted successfully.\n";
    } else {
        cout << "No matching record found.\n";
    }

    return true;
}
