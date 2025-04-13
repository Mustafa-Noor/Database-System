#include "QueryProcessor.h"
#include <iostream>
using namespace std;

int main() {
    string query;
    while (true) {
        cout << "LAVIDA> ";
        getline(cin, query);
        if (query == "EXIT") break;
        QueryProcessor::executeQuery(query);
    }
    return 0;
}
