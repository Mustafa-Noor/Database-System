#ifndef QUERY_PROCESSOR_H
#define QUERY_PROCESSOR_H

#include <string>
using namespace std;

class QueryProcessor {
private:
    static std::string currentDatabase;  // Store the active database
public:
    static void executeQuery(const std::string& query);
};

#endif // QUERY_PROCESSOR_H
