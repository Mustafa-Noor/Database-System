#include "DatabaseManager.h"
#include <sys/stat.h>
#include <iostream>
#include <dirent.h>

#ifdef _WIN32
    #include <direct.h>
    #define MKDIR(path) _mkdir(path.c_str())
#else
    #include <unistd.h>
    #define MKDIR(path) mkdir(path.c_str(), 0777)
#endif

bool DatabaseManager::createDatabase(const std::string& dbName) {
    std::string fullPath = "databases/" + dbName;
    
    if (databaseExists(dbName)) {
        std::cout << "Database '" << dbName << "' already exists.\n";
        return false;
    }

    if (MKDIR(fullPath) == 0) {
        std::cout << "Database '" << dbName << "' created successfully.\n";
        return true;
    }

    std::cerr << "Error creating database directory.\n";
    return false;
}

bool DatabaseManager::dropDatabase(const std::string& dbName) {
    std::string fullPath = "databases/" + dbName;

    if (!databaseExists(dbName)) {
        std::cerr << "Error: Database '" << dbName << "' does not exist.\n";
        return false;
    }

    // Delete all files in the database folder before removing it
    DIR* dir = opendir(fullPath.c_str());
    if (!dir) return false;

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name(entry->d_name);
        if (name == "." || name == "..") continue;
        std::string fullEntryPath = fullPath + "/" + name;
        remove(fullEntryPath.c_str()); // Delete file
    }
    closedir(dir);

    if (rmdir(fullPath.c_str()) == 0) {
        std::cout << "Database '" << dbName << "' deleted successfully.\n";
        return true;
    } else {
        std::cerr << "Error deleting database directory.\n";
        return false;
    }
}

bool DatabaseManager::databaseExists(const std::string& dbName) {
    struct stat info;
    std::string fullPath = "databases/" + dbName;
    return (stat(fullPath.c_str(), &info) == 0 && (info.st_mode & S_IFDIR));
}
