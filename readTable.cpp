#include "struct.h"
#include <filesystem>
#include "readTable.h"

using namespace std;
namespace fs = filesystem;

vector<vector<string>> readTableFromCSV(const fs::path& filePath) {
    vector<vector<string>> tableData;
    ifstream file(filePath);

    if (!file.is_open()) {
        throw runtime_error("Невозможно открыть CSV файл: " + filePath.string());
    }

    string line;
    while (getline(file, line)) {
        vector<string> row;
        stringstream ss(line);
        string cell;
        while (getline(ss, cell, ',')) {
            row.push_back(cell);
        }
        tableData.push_back(row);
    }

    file.close();
    return tableData;
}
