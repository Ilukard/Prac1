#include "struct.h"
#include <filesystem>
#include "delete.h"
#include "readTable.h"

using namespace std;
namespace fs = filesystem;

void Delete(const string& sql, const Schema& schema) {
    size_t deletePos = sql.find("DELETE FROM");
    size_t wherePos = sql.find("WHERE");

    if (deletePos == string::npos) {
        throw runtime_error("Неверный формат запроса DELETE");
    }

    string tableName = sql.substr(deletePos + 11);
    if (wherePos != string::npos) {
        tableName = tableName.substr(0, wherePos - (deletePos + 11));
    }

    tableName.erase(0, tableName.find_first_not_of(" \t\n\r\f\v"));
    tableName.erase(tableName.find_last_not_of(" \t\n\r\f\v") + 1);


    const Table* table = nullptr;
    for (const auto& t : schema.tables) {
        if (t.name == tableName) {
            table = &t;
            break;
        }
    }

    if (table == nullptr) {
        throw runtime_error("Таблица не найдена: " + tableName);
    }

    fs::path tableDir = fs::path(schema.name) / tableName;
    int fileCounter = 1;
    fs::path filePath;

    while (true) {
        filePath = tableDir / (to_string(fileCounter) + ".csv");
        if (fs::exists(filePath)) break;
        fileCounter++;
    }
    
    vector<vector<string>> data = readTableFromCSV(filePath);
    
    if(wherePos != string::npos){
        string whereClause = sql.substr(wherePos + 5);
        size_t eqPos = whereClause.find('=');
        if (eqPos == string::npos) {
            throw runtime_error("Неверный формат WHERE");
        }
        string columnCondition = whereClause.substr(0, eqPos);
        string valueCondition = whereClause.substr(eqPos + 1);
        
        columnCondition.erase(0, columnCondition.find_first_not_of(" \t\n\r\f\v"));
        columnCondition.erase(columnCondition.find_last_not_of(" \t\n\r\f\v") + 1);
        valueCondition.erase(0, valueCondition.find_first_not_of(" \t\n\r\f\v"));
        valueCondition.erase(valueCondition.find_last_not_of(" \t\n\r\f\v") + 1);

        if(valueCondition.length() > 2 && valueCondition[0] == '\'' && valueCondition.back() == '\''){
            valueCondition = valueCondition.substr(1, valueCondition.length() - 2);
        }

        int columnIndex = -1;
        for (size_t i = 0; i < table->columns.size(); i++) {
            if (table->columns[i] == columnCondition) {
                columnIndex = i;
                break;
            }
        }
        if (columnIndex == -1) {
            throw runtime_error("Столбец в WHERE не найден: " + columnCondition);
        }
        
        vector<vector<string>> filteredData;
        filteredData.push_back(data[0]);
        for(size_t i = 1; i < data.size(); ++i){
            if(data[i][columnIndex] != valueCondition){
                filteredData.push_back(data[i]);
            }
        }
        data = filteredData;
    } else {
        throw runtime_error("Необходимо указать условие WHERE для DELETE");
    }


    ofstream file(filePath);
    if (!file.is_open()) {
        throw runtime_error("Не удалось открыть файл для записи: " + filePath.string());
    }

    for (size_t i = 0; i < data.size(); ++i) {
        for (size_t j = 0; j < data[i].size(); ++j) {
            file << data[i][j];
            if (j < data[i].size() - 1) {
                file << ",";
            }
        }
        file << endl;
    }
    file.close();
}