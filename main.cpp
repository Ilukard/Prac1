#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "json.hpp"
#include <filesystem>
#include "struct.h"

using json = nlohmann::json;
using namespace std;
namespace fs = filesystem;



Schema readSchema(const string& filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        throw runtime_error("Невозможно открыть файл schema.json");
    }

    json json_schema;
    file >> json_schema;

    Schema schema;
    schema.name = json_schema["name"];
    schema.tuples_limit = json_schema["tuples_limit"];

    for (const auto& [tableName, columns] : json_schema["structure"].items()) {
        Table table;
        table.name = tableName;
        for (const string& column : columns) {
            table.columns.push_back(column);
        }
        schema.tables.push_back(table);
    }

    return schema;
}

void createDirectories(const Schema& schema) {
    fs::path schemaDir = schema.name;

    // Создаём директорию схемы
    if (!fs::exists(schemaDir)) {
        fs::create_directory(schemaDir);
    }

    for (const auto& table : schema.tables) {
        fs::path tableDir = schemaDir / table.name;
        if (!fs::exists(tableDir)) {
            fs::create_directory(tableDir);
        }

        // Создаём файлы CSV для таблицы (пока только один на таблицу)
        fs::path csvFile = tableDir / "1.csv";
        ofstream file(csvFile);
        if (file.is_open()) {
            // Записываем заголовки столбцов
            for (size_t i = 0; i < table.columns.size(); ++i) {
                file << table.columns[i];
                if (i < table.columns.size() - 1) {
                    file << ",";
                }
            }
            file << endl;
            file.close();
        } else {
            throw runtime_error("Не удалось создать CSV файл для таблицы " + table.name);
        }


        //Добавление первичного ключа
        fs::path pk_sequence_file = tableDir / (table.name + "_pk_sequence");
        ofstream pk_file(pk_sequence_file);
        if(pk_file.is_open()){
            pk_file << "0" << endl;
            pk_file.close();
        } else {
            throw runtime_error("Не удалось создать файл первичного ключа для таблицы " + table.name);
        }

        //Добавление файла блокировки
        fs::path lock_file = tableDir / (table.name + "_lock");
        ofstream lock_file_stream(lock_file);
        if(lock_file_stream.is_open()){
            lock_file_stream.close();
        } else {
            throw runtime_error("Не удалось создать файл блокировки для таблицы " + table.name);
        }
    }
}

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

void Insert(const string& sql, const Schema& schema) {
    size_t insertPos = sql.find("INSERT INTO");
    size_t valuesPos = sql.find("VALUES");

    if (insertPos == string::npos || valuesPos == string::npos) {
        throw runtime_error("Неверный формат запроса INSERT");
    }

    string tableName = sql.substr(insertPos + 11, valuesPos - insertPos - 11);
    string valuesStr = sql.substr(valuesPos + 7);

    // Удаляем лишние пробелы
    tableName.erase(0, tableName.find_first_not_of(" \t\n\r\f\v"));
    tableName.erase(tableName.find_last_not_of(" \t\n\r\f\v") + 1);
    valuesStr.erase(0, valuesStr.find_first_not_of(" \t\n\r\f\v"));
    valuesStr.erase(valuesStr.find_last_not_of(" \t\n\r\f\v") + 1);

    // Находим таблицу в схеме
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

    vector<string> values;
    stringstream ssValues(valuesStr);
    string value;
    while (getline(ssValues, value, ',')) {
        // Удаляем лишние пробелы и кавычки
        value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
        value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);
        if (value.length() > 2 && value[0] == '\'' && value.back() == '\'') {
            value = value.substr(1, value.length() - 2);
        }
        values.push_back(value);
    }

    if (values.size() + 1 != table->columns.size()) {
        throw runtime_error("Несоответствие количества значений и столбцов");
    }

    fs::path tableDir = fs::path(schema.name) / tableName;
    int fileCounter = 1;
    fs::path filePath;
    bool fileCreated = false;

    while (true) {
        filePath = tableDir / (to_string(fileCounter) + ".csv");
        if (!fs::exists(filePath)) {
            fileCreated = true;
            break;
        }

        //Проверяем, заполнен ли файл
        ifstream f(filePath);
        string line;
        int rowCount = 0;
        while(getline(f, line)){
          rowCount++;
        }
        f.close();
        if(rowCount < schema.tuples_limit){
          break;
        }
        fileCounter++;
    }

    ofstream file(filePath, ios::app);
    if (!file.is_open()) {
        throw runtime_error("Не удалось открыть файл для записи: " + filePath.string());
    }

    //Добавление первичного ключа
    fs::path pk_sequence_file = tableDir / (tableName + "_pk_sequence");
    ifstream pk_file(pk_sequence_file);
    long long pk_value;
    pk_file >> pk_value;
    pk_file.close();

    ofstream pk_file_out(pk_sequence_file);
    pk_value++;
    pk_file_out << pk_value << endl;
    pk_file_out.close();

    values.insert(values.begin(), to_string(pk_value)); //Добавляем первичный ключ в начало

        if (fileCreated) { // Добавляем заголовок только если создан новый файл
        for (size_t i = 0; i < table->columns.size(); ++i) {
            file << table->columns[i];
            if (i < table->columns.size() - 1) {
                file << ",";
            }
        }
        file << endl;
    }

    for (size_t i = 0; i < values.size(); ++i) {
        file << values[i];
        if (i < values.size() - 1) {
            file << ",";
        }
    }
    file << endl;
    file.close();

}

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

vector<vector<string>> SelectJoin(const string& sql, const Schema& schema) {
    size_t selectPos = sql.find("SELECT");
    size_t fromPos = sql.find("FROM");

    if (selectPos == string::npos || fromPos == string::npos) {
        throw runtime_error("Неверный формат запроса SELECT");
    }

    string columnsStr = sql.substr(selectPos + 6, fromPos - selectPos - 6);
    string tablesStr = sql.substr(fromPos + 4);

    vector<string> selectedColumns;
    stringstream ssColumns(columnsStr);
    string column;
    while (getline(ssColumns, column, ',')) {
        column.erase(0, column.find_first_not_of(" \t\n\r\f\v"));
        column.erase(column.find_last_not_of(" \t\n\r\f\v") + 1);
        selectedColumns.push_back(column);
    }

    vector<const Table*> tables; 
    stringstream ssTables(tablesStr);
    string tableNamePart;
    while (getline(ssTables, tableNamePart, ',')) {
        tableNamePart.erase(0, tableNamePart.find_first_not_of(" \t\n\r\f\v"));
        tableNamePart.erase(tableNamePart.find_last_not_of(" \t\n\r\f\v") + 1);
        const Table* table = nullptr; // Изменено: теперь указатель на константный объект
        for (const auto& t : schema.tables) { // Изменено: используем const auto& для итерации по константному вектору
            if (t.name == tableNamePart) {
                table = &t;
                break;
            }
        }
        if (table == nullptr) {
            throw runtime_error("Таблица не найдена: " + tableNamePart);
        }
        tables.push_back(table);
    }

    fs::path filePath1 = fs::path(schema.name) / tables[0]->name / "1.csv";
    fs::path filePath2 = fs::path(schema.name) / tables[1]->name / "1.csv";

    vector<vector<string>> data1 = readTableFromCSV(filePath1);
    vector<vector<string>> data2 = readTableFromCSV(filePath2);

    vector<vector<string>> result;
    for (size_t i = 1; i < data1.size(); ++i) {
        for (size_t j = 1; j < data2.size(); ++j) {
            vector<string> row;
            for (const string& colName : selectedColumns) {
                size_t dotPos = colName.find('.');
                string tableNamePart = colName.substr(0, dotPos);
                string columnNamePart = colName.substr(dotPos + 1);
                int tableIndex = (tableNamePart == tables[0]->name) ? 0 : 1;
                int columnIndex = -1;
                for (size_t k = 0; k < tables[tableIndex]->columns.size(); ++k) {
                    if (tables[tableIndex]->columns[k] == columnNamePart) {
                        columnIndex = k;
                        break;
                    }
                }
                if (columnIndex == -1) throw runtime_error("Столбец не найден: " + colName);
                row.push_back((tableIndex == 0) ? data1[i][columnIndex] : data2[j][columnIndex]);
            }
            result.push_back(row);
        }
    }
    return result;
}

vector<vector<string>> SelectWhere(const string& sql, const Schema& schema) {
    size_t selectPos = sql.find("SELECT");
    size_t fromPos = sql.find("FROM");
    size_t wherePos = sql.find("WHERE");

    if (selectPos == string::npos || fromPos == string::npos) {
        throw runtime_error("Неверный формат запроса SELECT");
    }

    string columnsStr = sql.substr(selectPos + 6, fromPos - selectPos - 6);
    string tablesStr = sql.substr(fromPos + 4);
    string whereClause = (wherePos != string::npos) ? sql.substr(wherePos + 5) : "";

    vector<string> selectedColumns;
    stringstream ssColumns(columnsStr);
    string column;
    while (getline(ssColumns, column, ',')) {
        column.erase(0, column.find_first_not_of(" \t\n\r\f\v"));
        column.erase(column.find_last_not_of(" \t\n\r\f\v") + 1);
        selectedColumns.push_back(column);
    }

    vector<const Table*> tables;
    stringstream ssTables(tablesStr);
    string tableNamePart;
    while (getline(ssTables, tableNamePart, ',')) {
        tableNamePart.erase(0, tableNamePart.find_first_not_of(" \t\n\r\f\v"));
        tableNamePart.erase(tableNamePart.find_last_not_of(" \t\n\r\f\v") + 1);
        const Table* table = nullptr;
        for (const auto& t : schema.tables) {
            if (t.name == tableNamePart) {
                table = &t;
                break;
            }
        }
        if (table == nullptr) {
            throw runtime_error("Таблица не найдена: " + tableNamePart);
        }
        tables.push_back(table);
    }

    fs::path filePath1 = fs::path(schema.name) / tables[0]->name / "1.csv";
    fs::path filePath2 = fs::path(schema.name) / tables[1]->name / "1.csv";

    vector<vector<string>> data1 = readTableFromCSV(filePath1);
    vector<vector<string>> data2 = readTableFromCSV(filePath2);

    vector<vector<string>> result;
    for (size_t i = 1; i < data1.size(); ++i) {
        for (size_t j = 1; j < data2.size(); ++j) {
            bool conditionMet = true;
            if (!whereClause.empty()) {
                stringstream ssWhere(whereClause);
                string condition;
                while (getline(ssWhere, condition, ' ')) {
                    //Обработка AND
                    size_t andPos = condition.find("AND");
                    if(andPos != string::npos){
                        string condition1 = condition.substr(0, andPos);
                        string condition2 = condition.substr(andPos + 3);
                        condition = condition1;
                        
                    }

                    size_t eqPos = condition.find('=');
                    if (eqPos == string::npos) {
                        throw runtime_error("Неверный формат WHERE");
                    }
                    string leftOperand = condition.substr(0, eqPos);
                    string rightOperand = condition.substr(eqPos + 1);

                    size_t dotPos = leftOperand.find('.');
                    string tableNamePart = leftOperand.substr(0, dotPos);
                    string columnNamePart = leftOperand.substr(dotPos + 1);
                    int tableIndex = (tableNamePart == tables[0]->name) ? 0 : 1;
                    int columnIndex = -1;
                    for (size_t k = 0; k < tables[tableIndex]->columns.size(); ++k) {
                        if (tables[tableIndex]->columns[k] == columnNamePart) {
                            columnIndex = k;
                            break;
                        }
                    }
                    if (columnIndex == -1) throw runtime_error("Столбец не найден: " + leftOperand);

                    rightOperand.erase(0, rightOperand.find_first_not_of(" \t\n\r\f\v"));
                    rightOperand.erase(rightOperand.find_last_not_of(" \t\n\r\f\v") + 1);
                    if(rightOperand.length() > 2 && rightOperand[0] == '\'' && rightOperand.back() == '\''){
                        rightOperand = rightOperand.substr(1, rightOperand.length() - 2);
                    }

                    string value = (tableIndex == 0) ? data1[i][columnIndex] : data2[j][columnIndex];
                    if (value != rightOperand) conditionMet = false;
                }
            }

            if (conditionMet) {
                vector<string> row;
                for (const string& colName : selectedColumns) {
                    size_t dotPos = colName.find('.');
                    string tableNamePart = colName.substr(0, dotPos);
                    string columnNamePart = colName.substr(dotPos + 1);
                    int tableIndex = (tableNamePart == tables[0]->name) ? 0 : 1;
                    int columnIndex = -1;
                    for (size_t k = 0; k < tables[tableIndex]->columns.size(); ++k) {
                        if (tables[tableIndex]->columns[k] == columnNamePart) {
                            columnIndex = k;
                            break;
                        }
                    }
                    if (columnIndex == -1) throw runtime_error("Столбец не найден: " + colName);
                    row.push_back((tableIndex == 0) ? data1[i][columnIndex] : data2[j][columnIndex]);
                }
                result.push_back(row);
            }
        }
    }
    return result;
}

void print(const vector<vector<string>>& result) {
    if (result.empty()) {
        cout << "Результат пуст" << endl;
        return;
    }

    // Выводим заголовок (первая строка результата, если она есть)
    if (result[0].size() > 0) {
        for (size_t i = 0; i < result[0].size(); ++i) {
            cout << result[0][i] << "\t";
        }
        cout << endl;
    }

    // Выводим данные
    for (size_t i = 1; i < result.size(); ++i) {
        for (size_t j = 0; j < result[i].size(); ++j) {
            cout << result[i][j] << "\t";
        }
        cout << endl;
    }
}

int main() {
    try {
        Schema schema = readSchema("schema.json");
        createDirectories(schema);

        ifstream sqlFile("SQL.txt");
        if (!sqlFile.is_open()) {
            throw runtime_error("Не удалось открыть файл SQL.txt");
        }

        string sqlLine;
        while (getline(sqlFile, sqlLine)) {
            sqlLine.erase(0, sqlLine.find_first_not_of(" \t\n\r\f\v"));
            sqlLine.erase(sqlLine.find_last_not_of(" \t\n\r\f\v") + 1);

            if (sqlLine.empty()) continue;

            size_t selectPos = sqlLine.find("SELECT");
            size_t insertPos = sqlLine.find("INSERT INTO");
            size_t deletePos = sqlLine.find("DELETE FROM");

            try {
                if (selectPos != string::npos) {
                    size_t fromPos = sqlLine.find("FROM");
                    size_t wherePos = sqlLine.find("WHERE");

                    if(fromPos == string::npos){
                        throw runtime_error("Неверный формат запроса SELECT");
                    }

                    if (wherePos != string::npos) {
                        vector<vector<string>> result = SelectWhere(sqlLine, schema);
                        print(result); 
                     } else {
                        vector<vector<string>> result = SelectJoin(sqlLine, schema);
                        print(result);
                    }
                } else if (insertPos != string::npos) {
                    Insert(sqlLine, schema);
                    cout << "Строка успешно добавлена" << endl;
                } else if (deletePos != string::npos) {
                    Delete(sqlLine, schema);
                    cout << "Строки успешно удалены" << endl;
                } else {
                    throw runtime_error("Неизвестная SQL-команда: " + sqlLine);
                }
            } catch (const runtime_error& error) {
                cerr << "Ошибка при выполнении запроса: " << error.what() << endl;
            }
        }
        sqlFile.close();
    } catch (const runtime_error& error) {
        cerr << "Ошибка: " << error.what() << endl;
        return 1;
    }

    return 0;
}