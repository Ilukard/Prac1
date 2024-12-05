#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "json.hpp"
#include <filesystem>
#include "struct.h"
#include "insert.h"
#include "delete.h"

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

int main() {
    // ЭСКЬЮЭЛЬ
        Schema schema = readSchema("schema.json");
        createDirectories(schema);
        string sql = "INSERT INTO таблица1 VALUES hello, brave, new, world";
        Insert(sql, schema);
        sql = "INSERT INTO таблица2 VALUES hello, bratan";
        Insert(sql, schema);
    return 0;
}