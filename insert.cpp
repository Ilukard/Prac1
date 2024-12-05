#include "struct.h"
#include <filesystem>
#include "insert.h"
#include "readTable.h"

using namespace std;
namespace fs = filesystem;

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