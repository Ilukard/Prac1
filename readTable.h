#ifndef READ_TABLE_
#define READ_TABLE_
#include <filesystem>
#include "struct.h"
namespace fs = filesystem;
vector<vector<string>> readTableFromCSV(const fs::path& filePath);
#endif