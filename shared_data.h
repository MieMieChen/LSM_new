#ifndef SHARED_DATA_H
#define SHARED_DATA_H
#include <vector>
#include <string>
#include <unordered_map>

extern std::unordered_map<std::string, std::vector<float> >sentence2line;
extern std::vector<std::vector<float>> embedding_lines;
extern std::vector<std::string> valid_sentences;
extern bool embedding_loaded;
extern size_t embedding_dim;

#endif 