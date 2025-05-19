#include "shared_data.h"  
  
std::unordered_map<std::string, std::vector<float> >sentence2line;
std::vector<std::vector<float>> embedding_lines;
std::vector<std::string> valid_sentences;
bool embedding_loaded = false;
size_t embedding_dim = 0;