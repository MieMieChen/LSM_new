#include "kvstore.h"
#include <fstream>
#include <iostream>
#include <vector>
#include "shared_data.h"
#include <numeric>
#include <chrono>

std::string trim(const std::string& str) {
    auto first = str.find_first_not_of(" \t\n\r\f\v");
    if (std::string::npos == first) {
        return ""; // 字符串全是空白字符或为空
    }
    auto last = str.find_last_not_of(" \t\n\r\f\v");
    return str.substr(first, (last - first + 1));
}

std::vector<float> parseVectorStringFloat(const std::string& vectorString) {
    std::vector<float> resultVector;
     if (vectorString.length() < 2 || vectorString.front() != '[' || vectorString.back() != ']') {
        // 根据需要处理错误：这里打印错误并返回空向量
        // std::cerr << "Warning: Input string format is not [f1, ...], returning empty vector." << std::endl;
        return resultVector;
    }

    // 提取方括号内的内容
    std::string content = vectorString.substr(1, vectorString.length() - 2);

    // 如果内容为空（例如输入是 "[]"），则返回空向量
    if (content.empty()) {
        return resultVector;
    }
    // 2. 使用 stringstream 和 getline 分割字符串
    std::stringstream ss(content);
    std::string segment;

    // 以逗号为分隔符读取每一个数字的字符串表示
    while (std::getline(ss, segment, ',')) {
        // 3. 去除每个片段两端的空白字符并转换成 float
        std::string trimmedSegment = trim(segment);

        // 检查是否是空字符串（例如输入为 "[1.0, , 2.0]" 中间会有空片段）
        if (trimmedSegment.empty()) {
            // std::cerr << "Warning: Found empty segment, skipping." << std::endl;
            continue; // 跳过这个空的片段
        }

            // 将修剪过的字符串片段转换成 float
            // 使用 std::stof 进行字符串到 float 的转换
            float value = std::stof(trimmedSegment);
            // 4. 将 float 值添加到结果向量
            resultVector.push_back(value);
    }

    return resultVector;
}



void load_sentence_embedding_mapping() {
    if (embedding_loaded) return;
    std::ifstream fin_text("../data/cleaned_text_100k.txt");
    std::ifstream fin_emb("../data/embedding_100k.txt");
    std::string text_line;
    std::string emb_line;
    // int idx = 0;
    // int LineNumber = 0;
    // 读取文本，跳过空行，建立映射
    while (std::getline(fin_text, text_line)&&std::getline(fin_emb, emb_line)) {
        if(text_line.empty()) 
            continue;
        valid_sentences.push_back(text_line);
        std::vector<float> floatVector = parseVectorStringFloat(emb_line);
        embedding_lines.push_back(floatVector);
        // LineNumber++;
    }
    embedding_loaded = true;
    for(int i = 0;i<valid_sentences.size();i++)
    {
       sentence2line[valid_sentences[i]] = embedding_lines[i];
    }
}


int main() {
  KVStore store("data/");
  load_sentence_embedding_mapping();
  store.load_embedding_from_disk("embedding_data/"); 
  store.load_embedding_from_disk("data/");


  bool pass = true;

  //int total = valid_sentences.size() ;
  //serial
  int total = 8192;
  // auto startTime = std::chrono::high_resolution_clock::now();
  // for (int i = 0; i < total; i++) {
  //   std::vector<std::pair<std::uint64_t, std::string>> result =
  //       store.search_knn(valid_sentences[i], 1);
  //   if (result.size() != 1) {
  //     std::cout << "Error: result.size() != 1" << std::endl;
  //     pass = false;
  //     continue;
  //   }
  //   if (result[0].second != valid_sentences[i]) {
  //     std::cout << "Error: value[" << i << "] is not correct" << std::endl;
  //     pass = false;
  //   }
  // }
  // auto endTime = std::chrono::high_resolution_clock::now();
  // auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
  // std::cout << "Serial Total time: " << totalDuration.count() << " milliseconds" << std::endl;

  //parallel
  auto startTime = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < total; i++) {
    std::vector<std::pair<std::uint64_t, std::string>> result =
        store.search_knn_parallel(valid_sentences[i], 1);
    if (result.size() != 1) {
      std::cout << "Error: result.size() != 1" << std::endl;
      pass = false;
      continue;
    }
    if (result[0].second != valid_sentences[i]) {
      std::cout << "Error: value[" << i << "] is not correct" << std::endl;
      pass = false;
    }
  }
  auto endTime = std::chrono::high_resolution_clock::now();
  auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
  std::cout << "Paralle Total time: " << totalDuration.count() << " milliseconds" << std::endl;




  if (pass) {
    std::cout << "Test passed" << std::endl;
  } else {
    std::cout << "Test failed" << std::endl;
  }

  return 0;
}