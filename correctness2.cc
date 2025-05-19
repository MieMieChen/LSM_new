#include "test.h"

#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

static std::unordered_map<std::string, int> sentence2line;
static std::vector<std::string> embedding_lines;
static std::vector<std::string> valid_sentences;
static bool embedding_loaded = false;
static size_t embedding_dim = 0;

class CorrectnessTest : public Test {
private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t LARGE_TEST_MAX  = 1024 * 64;

    void insert_test(uint64_t max) {
        uint64_t i;
        for (i = 0; i < max; ++i) {
            store.put(i, embedding_lines[i]);
        }

        for (i = 0; i < max; ++i)
            EXPECT(valid_sentences[i], store.get(i));
        phase();
    }

    void delete_test(uint64_t max) {
        uint64_t i;
        //Test a single key
        // EXPECT(not_found, store.get(1));
        // store.put(1, "SE");
        // EXPECT("SE", store.get(1));
        // EXPECT(true, store.del(1));
        // EXPECT(not_found, store.get(1));
        // EXPECT(false, store.del(1));

        // phase();

        // Test multiple key-value pairs
        for (i = 0; i < max; ++i) {
            store.put(i, embedding_lines[i]);
            EXPECT(valid_sentences[i], store.get(i));
        }
        phase();

        // Test deletions
        for (i = 0; i < max; i += 2)
            EXPECT(true, store.del(i));

        for (i = 0; i < max; ++i) {
            switch (i & 3) {
            case 0: // 4k
                EXPECT(not_found, store.get(i));
                store.put(i, std::string(i + 1, 't'));
                break;
            case 1: // 4k+1
                EXPECT(std::string(i + 1, 's'), store.get(i));
                store.put(i, std::string(i + 1, 't'));
                break;
            case 2: // 4k+2
                EXPECT(not_found, store.get(i));
                break;
            case 3: // 4k+3
                EXPECT(std::string(i + 1, 's'), store.get(i));
                break;
            }
        }

        phase();

        report();
    }

    void regular_test(uint64_t max) {
        uint64_t i;

        // Test a single key
        // EXPECT(not_found, store.get(1));
        // // 用embedding文件查找替换put
        // store.put(1, "SE");
        // EXPECT("SE", store.get(1));
        // EXPECT(true, store.del(1));
        // EXPECT(not_found, store.get(1));
        // EXPECT(false, store.del(1));

        // phase();

        //Test multiple key-value pairs
        for (i = 0; i < max; ++i) { 
            // 用embedding文件查找替换put
            std::string embedString =  embedding_lines[i];
            store.put(i, embedString);
            EXPECT(valid_sentences[i], store.get(i));
        }
        phase();

        // Test after all insertions
        for (i = 0; i < max; ++i)
            EXPECT(valid_sentences[i], store.get(i));
        phase();

        // Test scan
        std::list<std::pair<uint64_t, std::string>> list_ans;
        std::list<std::pair<uint64_t, std::string>> list_stu;

        for (i = 0; i < max / 2; ++i) {
            list_ans.emplace_back(std::make_pair(i, embedding_lines[i]));
        }

        store.scan(0, max / 2 - 1, list_stu);
        EXPECT(list_ans.size(), list_stu.size());

        auto ap = list_ans.begin();
        auto sp = list_stu.begin();
        while (ap != list_ans.end()) {
            if (sp == list_stu.end()) {
                EXPECT((*ap).first, -1);
                EXPECT((*ap).second, not_found);
                ap++;
            } else {
                EXPECT((*ap).first, (*sp).first);
                EXPECT((*ap).second, (*sp).second);
                ap++;
                sp++;
            }
        }

        phase();

        // Test deletions
        for (i = 0; i < max; i += 2)
            EXPECT(true, store.del(i));

        for (i = 0; i < max; ++i)
            EXPECT((i & 1) ? valid_sentences[i] : not_found, store.get(i));

        for (i = 1; i < max; ++i)
            EXPECT(i & 1, store.del(i));

        phase();

        report();
    }
    void load_sentence_embedding_mapping() {
    if (embedding_loaded) return;
    std::ifstream fin_text("cleaned_text_100k.txt");
    std::ifstream fin_emb("embedding_100k.txt");
    std::string line;
    int idx = 0;
    // 读取文本，跳过空行，建立映射
    while (std::getline(fin_text, line)) {
        if (line.empty()) continue;
        valid_sentences.push_back(line);
    }
    // 读取embedding所有行
    while (std::getline(fin_emb, line)) {
        embedding_lines.push_back(line);
    }
    // 建立映射（只映射非空行和embedding）
    int min_size = std::min(valid_sentences.size(), embedding_lines.size());
    for (int i = 0; i < min_size; ++i) {
        sentence2line[valid_sentences[i]] = i;
    }
    // 统计维度
    if (!embedding_lines.empty()) {
        std::istringstream iss(embedding_lines[0]);
        float tmp;
        while (iss >> tmp) embedding_dim++;
    }
    embedding_loaded = true;
}

// 根据语句获取embedding向量
    std::vector<float> get_embedding_from_sentence(const std::string& val) {
        load_sentence_embedding_mapping();
        auto it = sentence2line.find(val);
        if (it == sentence2line.end()) {
            // 若找不到，返回全0向量或抛异常
            return std::vector<float>(embedding_dim, 0.0f);
        }
        int line_no = it->second;
        if (line_no >= embedding_lines.size()) {
            return std::vector<float>(embedding_dim, 0.0f);
        }
        std::istringstream iss(embedding_lines[line_no]);
        std::vector<float> emb;
        float v;
        while (iss >> v) emb.push_back(v);
        return emb;
    }




public:
    CorrectnessTest(const std::string &dir, bool v = true) : Test(dir, v) {}

    void start_test(void *args = NULL) override {
        load_sentence_embedding_mapping();
        std::cout << "KVStore Correctness Test" << std::endl;

        // store.reset();

        // std::cout << "[Simple Test]" << std::endl;
        // regular_test(SIMPLE_TEST_MAX);

        store.reset();

        std::cout << "[Large Test]" << std::endl;
        regular_test(50000);

        store.reset();
        std::cout << "[Insert Test]" << std::endl;
        insert_test(50000);

        store.reset();
        std::cout << "[delete test]" << std::endl;
        delete_test(50000);
    }
};

// 全局变量或静态变量，便于测试用例直接访问


int main(int argc, char *argv[]) {
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    CorrectnessTest test("./data", verbose);

    test.start_test();

    return 0;
}