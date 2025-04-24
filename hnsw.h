#ifndef HNSW_H
#define HNSW_H
#include <cstdint>
#include <limits>
#include <list>
#include <string>
#include <vector>
#include <unordered_map>

#define M 8
#define M_max 16
#define efConstruction 25
#define m_L 9




class HNSW {
    public:
        HNSW() {
            // 初始化至少有一层
            layers.resize(1);
            cur_layer = 0;
            entry_point = -1;
        }
        void insert(uint64_t key, const std::vector<float>& vector);
        uint64_t get_max_layer() const;
        uint64_t get_entry_point() const;
        std::vector<std::pair<std::uint64_t, std::string>> query(const std::vector<float>& query_vector, int k);
        std::vector<std::unordered_map<uint64_t, std::vector<uint64_t>>> layers;   
    private:
        std::unordered_map<uint64_t, std::vector<float>> vectors; // 存储每个节点的向量
        uint64_t cur_layer = 0;
        int rand_level();
        int entry_point = -1;
        float cosine_similarity(std::vector<float> a,std::vector<float> b);
        float dot_product(std::vector<float>a,std::vector<float>b);
        float vector_norm(std::vector<float>a);
};

#endif