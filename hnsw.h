#ifndef HNSW_H
#define HNSW_H
#include <cstdint>
#include <limits>
#include <list>
#include <string>
#include <vector>
#include <unordered_map>

// #define M 8
// #define M_max 16
// #define efConstruction 25
// #define m_L 9


struct Node {
    uint64_t key;  // 节点的键
    uint64_t id;   // 节点的ID
    std::vector<float> vector;  // 节点的向量  存向量还是embedding
    std::vector<std::vector<uint64_t>> layer_connections;  // 每层的连接关系
    bool is_deleted = false; // 节点是否被删除
    int max_level;  // 节点所在的最高层
    Node() = default;   
    Node(uint64_t k, uint32_t i, const std::vector<float>& v, int level) 
        : key(k), id(i), vector(v), max_level(level) {
        layer_connections.resize(level + 1);
    }
};

class HNSW {
    public:
        HNSW(uint64_t M,uint64_t M_max,uint64_t efConstruction,uint64_t m_L,uint64_t dim) {
            // 初始化至少有一层
            layers.resize(1);
            entry_point = -1; // 默认维度
            globalHeader.M = M;
            globalHeader.M_max = M_max; 
            globalHeader.efConstruction = efConstruction;
            globalHeader.m_L = m_L;
            globalHeader.max_level = 0;
            globalHeader.num_nodes = 0;
            globalHeader.dim = dim;
            next_node_id = 0;  // 初始化节点ID计数器
        }
        HNSW() = default;
        void insert(uint64_t key, const std::vector<float>& vector);
        uint64_t get_max_layer() const;
        uint64_t get_entry_point() const;
        std::vector<std::pair<std::uint64_t, std::string>> query(const std::vector<float>& query_vector, int k);
        std::vector<std::unordered_map<uint64_t, std::vector<uint64_t>>> layers;   
        struct HNSWGlobalHeader {
            uint32_t M;                // 参数
            uint32_t M_max;            // 参数
            uint32_t efConstruction;   // 参数
            uint32_t m_L;              // 参数
            uint32_t max_level;        // 全图最高层级
            uint32_t num_nodes;        // 节点总数
            uint32_t dim;              // 向量维度
        } globalHeader;       
        std::unordered_map<uint64_t, uint64_t> key_to_id;  // 键值到节点ID的映射
        std::unordered_map<uint64_t, uint64_t> id_to_key;  // 节点ID到键值的映射
        std::vector<Node> nodes;
        void set_entry_point(uint64_t id);
    private:
        // std::unordered_map<uint64_t, std::vector<float>> vectors; // 存储每个节点的向量
        uint64_t next_node_id=0; // 下一个可用的节点ID
        int rand_level();
        int entry_point = -1;
        float cosine_similarity(std::vector<float> a,std::vector<float> b);
        float dot_product(std::vector<float>a,std::vector<float>b);
        float vector_norm(std::vector<float>a);
};

#endif