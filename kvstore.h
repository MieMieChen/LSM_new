#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "sstablehead.h"
#include "embedding.h"
#include "hnsw.h"
#include "embedding.h"
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

struct KVT
{
    uint64_t key;
    std::string value;
    uint64_t time;
    uint64_t level;
    KVT(uint64_t key, std::string value, uint64_t time,uint64_t level)
    {
        this->key = key;
        this->value = value;
        this->time = time;
        this->level = level;
    }
};

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    skiplist *s = new skiplist(0.5); // memtable
    // std::vector<sstablehead> sstableIndex;  // sstable的表头缓存
    std::vector<sstablehead> sstableIndex[15]; // the sshead for each level 这个是一个二维的。
    std::unordered_map<std::uint64_t, std::vector<float>> Cache;
    std::unordered_set<uint64_t> dirty_keys;  // 需要删除的key
    std::vector<std::vector<float>> deleted_nodes;
    int totalLevel = -1; // 层数
    uint64_t dim = 768;
public:
    KVStore(const std::string &dir);
    HNSW hnsw_index; // M, M_max, efConstruction, m_L, dim

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;
 
    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

    void compaction();

    void delsstable(std::string filename);  // 从缓存中删除filename.sst， 并物理删除
    void addsstable(sstable ss, int level); // 将ss加入缓存
    std::vector<std::pair<std::uint64_t, std::string>> search_knn(std::string query, int k);
    float cosine_similarity(std::vector<float> a,std::vector<float> b);
    float dot_product(std::vector<float>a,std::vector<float>b);
    float vector_norm(std::vector<float>a);
    std::vector<KVT> mergeSort(std::vector<KVT> left, std::vector<KVT> right );
    std::string fetchString(std::string file, int startOffset, uint32_t len);
    std::vector<std::pair<std::uint64_t, std::string>>search_knn_hnsw(std::string query, int k);
    std::vector<std::pair<std::uint64_t, std::string>> query_knn(std::vector<float> embStr,int k);
    void save_embedding_to_disk(const std::string &data_root);
    void load_embedding_from_disk(const std::string &data_root);
    void save_hnsw_index_to_disk(const std::string &hnsw_data_root);
    void load_hnsw_index_from_disk(const std::string &hnsw_data_root);
};


