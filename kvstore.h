#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "sstablehead.h"

#include <map>
#include <set>

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

    int totalLevel = -1; // 层数
public:
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

    void compaction();

    void delsstable(std::string filename);  // 从缓存中删除filename.sst， 并物理删除
    void addsstable(sstable ss, int level); // 将ss加入缓存


    std::vector<KVT> mergeSort(std::vector<KVT> left, std::vector<KVT> right );
    std::string fetchString(std::string file, int startOffset, uint32_t len);
};

