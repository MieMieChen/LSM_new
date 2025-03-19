#ifndef LSM_KV_SKIPLIST_H
#define LSM_KV_SKIPLIST_H

#include <cstdint>
#include <limits>
#include <list>
#include <string>
#include <vector>

enum TYPE {
    HEAD,
    NORMAL,
    TAIL
};

const int MAX_LEVEL = 18;
// slnode是skiplist里面的每个节点
class slnode {
public:
    uint64_t key;
    std::string val;
    TYPE type;
    std::vector<slnode *> nxt;

    slnode(uint64_t key, const std::string &val, TYPE type) {
        this->key  = key;
        this->val  = val;
        this->type = type;
        for (int i = 0; i < MAX_LEVEL; ++i)
            nxt.push_back(nullptr);
    }
};

class skiplist {
private:
    const uint64_t INF = std::numeric_limits<uint64_t>::max();
    double p; // p 表示增长概率
    uint64_t s     = 1;
    uint32_t bytes = 0x0; // bytes表示index + data区域的字节数，即跳表占用的内存大小。初始为0，随着数据插入会增加。
    int curMaxL    = 1; //表示当前跳表的最大层级，初始为1层。随着元素的插入和随机层级的生成，这个值可能会增加，但不会超过MAX_LEVEL(18)。
    slnode *head   = new slnode(0, "", HEAD);
    slnode *tail   = new slnode(INF, "", TAIL);

public:
    skiplist(double p) { // p 表示增长概率
        s       = 1;
        bytes   = 0x0;
        curMaxL = 1;
        this->p = p;
        for (int i = 0; i < MAX_LEVEL; ++i)
            head->nxt[i] = tail;
    }

    slnode *getFirst() {
        return head->nxt[0];
    }

    double my_rand();
    int randLevel();
    void insert(uint64_t key, const std::string &str);
    std::string search(uint64_t key);
    bool del(uint64_t key, uint32_t len);
    void scan(uint64_t key1, uint64_t key2, std::vector<std::pair<uint64_t, std::string>> &list);
    slnode *lowerBound(uint64_t key);
    void reset();
    uint32_t getBytes();
};

#endif // LSM_KV_SKIPLIST_H
