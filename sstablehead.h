#pragma once

#ifndef LSM_KV_SSTABLEHEAD_H
#define LSM_KV_SSTABLEHEAD_H
#include "bloom.h"

#include <cstdint>
#include <vector>
#include <limits>

struct Index {
    uint64_t key;
    uint32_t offset;

    Index() {}

    Index(uint64_t key, uint32_t offset) {
        this->key    = key;
        this->offset = offset;
    }
// 使用index的大小来比较key的大小。
    bool operator<(const Index &b) const {
        return this->key < b.key;
    }
};

class sstablehead //包含了header bloom filter 还有index区
{
protected:
    std::string filename; // filename表示该sstable的名字，含路径前缀和后缀
    uint64_t time, cnt, minV, maxV; //sstable的创建时间，数据条数，最小key，最大key 
    uint32_t bytes;          // 理论上的sstable转换成文件的大小
    uint32_t curpos;         // 当前offset的位置 如果将所有数据连续存储，下一个值应该开始的位置
    uint32_t nameSuffix = 0; // 区分同一时间戳，不同文件的姓名后缀
    bloom filter;
    std::vector<Index> index; // index区的每一个元素都是index 包含key和offset

public:
    bool operator<(const sstablehead &other) const {
        if (time == other.time) //较新的数据（较大的时间戳）通常包含最新的键值对，因此先比较时间戳
            return minV < other.minV; //如果时间戳相同，则比较最小key
        return time < other.time; //否则，直接比较时间戳
    }

    sstablehead() {
        time   = 0;
        cnt    = 0;
        curpos = 0;
        minV   = std::numeric_limits<uint64_t>::max();
        maxV   = 0;
        bytes  = 10240 + 32;
    }

    void loadFileHead(const char *path);
    void reset();

    void setFilename(std::string filename) {
        this->filename = filename;
    }

    void setNamesuffix(uint32_t nameSuffix) {
        this->nameSuffix = nameSuffix;
    }

    void setTime(uint64_t time) {
        this->time = time;
    }

    void setCnt(uint64_t cnt) {
        this->cnt = cnt;
    }

    void setMinV(uint64_t minV) {
        this->minV = minV;
    }

    void setMaxV(uint64_t maxV) {
        this->maxV = maxV;
    }

    void setBytes(uint32_t bytes) {
        this->bytes = bytes;
    }

    void setFilter(bloom filter) {
        this->filter.setBitset(filter.getBitset());
    }

    void setIndex(std::vector<Index> index) {
        this->index = index;
    } // 使用深复制

    std::string getFilename() {
        return filename;
    }

    uint64_t getTime() const {
        return time;
    }

    uint64_t getCnt() const {
        return cnt;
    }

    uint64_t getMinV() const {
        return minV;
    }

    uint64_t getMaxV() const {
        return maxV;
    }

    uint64_t getKey(int p) {
        return index[p].key;
    }

    uint32_t getBytes() const {
        return bytes;
    }

    uint32_t getNameSuf() const {
        return nameSuffix;
    }

    uint32_t getOffset(int p) {
        return (p < 0) ? 0 : index[p].offset;
    }

    Index getIndexById(int p) {
        return index[p];
    }

    int searchOffset(uint64_t key, uint32_t &len);

    int search(uint64_t key);
    int lowerBound(uint64_t key); /*返回大于等于的第一个的下标 没有返回len + 1*/
    void showIndexs();
};

#endif // LSM_KV_SSTABLEHEAD_H
