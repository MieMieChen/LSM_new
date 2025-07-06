#include "kvstore.h"

#include "skiplist.h"
#include "sstable.h"
#include "utils.h"
#include "hnsw.h"
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <cmath>
#include <unordered_set>
#include <unordered_map>
#include <filesystem>
#include "shared_data.h"

namespace fs = std::filesystem;
static const std::string DEL = "~DELETED~";
const uint32_t MAXSIZE       = 2 * 1024 * 1024;



struct poi {
    int sstableId; // vector中第几个sstable
    int pos;       // 该sstable的第几个key-offset
    uint64_t time;
    Index index;
    int sstableHeadI;
    int sstableHeadJ;
};

struct cmpPoi {
    bool operator()(const poi &a, const poi &b) {
        if (a.index.key == b.index.key)
            return a.time < b.time;
        return a.index.key > b.index.key;
    }
}; //这是一个比较器


// 在 C++ 优先级队列中：
// 返回 true 表示 a 的优先级低于 b
// 返回 false 表示 a 的优先级高于或等于 b
//  time 越大或者 key 越小的先出队



KVStore::KVStore(const std::string &dir) :hnsw_index(8,16,25,9,dim),
    KVStoreAPI(dir) // read from sstables
{
    
    for (totalLevel = 0;; ++totalLevel) {
        std::string path = dir + "/level-" + std::to_string(totalLevel) + "/";
        std::vector<std::string> files;
        if (!utils::dirExists(path)) {
            totalLevel--;
            break; // stop read
        }
        int nums = utils::scanDir(path, files);
        sstablehead cur;
        for (int i = 0; i < nums; ++i) {       // 读每一个文件头
            std::string url = path + files[i]; // url, 每一个文件名
            cur.loadFileHead(url.data());
            sstableIndex[totalLevel].push_back(cur);
            TIME = std::max(TIME, cur.getTime()); // 更新时间戳
        }
    }
    //需要修改这里的dir。
    // 冷启动（无数据）：使用默认参数创建新 HNSW 索引，并保存初始结构到磁盘。
    // 热启动（有数据）：从磁盘加载现有索引。
    // load_embedding_from_disk(dir);
    // load_hnsw_index_from_disk("hnsw_data_root/");
}

// 程序退出时：
// 1. 将内存数据写入 level-0
// 2. 执行 compaction 合并文件
// 3. 优化存储结构

KVStore::~KVStore()
{
    sstable ss(s);
    if (!ss.getCnt())
        return; // empty sstable
    std::string path = std::string("./data/level-0/");
    if (!utils::dirExists(path)) {
        utils::_mkdir(path.data());
        totalLevel = 0;
    }
    std::string filename = ss.getFilename();
    std::cout << "Writing to file: " << filename << std::endl;
    ss.putFile(ss.getFilename().data());
    //cache落入磁盘
    save_embedding_to_disk("./data/");
    sstableIndex[0].push_back(ss.getHead());
    compaction(); 
    // save_hnsw_index_to_disk("./hnsw_data_root/");

}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
// void KVStore::put(uint64_t key, const std::string &val) {
//     std::vector<float> embeddingString = embedding_single(val);
//     Cache[key]= embeddingString;
//     dirty_keys.insert(key);
//     hnsw_index.insert(key, embeddingString);

//     uint32_t nxtsize = s->getBytes();
//     std::string res  = s->search(key);
//     if (!res.length()) { // new add
//         nxtsize += 12 + val.length();
//     } else
//         nxtsize = nxtsize - res.length() + val.length(); // change string
//     if (nxtsize + 10240 + 32 <= MAXSIZE)
//        {
//         s->insert(key, val);
//        }  // 小于等于（不超过） 2MB
//     else {
//         sstable ss(s);
//         s->reset();
//         std::string url  = ss.getFilename();
//         std::string path = "./data/level-0";
//         if (!utils::dirExists(path)) {
//             utils::mkdir(path.data());
//             totalLevel = 0;
//         }
//         addsstable(ss, 0);      // 加入缓存
//         ss.putFile(url.data()); // 加入磁盘
//         //这里可以写一下cache
//         compaction();
//         s->insert(key, val);
//     }

// }
//现在要绕过embedding 直接根据val找到被embed好的向量
void KVStore::put(uint64_t key, const std::string &val) {
    // std::vector<float> embeddingString = embedding_single(val);
    std::vector<float> embeddingString = sentence2line[val];
    Cache[key]= embeddingString;
    // dirty_keys.insert(key);
    // hnsw_index.insert(key, embeddingString);

    uint32_t nxtsize = s->getBytes();
    std::string res  = s->search(key);
    if (!res.length()) { // new add
        nxtsize += 12 + val.length();
    } else
        nxtsize = nxtsize - res.length() + val.length(); // change string
    if (nxtsize + 10240 + 32 <= MAXSIZE)
       {
        s->insert(key, val);
       }  // 小于等于（不超过） 2MB
    else {
        sstable ss(s);
        s->reset();
        std::string url  = ss.getFilename();
        std::string path = "./data/level-0";
        if (!utils::dirExists(path)) {
            utils::mkdir(path.data());
            totalLevel = 0;
        }
        addsstable(ss, 0);      // 加入缓存
        ss.putFile(url.data()); // 加入磁盘
        //这里可以写一下cache
        compaction();
        s->insert(key, val);
    }

}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) //
{
    uint64_t time = 0;
    int goalOffset;
    uint32_t goalLen;
    std::string goalUrl;
    std::string res = s->search(key);
    if (res.length()) { // 在memtable中找到, 或者是deleted，说明最近被删除过，
                        // 不用查sstable
        if (res == DEL)
            return "";
        return res;
    }
    for (int level = 0; level <= totalLevel; ++level) {
        for (sstablehead it : sstableIndex[level]) {
            if (key < it.getMinV() || key > it.getMaxV())
                continue;
            uint32_t len; //当前 key 对应数据的长度
            int offset = it.searchOffset(key, len); //前一个元素的 offset 就是当前元素数据的起始位置
            if (offset == -1) {
                if (!level)
                    continue;
                else
                    break;
            }
            // sstable ss;
            // ss.loadFile(it.getFilename().data());
            if (it.getTime() > time) { // find the latest head
                time       = it.getTime();
                goalUrl    = it.getFilename();
                goalOffset = offset + 32 + 10240 + 12 * it.getCnt();
                goalLen    = len;
            }
        }
        if (time)
            break; // only a test for found
    }
    if (!goalUrl.length())
        return ""; // not found a sstable
    res = fetchString(goalUrl, goalOffset, goalLen);
    if (res == DEL)
        return "";
    return res;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    std::vector<float> embeddingString;
    uint64_t id;
    for(auto &it:hnsw_index.nodes) {
        if(it.key == key) {
            it.is_deleted = true; // 标记为删除
            id = it.id;
            embeddingString = it.vector; // 获取对应的向量
        }
    }
    deleted_nodes.push_back(DeletedNode(id, embeddingString));
    //是在落入磁盘的时候判断内存中是否还有这个key对应的向量 然后来判断是修改了还是删除，
    Cache.erase(key);  // 从内存移除
    dirty_keys.insert(key);  // 标记为删除
    std::string res = get(key);
    if (!res.length())
        return false; // not exist
    put(key, DEL);    // put a del marker
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    s->reset(); // 先清空memtable
    std::vector<std::string> files;
    for (int level = 0; level <= totalLevel; ++level) { // 依层清空每一层的sstables
        std::string path = std::string("./data/level-") + std::to_string(level);
        int size         = utils::scanDir(path, files);
        for (int i = 0; i < size; ++i) {
            std::string file = path + "/" + files[i];
            int ret = utils::rmfile(file.data());
        }
        utils::rmdir(path.data());
        sstableIndex[level].clear();
    }
    TIME = 0;
    totalLevel = -1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */

struct myPair {
    uint64_t key, time;
    int id, index;
    std::string filename;

    myPair(uint64_t key, uint64_t time, int index, int id,
           std::string file) { // construct function
        this->time     = time;
        this->key      = key;
        this->id       = id;  //SSTable在层级中的位置
        this->index    = index; // 在SSTable中的位置
        this->filename = file;
    }
};

struct cmp {
    bool operator()(myPair &a, myPair &b) {
        if (a.key == b.key)
            return a.time < b.time;
        return a.key > b.key;
    }
};

/*key and data*/
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    std::vector<std::pair<uint64_t, std::string>> mem;
    // std::set<myPair> heap; // 维护一个指针最小堆
    std::priority_queue<myPair, std::vector<myPair>, cmp> heap; //队列中存储的元素类型，优先队列使用的底层容器类型，比较器类型
    // std::vector<sstable> ssts;
    std::vector<sstablehead> sshs;
    s->scan(key1, key2, mem);   // add in mem
    std::vector<int> head, end; // [head, end)
    int cnt = 0;
    if (mem.size())
        heap.push(myPair(mem[0].first, INF, 0, -1, "qwq")); //TA很可爱哈，INF: 表示无穷大的时间戳，确保内存中的数据优先级最高，0: 在内存中的索引位置 -1: 特殊标识，表示这是内存中的数据（不是 SSTable） "qwq": 一个占位符文件名，因为内存数据没有实际的文件名   
    for (int level = 0; level <= totalLevel; ++level) {
        for (sstablehead it : sstableIndex[level]) {
            if (key1 > it.getMaxV() || key2 < it.getMinV())
                continue; // 无交集
                
            int hIndex = it.lowerBound(key1);
            int tIndex = it.lowerBound(key2);
            if (hIndex < it.getCnt()) { // 此sstable可用
                // sstable ss; // 读sstable
                std::string url = it.getFilename();
                // ss.loadFile(url.data());

                heap.push(myPair(it.getKey(hIndex), it.getTime(), hIndex, cnt++, url));
                head.push_back(hIndex);
                if (it.search(key2) == tIndex)
                    tIndex++; // tIndex为第一个不可的
                end.push_back(tIndex);
                // ssts.push_back(ss); // 加入ss
                sshs.push_back(it);
            }
        }
    }
    uint64_t lastKey = INF; // only choose the latest key
    while (!heap.empty()) { // 维护堆
        myPair cur = heap.top();
        heap.pop();
        if (cur.id >= 0) { // from sst
            if (cur.key != lastKey) {
                lastKey         = cur.key;
                uint32_t start  = sshs[cur.id].getOffset(cur.index - 1);
                uint32_t len    = sshs[cur.id].getOffset(cur.index) - start;
                uint32_t scnt   = sshs[cur.id].getCnt();
                std::string res = fetchString(cur.filename, 10240 + 32 + scnt * 12 + start, len);
                if (res.length() && res != DEL)
                    list.emplace_back(cur.key, res);
            }
            if (cur.index + 1 < end[cur.id]) { // add next one to heap
                heap.push(myPair(sshs[cur.id].getKey(cur.index + 1), cur.time, cur.index + 1, cur.id, cur.filename));
            }
        } else { // from mem
            if (cur.key != lastKey) {
                lastKey         = cur.key;
                std::string res = mem[cur.index].second;
                if (res.length() && res != DEL)
                    list.emplace_back(cur.key, mem[cur.index].second);
            }
            if (cur.index < mem.size() - 1) {
                heap.push(myPair(mem[cur.index + 1].first, cur.time, cur.index + 1, -1, cur.filename));
            }
        }
    }
}

bool isPathOfLevel(const std::string& path, int level) {
    // 构造要查找的层级字符串
    std::string levelStr = "/level-" + std::to_string(level) + "/";
    
    // 判断路径中是否包含该层级字符串
    return path.find(levelStr) != std::string::npos;
}

// struct waitStruct
// {
//     sstablehead tmp;
//     int sstableHeadI,sstableHeadJ;
//     waitStruct(sstablehead tmp,int i,int j)
//     {
//         this->tmp = tmp;
//         this->sstableHeadI = i;
//         this->sstableHeadJ = j;
//     }
// };

// struct SSTableWithPos {
//     sstablehead table;  // SSTable数据
//     int i;          // 在sstableIndex中的层级i
//     int j;          // 在sstableIndex[level]中的位置j
// };
struct waitTablehead
{
    uint64_t level;
    sstablehead tablehead;
    waitTablehead(sstablehead tablehead,uint64_t level)
    {
        this->level = level;
        this->tablehead = tablehead;
    }
};

std::vector<KVT>  KVStore::mergeSort(std::vector<KVT> left, std::vector<KVT> right )
{
    std::vector<KVT> result;
    int leftIndex = 0, rightIndex = 0;

    while (leftIndex < left.size() && rightIndex < right.size()) {
        if (left[leftIndex].key < right[rightIndex].key) {
            result.push_back(left[leftIndex]);
            leftIndex++;
        } else if(left[leftIndex].key > right[rightIndex].key) {
            result.push_back(right[rightIndex]);
            rightIndex++;
        }
        else
        {
            if(left[leftIndex].level<right[rightIndex].level)
            {
                result.push_back(left[leftIndex]);
            }
            else if(left[leftIndex].level>right[rightIndex].level)
            {
                result.push_back(right[rightIndex]);
            }
            else
            {
                if(left[leftIndex].time > right[rightIndex].time) //说明更新
                    result.push_back(left[leftIndex]);
                else    
                    result.push_back(right[rightIndex]);
            }
            rightIndex++;
            leftIndex++;
        }
    }

    //把剩余的都塞进去
    while (leftIndex < left.size()) {
        result.push_back(left[leftIndex]);
        leftIndex++;
    }

    while (rightIndex < right.size()) {
        result.push_back(right[rightIndex]);
        rightIndex++;
    }

    return result;

}

// std::vector<KVT> KVStore::parallelMergeSort(std::vector<KVT> left, std::vector<KVT> right) {
//     const size_t total_size = left.size() + right.size();
//     std::vector<KVT> result(total_size);
    
//     // 如果数据量太小，直接串行归并
//     if (total_size < 1000) {
//         return mergeSort(left, right);
//     }
    
//     const int num_threads = std::thread::hardware_concurrency();
//     std::vector<std::future<void>> futures;
    
//     // 按照left数组分块
//     size_t left_block_size = left.size() / num_threads;
    
//     for (int i = 0; i < num_threads; i++) {
//         size_t left_start = i * left_block_size;
//         size_t left_end = (i == num_threads - 1) ? left.size() : (i + 1) * left_block_size;
        
//         // 找到right中对应的范围
//         uint64_t start_key = left[left_start].key;
//         uint64_t end_key = (left_end < left.size()) ? left[left_end].key : UINT64_MAX;
        
//         futures.push_back(std::async(std::launch::async, [&, left_start, left_end, start_key, end_key]() {
//             // 在right中找到对应范围
//             auto right_start = std::lower_bound(right.begin(), right.end(), start_key,
//                 [](const KVT& a, uint64_t key) { return a.key < key; });
//             auto right_end = std::upper_bound(right.begin(), right.end(), end_key,
//                 [](uint64_t key, const KVT& a) { return key < a.key; });
            
//             // 归并这部分数据
//             size_t pos = left_start;
//             auto lit = left.begin() + left_start;
//             auto rit = right_start;
            
//             while (lit < left.begin() + left_end && rit < right_end) {
//                 if (lit->key <= rit->key) {
//                     result[pos++] = *lit++;
//                 } else {
//                     result[pos++] = *rit++;
//                 }
//             }
            
//             while (lit < left.begin() + left_end) {
//                 result[pos++] = *lit++;
//             }
            
//             while (rit < right_end) {
//                 result[pos++] = *rit++;
//             }
//         }));
//     }
    
//     // 等待所有任务完成
//     for (auto& future : futures) {
//         future.wait();
//     }
    
//     return result;
// }

void KVStore::compaction() {
    int curLevel = 0;
    uint64_t minVtmp = UINT64_MAX, maxVtmp = 0;
    std::vector<waitTablehead> waitlist;
    std::priority_queue<poi, std::vector<poi>, cmpPoi> pq;
    std::unordered_set<uint64_t> processedKeys;
   //  std::unordered_map<uint64_t, uint64_t> keyMaxTime;
    int j = 0;
    int sizeCur, sizeNxt;
    bool updateLevel = false;
    sstable newTable;
    std::string newPath;
    
    for(; curLevel <= totalLevel; curLevel++) {
        updateLevel = false;
        int num = 0;
        std::vector<std::string> filesCur,filesNxt;
        sizeCur = utils::scanDir("./data/level-" + std::to_string(curLevel),filesCur);
        sizeNxt = (curLevel + 1 <= totalLevel) ? utils::scanDir("./data/level-" + std::to_string(curLevel+1), filesNxt) : 0;
        if(sizeCur > pow(2, curLevel+1)) {
            // Prepare for next level
            if(sizeNxt == 0) {
                updateLevel = true;
                totalLevel++;
            }            
            waitlist.clear();

            minVtmp = UINT64_MAX;
            maxVtmp = 0;
            if(curLevel == 0) {
                //Level 0: take all SSTables
                j = 0;
                for(; j < sizeCur; j++) {
                if(minVtmp > sstableIndex[curLevel][j].getMinV())
                    minVtmp = sstableIndex[curLevel][j].getMinV();
                if(maxVtmp < sstableIndex[curLevel][j].getMaxV())
                    maxVtmp = sstableIndex[curLevel][j].getMaxV();
                waitlist.push_back(waitTablehead(sstableIndex[curLevel][j],curLevel));
            }
            } else {
                std::vector<waitTablehead> candidates;
                for (int j = 0; j < sizeCur; j++) {
                    candidates.push_back({waitTablehead(
                        sstableIndex[curLevel][j],curLevel)
                    });
                }
                
                std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) {
                    if (a.tablehead.getTime() != b.tablehead.getTime()) {
                        return a.tablehead.getTime() < b.tablehead.getTime();
                    }
                    return a.tablehead.getMinV() < b.tablehead.getMinV();
                });
            //把time比较小（老东西），或者数值比较小的合并到后面去
                int tablesToSelect = sizeCur - pow(2, curLevel+1);
                for(j = 0; j < tablesToSelect && j < candidates.size(); j++)
{
                    if(minVtmp > candidates[j].tablehead.getMinV())
                        minVtmp = candidates[j].tablehead.getMinV();
                    if(maxVtmp < candidates[j].tablehead.getMaxV())
                        maxVtmp = candidates[j].tablehead.getMaxV();
                    waitlist.push_back(candidates[j]);
                }
            }
            
            // Add overlapping SSTables from next level
            if(!updateLevel && (curLevel + 1 <= totalLevel)) {
                for(int j = 0; j < sizeNxt; j++) {
                    if(!(sstableIndex[curLevel+1][j].getMaxV() < minVtmp || 
                         sstableIndex[curLevel+1][j].getMinV() > maxVtmp)) {
                        waitlist.push_back(waitTablehead(sstableIndex[curLevel+1][j],curLevel+1));
                    }
                }
            }
            
            // Find the maximum timestamp for each key
            int sizeWait = waitlist.size();
            sstable sstables[sizeWait];
            sstablehead sstableheads[sizeWait];
            std::vector<KVT>  kvs;
            std::vector<KVT>  mergedKVs;
            for(int i = 0;i<sizeWait;i++) {
                sstables[i].loadFile(waitlist[i].tablehead.getFilename().data());
                sstableheads[i] = sstables[i].getHead();
                kvs.clear();
                for(int j = 0;j<sstableheads[i].getCnt();j++) {
                    kvs.push_back(KVT(sstableheads[i].getKey(j), sstables[i].getData(j),sstableheads[i].getTime(),waitlist[i].level));
                }
                mergedKVs = mergeSort(mergedKVs,kvs);
            }
             //mergedKVs 现在是一个完全全新的要被加入到sstable的数据，然后顺序就是越早出队的越优先
            
             newPath = "./data/level-" + std::to_string(curLevel + 1) + "/"; //往
             if (!utils::dirExists(newPath)) {
                 utils::mkdir(newPath.data());
             }
             
            for(int i = 0;i<mergedKVs.size();i++)
            {
                std::string value = mergedKVs[i].value;
                uint64_t key =mergedKVs[i].key;
                uint32_t nxtBytes = newTable.getHead().getBytes() + 12 + value.length() ;
                if(nxtBytes <= MAXSIZE) {
                    newTable.insert(key, value);
                } else {
                    // Flush current SSTable and create a new one
                    std::string filename = newPath + std::to_string(++TIME) + ".sst";
                    newTable.setFilename(filename);
                    newTable.setTime(TIME);
                    newTable.putFile(newTable.getFilename().data());
                    addsstable(newTable, curLevel+1);
                    newTable.reset();
                    newTable.insert(key, value);
                }
            }
            // Flush the final SSTable if it has entries
            if(newTable.getCnt() > 0) {
                std::string filename = newPath + std::to_string(++TIME) + ".sst";
                newTable.setFilename(filename);
                newTable.setTime(TIME);
                newTable.putFile(newTable.getFilename().data());
                addsstable(newTable, curLevel+1);
                newTable.reset();
            }
            
            // Delete processed SSTables
            for(auto &it : waitlist) {
                delsstable(it.tablehead.getFilename());
            }
            waitlist.clear();
        }
    }
}
void KVStore::delsstable(std::string filename) {
    for (int level = 0; level <= totalLevel; ++level) {
        int size = sstableIndex[level].size(), flag = 0;
        for (int i = 0; i < size; ++i) {
            if (sstableIndex[level][i].getFilename() == filename) {
                sstableIndex[level].erase(sstableIndex[level].begin() + i);
                flag = 1;
                break;
            }
        }
        if (flag)
            break;
    }
    int flag = utils::rmfile(filename.data());
    if (flag != 0) {
        std::cout << "delete fail!" << std::endl;
        std::cout << "filename" <<filename<<std::endl;
        std::cout << strerror(errno) << std::endl;
    }
}

void KVStore::addsstable(sstable ss, int level) {
    sstableIndex[level].push_back(ss.getHead());
}

char strBuf[2097152];

/**
 * @brief Fetches a substring from a file starting at a given offset.
 *
 * This function opens a file in binary read mode, seeks to the specified start offset,
 * reads a specified number of bytes into a buffer, and returns the buffer as a string.
 *
 * @param file The path to the file from which to read the substring.
 * @param startOffset The offset in the file from which to start reading. 
 * @param len The number of bytes to read from the file.
 * @return A string containing the read bytes.
 */
std::string KVStore::fetchString(std::string file, int startOffset, uint32_t len) {
    // TODO here
    memset(strBuf, 0, sizeof(strBuf));
    FILE *fp = fopen(file.c_str(), "rb");
    if (fp == NULL) {
        std::cerr << "Failed to open file: " << file << std::endl;
        return "";
    }   
    fseek(fp, startOffset, SEEK_SET);
    size_t bytesRead = fread(strBuf, 1, len, fp);
    strBuf[len] = '\0';
    if (bytesRead != len) {
        // 处理错误：未能读取所有请求的字节
        if (feof(fp)) {
            // 文件尾
            std::cerr << "Reached end of file unexpectedly" << std::endl;
        } else if (ferror(fp)) {
            // 读取错误
            std::cerr << "Error reading file: " << strerror(errno) << std::endl;
        }
    }
    fclose(fp);
    return std::string(strBuf, len);
}
float  KVStore::cosine_similarity(std::vector<float> a,std::vector<float> b)
{
    float result = 0.0f;
    result = dot_product(a,b)/(vector_norm(a)*vector_norm(b));
    return result;
}
float KVStore::dot_product(std::vector<float>a,std::vector<float>b)
{
    float result = 0.0f;
    for(size_t i = 0;i<a.size();i++)
    {
        result +=a[i]*b[i];
    }
    return result;
}
float KVStore::vector_norm(std::vector<float>a)
{
    float sum = 0.0f;
    for(float x:a)
    {
        sum+=x*x;
    }
    return std::sqrt(sum);
}
std::vector<std::pair<std::uint64_t, std::string>> KVStore::query_knn(std::vector<float> embStr,int k)
{
    std::vector<std::pair<std::uint64_t, std::string>> result;
    auto cmp = [](const auto& a, const auto& b) { return a.first > b.first; };
    using SimilarityPair = std::pair<float, std::pair<uint64_t, std::string>>;
    std::priority_queue<SimilarityPair, std::vector<SimilarityPair>, decltype(cmp)> top_k(cmp);

    for (const auto& [key, embedding] : Cache) {
        
        float sim = cosine_similarity(embedding, embStr);
        top_k.push({sim, {key, get(key)}});
        if (top_k.size() > k) {
            top_k.pop(); 
        }
    }

   
    while (!top_k.empty()) {
        result.push_back(top_k.top().second);
        top_k.pop();
    }
    std::reverse(result.begin(), result.end()); // 现在 result[0] 是最相似的
    
    return result;
}


std::vector<std::pair<std::uint64_t, std::string>> KVStore::search_knn(std::string query, int k) {
    // std::vector<float> embStr = embedding_single(query);
    std::vector<float> embStr = sentence2line[query];
    std::vector<std::pair<std::uint64_t, std::string>> result = query_knn(embStr,k);
    return result;
}
std::vector<std::pair<std::uint64_t, std::string>> KVStore::search_knn_parallel(std::string query, int k) {
    // std::vector<float> embStr = embedding_single(query);
    std::vector<float> embStr = sentence2line[query];
    std::vector<std::pair<std::uint64_t, std::string>> result = query_knn_parallel(embStr,k);
    return result;
}


std::vector<std::pair<std::uint64_t, std::string>> KVStore::search_knn_hnsw(std::string query, int k)
{
    std::vector<float> embStr = embedding_single(query);
    std::vector<std::pair<std::uint64_t, std::string>> result;
    
    result = hnsw_index.query(embStr, k);
    for(int i = 0; i < result.size(); i++)
    {
        std::vector<float> emb = hnsw_index.nodes[result[i].first].vector;
        for(auto it:Cache) {
            if(it.second == emb) {
                result[i].second = get(it.first);
            }
            if(result[i].second == DEL)
               result[i].second = "";
        }
    }
    
    return result;

}
//注意 这个传入的参数是什么！！   
void KVStore::load_embedding_from_disk(const std::string &data_root)
{
    Cache.clear();
    if (!utils::dirExists(data_root)) {
        return;
    }
    std::string file_path = data_root + "/embedding.bin";
    FILE* file = fopen((file_path).c_str(), "rb");
    if(file==NULL)
    {
        printf("cannot open a file 1\n");
        return;
    }
    fread(&dim,sizeof(std::uint64_t),1,file);
    uint64_t blocksize = 8 + 4*dim;
    fseek(file, 0, SEEK_END);
    uint64_t file_size = ftell(file);
    uint64_t blockNum =(file_size-8)/blocksize;
    for(int i = blockNum-1;i>=0;i--)
    {
        size_t block_start = i*blocksize+8;
        fseek(file, block_start, SEEK_SET);
        uint64_t key;
        fread(&key,sizeof(std::uint64_t),1,file);
        std::vector<float> embedding(dim);
        fread(embedding.data(),sizeof(float),dim,file);
        bool is_deleted = true;
        for (float val : embedding) {
            if (val != std::numeric_limits<float>::max()) {
                is_deleted = false;
                break;
            }
        }
        if(i>127)
        {
            int a = 1;
        }
        if(!is_deleted&&!Cache.count(key))
        { 
            Cache[key] = embedding;
        }
        else if(is_deleted&&Cache.count(key))
        {
            Cache.erase(key);
        } 
    }
    fclose(file);
}


void KVStore::save_hnsw_index_to_disk(const std::string &hnsw_data_root)
{
    if (!utils::dirExists(hnsw_data_root)) {
        utils::mkdir(hnsw_data_root.data());
    }
    std::string file_path1 = hnsw_data_root + "/global_header.bin";
    std::string file_path2 = hnsw_data_root + "/deleted_notes.bin";
    FILE* file1 = fopen(file_path1.c_str(), "wb");
    FILE* file2 = fopen(file_path2.c_str(), "wb");
    if(file1==NULL||file2==NULL)
    {
        printf("cannot open a file 2\n");
        return;
    }

    uint64_t deleted_size = deleted_nodes.size();
    for(int i = 0;i<deleted_size;i++)
    {
        fwrite(&deleted_nodes[i].id,sizeof(uint64_t),1,file2);
        fwrite(deleted_nodes[i].vector.data(),sizeof(float),dim,file2);
    }
    fclose(file2);

    if (!utils::dirExists(hnsw_data_root + "/nodes")) {
        utils::mkdir((hnsw_data_root + "/nodes").data());
    }
    hnsw_index.globalHeader.num_nodes = 0;
    //id是为了让其看起来更加正解
    for(auto it : hnsw_index.nodes)
    {
        hnsw_index.globalHeader.num_nodes++;
        std::string file_root_path = hnsw_data_root + "/nodes/" + std::to_string(it.id) + "/";
        if (!utils::dirExists(file_root_path)) {
            utils::mkdir((file_root_path).c_str());
        }
        uint32_t max_level = it.max_level;
        uint64_t key_of_embedding_vector = it.key;
        std::string header_path = file_root_path + "header.bin";
        FILE* file = fopen(header_path.c_str(), "wb");
        if(file==NULL)
        {
            printf("cannot open a file 3\n");
            return;
        }
        fwrite(&max_level,sizeof(uint32_t),1,file);
        fwrite(&key_of_embedding_vector,sizeof(uint64_t),1,file);
        fclose(file);
        std::string edges_root_path = file_root_path + "edges/";
        if (!utils::dirExists(edges_root_path)) {
            utils::mkdir((edges_root_path).c_str());
        }
        for(int i = it.layer_connections.size()-1;i>=0;i--)
        {
            std::string edges_path = edges_root_path + std::to_string(i) + ".bin";
            FILE* file = fopen(edges_path.c_str(), "wb");
            if(file==NULL)
            {
                printf("cannot open a file 4\n");
                return;
            }
            uint32_t num_edges = it.layer_connections[i].size();
            fwrite(&num_edges,sizeof(uint32_t),1,file);
            for(auto it2 : it.layer_connections[i])
            {
                fwrite(&it2,sizeof(uint64_t),1,file); //我存的是key而不是id
            }
            fclose(file);
        }
    }
    fwrite(&hnsw_index.globalHeader.M,sizeof(uint32_t),1,file1);
    fwrite(&hnsw_index.globalHeader.M_max,sizeof(uint32_t),1,file1);
    fwrite(&hnsw_index.globalHeader.efConstruction,sizeof(uint32_t),1,file1);
    fwrite(&hnsw_index.globalHeader.m_L,sizeof(uint32_t),1,file1);
    fwrite(&hnsw_index.globalHeader.max_level,sizeof(uint64_t),1,file1);
    fwrite(&hnsw_index.globalHeader.num_nodes,sizeof(uint64_t),1,file1);
    fwrite(&hnsw_index.globalHeader.dim,sizeof(uint64_t),1,file1);
    fclose(file1);
    std::cout << "save global header successfully!" << std::endl;
}
void KVStore::load_hnsw_index_from_disk(const std::string &hnsw_data_root)
{
    hnsw_index.nodes.clear();
    hnsw_index.layers.clear();
    if (!utils::dirExists(hnsw_data_root)) {
        return;
    }
    std::string file_path1 = hnsw_data_root + "global_header.bin";
    std::string file_path2 = hnsw_data_root + "deleted_notes.bin";
    FILE* file1 = fopen(file_path1.c_str(), "rb");
    FILE* file2 = fopen(file_path2.c_str(), "rb");
    if(file1==NULL||file2==NULL)
    {
        printf("cannot open a file 5\n");
        return;
    }
    fread(&hnsw_index.globalHeader.M,sizeof(uint32_t),1,file1);
    fread(&hnsw_index.globalHeader.M_max,sizeof(uint32_t),1,file1);
    fread(&hnsw_index.globalHeader.efConstruction,sizeof(uint32_t),1,file1);
    fread(&hnsw_index.globalHeader.m_L,sizeof(uint32_t),1,file1);
    fread(&hnsw_index.globalHeader.max_level,sizeof(uint64_t),1,file1);
    fread(&hnsw_index.globalHeader.num_nodes,sizeof(uint64_t),1,file1);
    fread(&hnsw_index.globalHeader.dim,sizeof(uint64_t),1,file1);
    fclose(file1);
    std::cout << "load global header successfully!" << std::endl;
    fseek(file2, 0, SEEK_END);
    uint64_t file_size = ftell(file2);
    uint64_t blocksize = 4*dim+sizeof(uint64_t);
    uint64_t blockNum =(file_size)/blocksize;
    for(int i = 0;i<blockNum;i++)
    {
        std::vector<float> deleted(dim);
        uint64_t id;
        fread(&id,sizeof(uint64_t),1,file2);
        fread(deleted.data(),sizeof(float),dim,file2);
        deleted_nodes.push_back(DeletedNode(id, deleted));
    }
    fclose(file2);
    std::cout << "load deleted nodes successfully!" << std::endl;
    std::vector<std::string> files;
    std::string nodes_path = hnsw_data_root + "nodes/";
    hnsw_index.layers.clear();
    hnsw_index.layers.resize(hnsw_index.globalHeader.max_level+1);
    for(int i = 0;i<hnsw_index.globalHeader.num_nodes;i++)
    {
        std::string node_path_root = nodes_path + std::to_string(i) + "/";
        std::string header_path = node_path_root + "header.bin";
        FILE* file = fopen(header_path.c_str(), "rb");
        if(file==NULL)
        {
            printf("cannot open a file 6\n");
            return;
        }
        std::vector<float> vector;
        Node node;
        fread(&node.max_level,sizeof(uint32_t),1,file);
        fread(&node.key,sizeof(uint64_t),1,file);
        node.id = i;
        node.is_deleted = false;
        node.layer_connections.clear();
        // node.layer_connections.resize(node.max_level+1);
        vector.resize(hnsw_index.globalHeader.dim);
        node.vector = Cache[node.key];
        std::string edges_root_path = node_path_root + "edges/";
        files.clear();
        size_t neighbors = utils::scanDir(edges_root_path,files);
        for(int j = 0;j<neighbors;j++)
        {
            std::string edges_path = edges_root_path + std::to_string(j) + ".bin";
            FILE* file2 = fopen(edges_path.c_str(), "rb");
            if(file2==NULL)
            {
                printf("cannot open a file 7\n");
                return;
            }
            uint32_t num_edges;
            fread(&num_edges,sizeof(uint32_t),1,file2);
            std::vector<uint64_t> edges(num_edges);
            fread(edges.data(),sizeof(uint64_t),num_edges,file2);
            node.layer_connections.push_back(edges);
            fclose(file2);
        }
        if(hnsw_index.nodes.size()==node.id)
        {
            hnsw_index.nodes.push_back(node);
        }

        for(int i = 0;i<=node.max_level;i++)
        {
            hnsw_index.layers[i][node.id] = node.layer_connections[i];
        }
        fclose(file);
    }   
    //used to set breakpoints.
    int a = 1;
            
}
void KVStore::save_embedding_to_disk(const std::string &data_root)
{
    if (!utils::dirExists(data_root)) {
        utils::mkdir(data_root.data());
    }
    std::string file_path = data_root + "/embedding.bin";
    bool is_initialized = fs::exists(file_path) && fs::file_size(file_path) > 0;
    if (is_initialized) {  //这就只用追加修改了，否则就是先把cache当中的都写进去
        std::cout << "Embedding file already exists. appending..." << std::endl;
        FILE* file = fopen((file_path).c_str(), "ab");
        if(file==NULL)
        {
            printf("cannot open a file 8\n");
            return;
        }
        for(auto it:dirty_keys) //被修改过的数据
        {
            fwrite(&it,sizeof(std::uint64_t),1,file);
            if(Cache.count(it)) //如果现在还在cache当中，说明是被修改
            {
                fwrite(Cache[it].data(),sizeof(float),dim,file);
            }
            else //说明是被删了，写入deleted的标志
            {
                std::vector<float> deleted(dim, std::numeric_limits<float>::max());
                fwrite(deleted.data(), sizeof(float), dim, file);
            }
        }
        dirty_keys.clear();
        fclose(file);
        std::cout << "save embedding to disk successfully!" << std::endl;
    }    
    else
    {
        std::cout << "Creating new embedding file..." << std::endl;
        FILE* file = fopen((file_path).c_str(), "wb");
        if(file==NULL)
        {
            printf("cannot open a file 9\n");
            return;
        }
        fwrite(&dim,sizeof(std::uint64_t),1,file);
        for(auto it:Cache)
        {
            fwrite(&it.first,sizeof(std::uint64_t),1,file);
            fwrite(it.second.data(),sizeof(float),dim,file);
        }
        dirty_keys.clear();
        fclose(file);
        std::cout << "save embedding to disk successfully!" << std::endl;
    }
    
   
}


std::vector<KVStore::SimKey> KVStore::find_top_k_in_chunk(
    std::unordered_map<std::uint64_t, std::vector<float>>::const_iterator begin,
    std::unordered_map<std::uint64_t, std::vector<float>>::const_iterator end,
    const std::vector<float>& query_embedding,
    int k_per_chunk)
{
    auto cmp = [](const SimKey& a, const SimKey& b) { return a.first > b.first; };
    std::priority_queue<SimKey, std::vector<SimKey>, decltype(cmp)> top_k_chunk(cmp);

    for (auto it = begin; it != end; ++it) {
        const auto& key = it->first;
        const auto& embedding = it->second;

        float sim = cosine_similarity(embedding, query_embedding);

        if (top_k_chunk.size() < k_per_chunk || sim > top_k_chunk.top().first) {
            top_k_chunk.push({sim, key});
            if (top_k_chunk.size() > k_per_chunk) {
                top_k_chunk.pop(); // 移除块内当前最小相似度的项
            }
        }
    }

    // 将堆中的元素转移到向量中
    std::vector<SimKey> result_chunk;
    result_chunk.reserve(top_k_chunk.size());
    while (!top_k_chunk.empty()) {
        result_chunk.push_back(top_k_chunk.top());
        top_k_chunk.pop();
    }
    // 不需要在这里排序，最终的合并步骤会处理总排序

    return result_chunk;
}


// 并行版本的 query_knn 实现
std::vector<std::pair<std::uint64_t, std::string>> KVStore::query_knn_parallel(
    const std::vector<float>& embStr,
    int k)
{
    if (Cache.empty() || k <= 0) {
        return {}; // 无数据或 k 无效
    }

    
    const size_t num_threads = std::thread::hardware_concurrency(); //可以修改这个变量来改变线程数
    // const size_t num_threads = 1;


    std::vector<std::unordered_map<std::uint64_t, std::vector<float>>::const_iterator> chunk_starts;
    chunk_starts.push_back(Cache.cbegin());
    size_t cache_size = Cache.size();
    size_t elements_per_thread = cache_size / num_threads;

    auto current_it = Cache.cbegin();
    for (size_t i = 0; i < num_threads - 1; ++i) {

        size_t elements_advanced = 0;
        while(elements_advanced < elements_per_thread && current_it != Cache.end()){
             ++current_it;
             ++elements_advanced;
        }
        chunk_starts.push_back(current_it);
    }
    chunk_starts.push_back(Cache.cend()); 

    std::vector<std::future<std::vector<SimKey>>> map_futures;
    int k_per_chunk = k;
    for (size_t i = 0; i < num_threads; ++i) {
        map_futures.push_back(
            std::async(std::launch::async,
                       &KVStore::find_top_k_in_chunk, // 成员函数指针
                       this, // 调用成员函数需要传递对象指针
                       chunk_starts[i],    // 块开始迭代器
                       chunk_starts[i+1],  // 块结束迭代器
                       embStr,  // 查询 embedding，使用引用包装器避免拷贝大向量
                       k_per_chunk)
        );
    }

    auto cmp_global = [](const SimKey& a, const SimKey& b) { return a.first > b.first; };
    std::priority_queue<SimKey, std::vector<SimKey>, decltype(cmp_global)> top_k_global(cmp_global);

    for (auto& fut : map_futures) {
        try {
            std::vector<SimKey> chunk_results = fut.get(); // 等待并获取块内 Top-K
            for (const auto& sim_key : chunk_results) {
                if (top_k_global.size() < k || sim_key.first > top_k_global.top().first) {
                     top_k_global.push(sim_key);
                     if (top_k_global.size() > k) {
                        top_k_global.pop(); // 移除全局当前最小相似度的项
                     }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error in Map task: " << e.what() << std::endl;
          
        }
    }

    std::vector<SimKey> final_sim_keys;
    final_sim_keys.reserve(top_k_global.size());
    while(!top_k_global.empty()){
        final_sim_keys.push_back(top_k_global.top());
        top_k_global.pop();
    }
    std::reverse(final_sim_keys.begin(), final_sim_keys.end()); // 变为降序排列
    std::vector<std::future<std::string>> get_futures;
    std::vector<std::uint64_t> keys;
    get_futures.reserve(final_sim_keys.size());
    keys.reserve(final_sim_keys.size());

    for(const auto& sim_key : final_sim_keys){
        std::uint64_t key = sim_key.second;
        keys.push_back(key);
        get_futures.push_back(
            std::async(std::launch::async,
                       &KVStore::get, // 成员函数指针
                       this, // 调用成员函数需要传递对象指针
                       key)
        );
    }

    std::vector<std::pair<std::uint64_t, std::string>> result;
    result.reserve(get_futures.size());

    for (size_t i = 0; i < get_futures.size(); ++i) {
        try {
            result.emplace_back(keys[i], get_futures[i].get());
        } catch (const std::exception& e) {
            std::cerr << "Error in Get task: " << e.what() << std::endl;
        }
    }
    return result;
}
