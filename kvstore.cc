#include "kvstore.h"

#include "skiplist.h"
#include "sstable.h"
#include "utils.h"

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



KVStore::KVStore(const std::string &dir) :
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
    ss.putFile(ss.getFilename().data());
    compaction(); // 从0层开始尝试合并  c:需要实现
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &val) {
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
// SSTable文件：
// [文件头]      // 32字节
// [布隆过滤器]  // 10240字节
// [索引区]      // 12字节 * 键值对数量
// [数据区]      // 实际数据
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
            utils::rmfile(file.data());
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


void KVStore::compaction() {
    int curLevel = 0;
    uint64_t minVtmp = UINT64_MAX, maxVtmp = 0;
    std::list<std::pair<uint64_t, std::string>> complist;
    std::vector<sstablehead> waitlist;
    std::priority_queue<poi, std::vector<poi>, cmpPoi> pq;
    std::unordered_set<uint64_t> key;
    std::unordered_map<uint64_t, uint64_t> keyMaxTime;
    int j = 0;
    int sizeCur = sstableIndex[curLevel].size();
    int sizeNxt = sstableIndex[curLevel+1].size();
    bool updateLevel = false;
    sstable newTable;
    std::string newPath;
    for(curLevel;curLevel<=totalLevel;curLevel++)
    {
        updateLevel = false;
        sizeCur = sstableIndex[curLevel].size();
        sizeNxt = sstableIndex[curLevel+1].size();
        if(sizeNxt==0)
        {
            updateLevel = true;
        }
        if(sizeCur>pow(2,curLevel+1))
        {
            if(updateLevel)
            {
                totalLevel++;
            }
            if(curLevel==0)
            {
                j = 0;
            }
            else
            {
                j = pow(2,curLevel+1);
            }
            for(;j<sizeCur;j++)
            {
                if(minVtmp>sstableIndex[curLevel][j].getMinV())
                    minVtmp = sstableIndex[curLevel][j].getMinV();
                if(maxVtmp<sstableIndex[curLevel][j].getMaxV())
                    maxVtmp = sstableIndex[curLevel][j].getMaxV();
                int IndexSize = sstableIndex[curLevel][j].getIndexSize();
                waitlist.push_back(sstableIndex[curLevel][j]);
                bool isX = isPathOfLevel(sstableIndex[curLevel][j].getFilename(),0);
                if(isX)
                {
                    std::cout<< "here!" << std::endl;
                }
                // std::cout << sstableIndex[curLevel][j].getFilename()<<std::endl;
                for(int k = 0;k<IndexSize;k++)
               { 
                    poi p;
                    for(int q = 0;q<curLevel;q++)
                    {
                        p.sstableId +=sstableIndex[q].size();
                    }
                    p.sstableId += j;
                    p.pos = k;
                    p.time = sstableIndex[curLevel][j].getTime();
                    p.index = sstableIndex[curLevel][j].getIndexById(p.pos);
                    p.sstableHeadI = curLevel;
                    p.sstableHeadJ = j;
                    pq.push(p); 
               }
            }
            //scan(minVtmp,maxVtmp,*complist);//把所有需要汇总的键值对给收集起来了
            //L0层的都必须去归并
            if(!updateLevel)
            {
                for(int j = 0;j<sizeNxt;j++)
                {
                    if(!(sstableIndex[curLevel+1][j].getMaxV()<minVtmp||sstableIndex[curLevel+1][j].getMinV()>maxVtmp))
                    {
                        waitlist.push_back(sstableIndex[curLevel+1][j]);
                        bool isX = isPathOfLevel(sstableIndex[curLevel+1][j].getFilename(),0);
                        if(isX)
                        {
                            std::cout<< "here!" << std::endl;
                        }
                        int IndexSize = sstableIndex[curLevel+1][j].getIndexSize();
                        for(int k = 0;k<IndexSize;k++)
                        {
                            poi p;
                            for(int q = 0;q<curLevel+1;q++)
                            {
                                p.sstableId +=sstableIndex[q].size();
                            }
                            p.sstableId += j;
                            p.sstableHeadI = curLevel;
                            p.sstableHeadJ = j;
                            p.pos = k;
                            p.time = sstableIndex[curLevel+1][j].getTime();
                            p.index = sstableIndex[curLevel+1][j].getIndexById(p.pos);
                            pq.push(p);
                        }
                    }
                }
            }
            //将涉及到的文件读到内存，进行归并排序，并生成sstable写回下一层。
            //如果后插入的time比已经pop出的要大,所以需要先记录所有key的最大time
            for(auto &it : waitlist)
            {
                for(int i = 0;i<it.getCnt();i++)
                {
                    uint64_t key = it.getKey(i);
                    if(keyMaxTime.find(key)==keyMaxTime.end()||keyMaxTime[key]<it.getTime())
                        keyMaxTime[key] = it.getTime();
                }
            }
    //只是把每个sstable的第一个key给放进去了，后面的要慢慢塞进去，现在需要创造新的sstable
            newPath = "./data/level-" + std::to_string(curLevel + 1) + "/";
            if (!utils::dirExists(newPath)) {
                utils::mkdir(newPath.data());
            }
            // 生成文件名（通常基于时间戳）

            while(!pq.empty())
            {
                
                poi p = pq.top();
                pq.pop();
                if(key.find(p.index.key)==key.end()&&p.time == keyMaxTime[p.index.key])
                {
                    key.insert(p.index.key);
                    sstable ss;
                    ss.loadFile(sstableIndex[p.sstableHeadI][p.sstableHeadJ].getFilename().data());
                    uint32_t len;
                    int offset = ss.searchOffset(p.index.key, len);
                    if(offset != -1) {
                        // 使用 fetchString 读取数据
                        std::string value = fetchString(
                            sstableIndex[p.sstableHeadI][p.sstableHeadJ].getFilename(),
                            offset,
                            len
                        );
                        // 将key-value对插入新的SSTable
                        if(!newTable.checkSize(value,curLevel,0))
                        {
                            newTable.insert(p.index.key, value);
                        }
                        else
                        {
                            std::string filename = newPath + std::to_string(++TIME) + ".sst";
                            newTable.setFilename(filename);  // 设置文件名
                            newTable.putFile(newTable.getFilename().data());
                            addsstable(newTable,curLevel+1);
                            newTable.reset();
                            //newTable.setFilename(newPath + std::to_string(++TIME) + ".sst");
                            newTable.insert(p.index.key, value);
                        }
                    }
                }
                if(p.pos + 1 < sstableIndex[p.sstableHeadI][p.sstableHeadJ].getIndexSize()) {
                    poi next;
                    next.sstableId = p.sstableId;
                    next.pos = p.pos + 1;
                    next.time = p.time;
                    next.index = sstableIndex[p.sstableHeadI][p.sstableHeadJ].getIndexById(next.pos);
                    next.sstableHeadI = p.sstableHeadI;
                    next.sstableHeadJ = p.sstableHeadJ;
                    pq.push(next);
                }
            }
            //删除已经被合并的文件
            // 处理当前层// 处理最后一个SSTable
            if(newTable.getCnt() > 0) {
                std::string filename = newPath + std::to_string(++TIME) + ".sst";
                newTable.setFilename(filename);  // 设置文件名
                newTable.putFile(newTable.getFilename().data());
                addsstable(newTable,curLevel+1);
            }

            for(auto &it : waitlist)
            {
                delsstable(it.getFilename());
            }
        }
    }
//从L1层往下 ，仅需要将超出的文件往下一层进行合并即可，无需合并该层的所有文件。
//在合并的时候如果遇到相同键K的多条记录，通过比较时间戳来决定K的最新值，时间戳大的被保留。
//完成一次合并操作需要更新涉及到的SSTable在内存中的缓存信息。也就是sstableindex
    // TODO here
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
    FILE *fp = fopen(file.c_str(), "rb");
    if (fp == NULL) {
        std::cerr << "Failed to open file: " << file << std::endl;
        return "";
    }   
    fseek(fp, startOffset, SEEK_SET);
    fread(strBuf, 1, len, fp);
    fclose(fp);
    return std::string(strBuf, len);
}
