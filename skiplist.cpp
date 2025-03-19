#include <cstdint>
#include <limits>
#include <list>
#include <string>
#include <vector>

#include "skiplist.h"

double skiplist::my_rand()
{
    int S = 0xFFFF, PS = S * p;
    return rand() & S;
}
int skiplist::randLevel()
{
    int lv = 1; 
    int S = 0xFFFF, PS = S * p;
    while (my_rand()< PS) 
        ++lv; 
    curMaxL = std::min(MAX_LEVEL, lv); 
    return curMaxL; 
}
void skiplist::insert(uint64_t key, const std::string &str)
{
    std::vector<slnode*> update(MAX_LEVEL);
	slnode *cur = head;
	for(int i = curMaxL-1; i >= 0; --i){
		//一旦大了就下移一层
		while(cur->nxt[i]->key < key){
			cur = cur->nxt[i];
		}
		update[i] = cur;
	}
    if (cur->nxt[0]->key == key && cur->nxt[0]->type == NORMAL) {
        cur->nxt[0]->val = str;
        return;
    }
	slnode *newNode = new slnode(key, str, NORMAL);
    for(int i = 0; i < curMaxL; ++i){
		newNode->nxt[i] = update[i]->nxt[i];
		update[i]->nxt[i] = newNode;
	}
    bytes += 12+str.length();
	return;
}
std::string skiplist::search(uint64_t key)
{
    slnode *cur = head;
    for(int i = curMaxL-1; i >= 0; --i){
        while(cur->nxt[i]->key < key){
            cur = cur->nxt[i];
        }
    }   
    cur = cur->nxt[0];
    if(cur->key == key && cur->type == NORMAL){
        return cur->val;
    }
    return " ";
}
bool skiplist::del(uint64_t key, uint32_t len)
{
    slnode *cur = head;
    std::vector<slnode*> update(MAX_LEVEL);
    for(int i = curMaxL-1; i >= 0; --i){
        while(cur->nxt[i]->key < key){
            cur = cur->nxt[i];
        }
        update[i] = cur;
    }
    cur = cur->nxt[0];
    if(cur->key == key && cur->type == NORMAL){
        for(int i = 0; i < curMaxL; ++i){
            if(update[i]->nxt[i] != cur){
                break;
            }
            update[i]->nxt[i] = cur->nxt[i];
        }
        bytes -= 12+len;
        delete cur;
        return true;
    }

}
void skiplist::scan(uint64_t key1, uint64_t key2, std::vector<std::pair<uint64_t, std::string>> &list)
{
    slnode *Lnode = lowerBound(key1);
    slnode *Rnode = lowerBound(key2);
    if(Lnode->key<=Rnode->key&& Rnode->key<= tail->nxt[0]->key)
    {
        slnode *cur = Lnode;
        while(cur!=Rnode){
            if(cur->type == NORMAL){
                list.push_back(std::pair<uint64_t, std::string>(cur->key, cur->val));
            }
            cur = cur->nxt[0];
        }
    }
    return;

}
slnode *skiplist::lowerBound(uint64_t key)
{
    slnode *cur = head;
    for(int i = curMaxL-1; i >= 0; --i){
        while(cur->nxt[i]->key < key){
            cur = cur->nxt[i];
        }
    }
    return cur->nxt[0];
}
void skiplist::reset()
{
    slnode *cur = head->nxt[0];
    while(cur->type != TAIL){
        slnode *tmp = cur->nxt[0];
        delete cur;
        cur = tmp;
    }
}
uint32_t skiplist::getBytes()
{
    return bytes;
}