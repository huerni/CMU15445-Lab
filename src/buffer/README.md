# lru与clock
顺序泛洪问题  
# lru_k_replacer  
```
make lru_k_replacer_test -j$(nproc)
./test/lru_k_replacer_test 
```
设置三个数据结构  
```c++
// 存储存入的帧id
std::list<frame_id_t> lru_;
// 能在O(1)时间内查找到帧id
std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> cache_;
// lru_k 专属，存储每个帧的访问历史，存储k个
std::unordered_map<frame_id_t, std::vector<int>> hast_;
// O(1)时间判断帧是否可替换
std::unordered_set<frame_id_t> is_evictable_;
```
## Evict 
选择需要替换的帧进行替换，根据两条规则：  
1. 按照进入时间先后顺序遍历，当没有k次访问历史时，即为初始值INF，进行替换  
2. 若都有k次访问，选择最早的第k次访问时间的帧，进行替换  

## RecordAccess
1. 判断是否存在帧，如果不存在，添加帧  
2. 如果存在，更新访问历史  

# buffer_pool_manager
```
make buffer_pool_manager_instance_test -j$(nproc)
./test/buffer_pool_manager_instance_test


 sudo perf record -g ./test/buffer_pool_manager_instance_test
 sudo perf report -g

make format
make check-lint
make check-clang-tidy-p1

zip project1-submission.zip \
    src/include/container/hash/extendible_hash_table.h \
    src/container/hash/extendible_hash_table.cpp \
    src/include/buffer/lru_k_replacer.h \
    src/buffer/lru_k_replacer.cpp \
    src/include/buffer/buffer_pool_manager_instance.h \
    src/buffer/buffer_pool_manager_instance.cpp
```
## NewPgImp
创建新的page送入buffer pool。  
1. 若有空帧，从free_list中选择空帧
2. 若无空帧，选择替换帧，进行替换。若替换帧修改过，写入磁盘  

## FetchPgImp  
从buffer pool中取指定page。  
1. 若在pool中，访问取出  
2. 若不在，与NewPgImp操作差不多，选择帧替换  

## UnpinPgImp
page引用数减1  

## FlushPgImp
将buffer pool中指定page写入磁盘

## FlushAllPgsImp
将buffer pool中所有page写入磁盘

## DeletePgImp
删除buffer pool中没有修改，没有引用的指定page  
## 参考资料
https://segmentfault.com/a/1190000022558044  
https://www.gradescope.com/courses/425272/assignments/2305366/submissions/169059422?view=results#Leaderboard.Time
https://15445.courses.cs.cmu.edu/fall2022/project1/