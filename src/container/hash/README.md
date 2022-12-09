参考资料：
https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/

## Bucket
插入，删除，查找都是直接list遍历  

## Insert
true循环判断是否能够插入，一但插入成功跳出循环  
判断localDepth是否等于globalDepth，等于执行操作：globalDepth+1，扩充dir长度一倍，重新分配dir指针，多个指针可以指向同一个bucket。  
插入失败即该bucket满，分裂bucket，重新分配其中的kv数据。  
