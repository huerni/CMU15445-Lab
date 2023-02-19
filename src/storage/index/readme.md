
## insert
1. 根据 key 找到指定叶子节点，插入  
2. 若叶子节点 `size==max_size` ,对叶子节点进行分裂，向父节点添加key，并修改右指针指向  
3. 向上递归判断父节点 `size ?= max_size` ，若等于，继续分裂递归，若小于，插入结束。  
```
make b_plus_tree_insert_test -j$(nproc)
./test/b_plus_tree_insert_test
```

## delete 
1. 根据 key 找到指定叶子节点，删除  
2. 若叶子节点 `size<min_size` ，则借左(右)兄弟节点    
3. 若借到了(`size > min_size`)，修改父节点key，删除结束  
4. 若借不到，则和左(右)兄弟合并，并删除父节点key  
5. 向上递归判断父节点 `size ?< min_size` ，若小于，同借兄弟，借到了，删除结束  
6. 若借不到，则和左(右)兄弟，父节点合并，返回第5步继续。
```
make b_plus_tree_delete_test -j$(nproc)
./test/b_plus_tree_delete_test
```

## index iterator
```
make b_plus_tree_concurrent_test -j$(nproc)
./test/b_plus_tree_concurrent_test
```

## submit
https://www.gradescope.com/courses/425272
```
make format -j$(nproc)
make format -j$(nproc)
make check-clang-tidy-p2 -j$(nproc)

make submit-p2
```

## 参考资料
https://15445.courses.cs.cmu.edu/fall2022/project2/#index-iterator  
https://www.cnblogs.com/nullzx/p/8729425.html    
https://dichchankinh.com/~galles/visualization/BPlusTree.html    
