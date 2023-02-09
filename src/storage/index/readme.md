
## insert
```
make b_plus_tree_insert_test -j$(nproc)
./test/b_plus_tree_insert_test
```

## delete 
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
make check-lint -j$(nproc)
make check-clang-tidy-p2 -j$(nproc)

make submit-p2
```

## 参考资料
https://15445.courses.cs.cmu.edu/fall2022/project2/#index-iterator  
https://www.cnblogs.com/nullzx/p/8729425.html    
https://dichchankinh.com/~galles/visualization/BPlusTree.html    
