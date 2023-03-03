
## seq_scan
表达式树为根节点，只需要从头到尾遍历表中数据  
```
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.01-seqscan.slt --verbose
```

## insert_executor
向表中插入元组并更新索引。  
The planner will ensure values have the same schema as the table.   
执行器将生成一个整数元组作为输出，表示在插入所有行之后，已经向表中插入了多少行。  
如果表中有关联的索引，请记住在插入表时更新索引。  
```
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.02-insert.slt --verbose
```

## delete_executor
```
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.03-delete.slt --verbose
```

## index_scan
```
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.04-index-scan.slt --verbose
```

## aggregation_executor
```
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.06-simple-agg.slt --verbose
./bin/bustub-sqllogictest ../test/sql/p3.07-group-agg-1.slt --verbose
./bin/bustub-sqllogictest ../test/sql/p3.08-group-agg-2.slt --verbose
```

## nested_loop_join
```
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.09-simple-join.slt --verbose
```

## nested_index_join
```
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.12-nested-index-join.slt --verbose
```

## 提交
```
cd build && make -j$(nproc) shell
./bin/bustub-shell


make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.00-primer.slt --verbose

make format
make check-lint
make check-clang-tidy-p3
```

## 参考资料
https://15445.courses.cs.cmu.edu/fall2022/project3/