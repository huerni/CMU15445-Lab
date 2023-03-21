
## lock_manager
四种隔离等级  
五种锁  
2PL模式  

```
cd build
make lock_manager_test
./test/lock_manager_test
``` 

## Deadlock Detection
```
cd build
make deadlock_detection_test
./test/deadlock_detection_test
```
## transaction test
对seq_scan,insert,delete进行加锁，但加减锁失败时，abort  
且在abort之前恢复所有写操作  

在有意向锁的情况下，数据库管理系统会为事务分配锁来确保事务并发执行时数据的一致性。四种隔离级别分别如下分配锁：  
  
读未提交（Read Uncommitted）：在该隔离级别下，读取操作不会获取任何锁，写操作会获取排它锁。因此，即使其他事务正在修改该数据，当前事务也可以读取未提交的数据。  
  
读已提交（Read Committed）：在该隔离级别下，读取操作会获取共享锁，写操作会获取排它锁。当一个事务正在修改数据时，其他事务只能读取已提交的数据，因为读取操作会获取共享锁，而写操作需要排它锁，两者之间是互斥的。  
  
可重复读（Repeatable Read）：在该隔离级别下，读取操作会获取共享锁，写操作会获取排它锁。当一个事务正在修改数据时，其他事务只能读取已提交的数据，因为读取操作会获取共享锁，而写操作需要排它锁，两者之间是互斥的。与读已提交不同的是，可重复读会在事务开始时获取一个快照，并在事务执行期间保持不变，因此可以读取已提交的数据，而不受其他事务的修改影响。  
   
序列化（Serializable）：在该隔离级别下，读取和写入操作都会获取排它锁。与其他隔离级别不同的是，序列化隔离级别会通过强制事务串行执行来避免并发问题，因此会对所有数据项加排它锁，防止其他事务修改数据。  
```
cd build
make transaction_test
./test/transaction_test
```
## 提交
```
make format
make check-lint
make check-clang-tidy-p4
make submit-p4
```
https://15445.courses.cs.cmu.edu/fall2022/project4/
https://www.gradescope.com/courses/425272/assignments/2508579/submissions/169521698