# lru_k_replacer  
```
make lru_k_replacer_test -j$(nproc)
./test/lru_k_replacer_test 
```
## Evict


# buffer_pool_manager
```
make buffer_pool_manager_instance_test -j$(nproc)
./test/buffer_pool_manager_instance_test

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