# 核账运行手册

需要 Python 3.11，所有命令在项目根目录运行。测试及日剧本只在自身创建的临时目录内生成资料库、候选和完整性夹具，不使用用户现有资料目录。

完整原套件（含所有扩展断言）：

```sh
python3 -B -m unittest discover -s tests -t .
```

接纳完整替换、NULL 覆盖、删除及重开序号：

```sh
python3 -B -m unittest tests.test_admission.TestAdmission.test_full_replace_delete
```

重放身份、坏批次、重复打开、提交失败及源异常：

```sh
python3 -B -m unittest tests.test_admission.TestAdmission.test_replay_and_rejection tests.test_admission.TestAdmission.test_bad_batch_atomic tests.test_admission.TestAdmission.test_initialization_preserves_state tests.test_admission.TestAdmission.test_publish_rollback tests.test_admission.TestAdmission.test_source_failure_atomic
```

整数核账、过滤、必要列读取、损坏和部分文件压实：

```sh
python3 -B -m unittest tests.test_processing.TestTotals tests.test_column_access.TestColumns tests.test_compaction.TestCompaction
```

固定成果、发布失败、失败候选与恢复重试：

```sh
python3 -B -m unittest tests.test_snapshots.TestBindings tests.test_publication.TestPublication
```

实际日工作流：

```sh
python3 -B -m tests.test_processing
```

稀疏更正联合演示（离线更正、显式 NULL、删除重建、交错压实、关闭重开）：

```sh
python3 -B -m tests.test_patching_demo
```

稀疏更正的独立按序重放核账与边界（含 put/patch/delete 全交错枚举、非连续子集、连续两次压实、必要载荷访问、重放身份、发布原子性）：

```sh
python3 -B -m unittest tests.test_patching
```

日工作流读取 samples/day.json，先断言手算结果及独立完整行重放参照，再打印单行 POINTWISE_SCENARIO JSON，其中 actual 和 downstream_actual 来自已保存成果。它同时执行站点过滤、部分压实和关闭重开；异常或断言不符会以非零退出。它不会打印固定成功标志代替计算。test_patching_demo 同样以断言驱动并打印单行 PATCHING_SCENARIO JSON，期望值来自不调用待验证实现的独立字典重放。

压实回归对 27 种三操作历史的全部 7 个非空选取子集逐项核对，再做第二轮压实；另有跨未选文件的删除屏障及原序号反例。稀疏更正回归枚举 put×2、patch×2、delete 共 5 种操作在 1/2/3 长度下的全部历史（155 条），对每条历史的全部非空选取子集在独立资料库中核对独立按序重放参照与两轮压实，并以 4/5 步屏障分段的精选历史补覆盖。候选完整性回归会在测试临时目录构造真实错误候选和损坏字节。失败测试使用普通提交前异常，不模拟杀进程。完整套件包含这些断言，分组命令不替代 discover。
