# 流域监测资料湖

Python 3.11 标准库应用。接入员接纳网关的完整监测行及撤回批次，加工员固定快照生成站点日核账，维护员合并小文件后比较当前明细、历史快照和已交付成果。

所有业务列为可空整数，整数排除 bool；ID 和批次 ID 是非空字符串。单位为设备输出的整数原单位，无浮点换算。完整写替换全部列，NULL 是明确的缺测值，删除隐藏整行。列集合在首次创建时固定；相同路径、相同有序模式重复打开保留已有修改，不兼容模式抛 ValueError。支持单进程串行使用，不提供并发写、动态模式、对象回收或通用 SQL。

## 运行

在项目根目录执行：

```sh
python3 -B -m unittest discover -s tests -t .
python3 -B -m tests.test_processing
python3 -B -m tests.test_patching_demo
```

第二条命令实际读取 samples/day.json，在测试临时目录执行接纳、重放裁决、核账、非连续压实及关闭重开，断言后打印 POINTWISE_SCENARIO JSON。第三条在临时目录联合演示稀疏更正、显式 NULL、删除重建、交错压实、关闭重开，断言后打印 PATCHING_SCENARIO JSON。没有安装步骤或第三方依赖。完整分组运行命令见 RUNBOOK.md。

## 公开 API

```python
from lake import Lake, Unavailable, IntegrityError

with Lake("monitoring-data", ("station", "a", "b")) as lake:
    receipt = lake.ingest("gateway-001", [
        {"op": "put", "id": "x", "values": {"station": 1, "a": 2, "b": None}},
        {"op": "put", "id": "y", "values": {"station": 1, "a": -2, "b": 8}},
    ])
    snapshot_id = receipt["snapshot_id"]
    rows = lake.read(snapshot_id, ["a", "b"])
    correction = lake.apply_patch("gateway-001-fix", {"x": {"a": 3}})
    artifact_id = lake.run_totals(correction["snapshot_id"], None, "station", "a")
    delivered = lake.read_artifact(artifact_id)
    maintenance = lake.compact(lake.snapshot()["files"])
```

- ingest(batch_id, operations) 接受操作 iterable。put 必须恰好含 op、id、values；values 必须提供全部模式列。delete 仅含 op、id。批内 ID 唯一且批次非空。非法操作、类型、缺列、多列、重复 ID 或异内容重放抛 ValueError。源迭代器异常传播，整批不发布。
- apply_patch(batch_id, changes) 接受非空 dict：键是非空行 ID，值是含一个或多个已知列的非空 dict，值为整数或 None（bool 不是整数）。缺行上的 patch 直接建行，其余列初始为 NULL；已提供列覆盖原值（显式 None 写 NULL），未提供列沿继承链取此前值。非法结构、空批、空列映射、未知列或错误类型抛 ValueError，整批不发布。patch 与 ingest 共用批次身份空间、回执结构、原子提交和递增业务序号；同 batch_id 异内容或异操作种类拒绝，同内容重放返回原回执且不产生新序号。发布只写新文件并核对自身目录与摘要，不读取任何历史列载荷；V1 名称 `Lake.patch` 继续不存在，ingest 中 `op="patch"` 仍拒绝。
- 规范身份按 ID 排序、按固定模式顺序保存全部值和操作类型；操作顺序及映射键顺序不影响身份。同批同内容返回原回执，不移动当前指针。回执含 batch_id、commit、snapshot_id、file_id。一次批次共享一个递增业务序号；重开继续递增。
- snapshot() 返回当前 {id, files}；get_snapshot(id) 返回指定固定成员。未知快照抛 KeyError。返回容器可修改，不影响目录。
- read(snapshot_id, columns) 返回按 ID 排序的 [{id, values: {列: 值}}]。投影必须是无重复已知列的 list 或 tuple，允许空投影；未知或重复列抛 ValueError。按原业务序号重放该快照的完整写、patch 与删除：完整写或删除是继承屏障，删除隐藏整行；屏障之后的 patch 按列取最后一次覆写，未覆写列继承屏障完整写的值，屏障为删除或缺行则为 NULL。只加载最终所需值所在的 (文件, 列) 页，被完全覆写或被删除屏蔽的旧载荷不访问；所需值来自不可用载荷时抛 Unavailable，不能当 NULL。
- run_totals(snapshot_id, filter_spec, group_column, value_column) 保存成果并返回字符串成果 ID。filter_spec 可为 None、{op: "eq", column: 列, value: 整数或None}，或 {op: "range", column: 列, min: 下界, max: 上界}。范围为闭区间，可省略一侧或以 None 表示无界，至少一侧是整数；bool、逆序区间和额外字段拒绝。NULL 不匹配范围，等值 None 匹配 NULL。
- 核账读取过滤、分组及计量列的并集。同一列只加载一次。每组输出 {group, rows, non_null, sum}，行数包括缺测，SUM 忽略 NULL；非空数为零时 SUM=None，真实零和为 0。空结果为 []。分组 NULL 在前，整数升序。group 和 value 可为同列。
- read_artifact(id) 返回 {id, source, groups}；source 含 snapshot_id、filter、group_column、value_column。只读取已保存成果和绑定，不重新扫描资料；未知成果抛 KeyError，返回独立容器。
- compact(files) 选择当前快照中的非空、不重复文件 list/tuple，允许非连续选择。每个选中 ID 按未选操作形成的屏障分段归约：段内只保留最后一个完整操作及其后按列合并的 patch（同列重复覆写仅留最后一次），保留原操作类型与原序号；不读取未选文件的列载荷，未选文件保持原样。返回 plan_id、input_snapshot_id、snapshot_id、file_id、selected。维护不产生业务序号，不删除旧文件；旧快照及成果继续可读。固定行数、列数下，同列重复覆写次数增加不会使新产物的目录或列载荷规模增长。非法选择或过时计划抛 ValueError。

## 载荷、错误与原子性

lake.payload_port.set_available(file_id, column, False) 可精确离线一列；True 恢复。access_log 返回实际访问的 (file_id, column) 副本，clear_access_log() 清空记录。覆盖状态仅在当前 Lake 实例内有效，重开默认可用。必要文件缺失或离线抛 Unavailable，必要字节摘要或编码错误抛 IntegrityError，均不能变成 NULL 或成功成果。操作目录不经过离线端口，仍核对摘要；无关列及完全被覆盖的旧载荷不影响投影。

SQLite catalog.sqlite 保存模式、提交、回执、快照、来源绑定、成果及维护计划。segments/ 下每个随机文件 ID 对应一个不可变目录和各列独立 JSON 页。目录只含 ID、原业务序号、操作类型（put/delete/patch）、行位置及 patch 的列索引掩码；patch 未提供的列在页上编码为 NULL 且不进入掩码，完整操作不带掩码。列值、批次规范内容及成果数值使用规范十六进制整数编码，避免十进制 JSON 位数限制；公开 API 仍只接受和返回整数或 None，不接受数值字符串。文件先完成和核验，再在目录事务中发布引用。失败残留文件不可见，V1 不做回收。

接纳的提交、成员、当前指针与回执同事务发布；成果绑定与结果同事务发布；维护的新成员、当前指针、成功状态及回执同事务发布。普通异常回滚，既有成果和旧快照不变。维护计划依次为 PLANNED、CANDIDATE、VERIFIED、PUBLISHED；未成功的尝试可记录 FAILED，恢复后用 compact 创建新计划重试。候选检查独立核对 ID、操作、原序号、全部值和摘要。这里不承诺杀进程恢复或跨进程协调。

诊断接口 lake.catalog.before_commit 是可选回调，接收事务标签；回归使用 contextlib 夹具在 ingest、artifact 或 compact 的 COMMIT 前抛普通异常，随后恢复回调。lake.planner.get(plan_id) 可读取持久维护状态。lake.segments.payload_path(file_id, column) 用于离线完整性核验；正常业务不得原地修改已发布文件。

## 手算日剧本

samples/day.json 包含五条晨间完整行、替换 x、撤回 q、补入 t。重放晨间批次取得原回执，缺列及异内容重放均拒绝。固定最新快照后按 station 分组、a 计量：

| station | rows | non_null | sum |
| --- | ---: | ---: | ---: |
| NULL | 1 | 1 | -5 |
| 1 | 3 | 2 | 0 |
| 2 | 1 | 0 | NULL |

下游质量核账程序按保存成果读取上述结果。维护选择晨间和撤回文件压实，保留夹层 x 替换及 t 文件，重新核账得到相同结果；原快照、成果及重开后的内容均保持固定。

## 稀疏更正的表示不变量与失败边界（交接说明）

表示不变量：

1. 每个文件不可变；目录按 (ID, 原业务序号) 排序，同一 ID 的序号严格递增。put/delete 行无列掩码，patch 行的掩码是非空、升序、去重的模式列索引；patch 掩码外的页单元必须为 NULL。
2. 读语义是纯函数：对固定快照按序号重放 put/patch/delete，完整写与删除构成按列继承屏障；删除切断此前全部列继承，完整写替换全部列；屏障之后的 patch 仅影响其掩码列，显式 NULL 与未提供列的区别只由掩码表达。
3. patch 批次与完整写批次共用同一序号、回执、快照与原子提交表；同 batch_id 的规范身份包含操作种类，重放不产生新序号、新文件或当前指针移动。
4. 压实产物在交错的未选操作下与压实前逐值等价：未选完整操作是屏障，候选保留原 op 与原 seq，绝不把屏障两侧的 patch 合成带后者序号的完整行；每个屏障分段每列至多保留一次覆写，重复覆写次数不改变产物规模。
5. 旧快照绑定旧文件成员，已保存成果只读存档值；压实失败不修改当前目录、指针或旧成果。

失败边界：

- 非法 patch 结构、空批、空列映射、未知列、bool/浮点等错误类型在进入事务前抛 ValueError，整批不发布；同 batch_id 异内容或异操作种类同样拒绝。
- patch 发布不访问任何历史列载荷，旧列离线不阻塞更正；压实只读取选中文件的获胜载荷，未选文件仅读目录元数据。读取仅在最终所需值来自离线或缺失载荷时抛 Unavailable，绝不折算为 NULL；摘要或编码损坏仍抛 IntegrityError。
- 普通提交前异常（catalog.before_commit）回滚 ingest、artifact、compact 事务；压实尝试可留 FAILED 计划与不可见残留文件，重开后用新计划重试。不承诺杀进程恢复、并发写、动态模式或对象回收。

实现按状态职责组织：schema/batches/patching/admission 管接纳与稀疏规范化；row_directory/value_pages/segments/payload_port 管分列输入与 patch 掩码；catalog_store/commits/snapshots/receipts/bindings 管发布与绑定；scans/filters/group_totals/accounting/artifacts 管按列继承读取与核账；compaction_plans/compaction_merge/compaction_checks/rewrite/compaction_publish 管屏障分段维护。
