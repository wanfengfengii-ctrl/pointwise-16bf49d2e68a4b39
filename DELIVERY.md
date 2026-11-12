# 三库水质加工流水线：离线灾备发布与运维交付说明

本说明给出可直接重跑的打包、复制发布、备份、目标恢复、恢复后继续加工及回收入口，
并关联本轮真实运行产生的包摘要、截点、恢复输出与异常证据。所有命令均为独立 argv
（不是 shell 脚本），只依赖 Python 3.9+ 标准库，不需要联网、第三方依赖或常驻服务。

## 交付物

| 文件 | 作用 |
| --- | --- |
| `ops.py` | 运维组件：`build` / `backup` / `restore` / `prune` 四个子命令 |
| `pipeline.py` | 原业务三库加工；新增 `STORE_DDL` 常量与 trim 历史保护协调钩子（不改业务计算、请求绑定及提交语义） |
| `run.py` | 原业务入口；`demo` 不再依赖 `checks.py`，使发布包自包含 |
| `checks.py` | 原业务断言（保留）+ 上线回归（真实构建、备份、杀进程、恢复、回收证据） |
| `demo.py` | 真实发布包恢复演示（go-live demo） |
| `.pointwise-runtime.json` | 运行清单：`verify` 重算业务与上线证据，`demo` 展示真实恢复 |

发布包是 `ops.py build` 产出的单文件 zipapp（`__main__.py` + `pipeline.py` + `run.py`
+ `ops.py` + `samples.py` + `release.json`），固定条目时间戳与压缩参数，字节级可复现；
构建不混入构建目录、时间或业务库。包同时提供 `run`（既有业务）与 `ops`（运维）两个组件：

```
python3 -B OUT run  <既有业务参数>
python3 -B OUT ops  <运维参数>
```

## 快速重跑

```bash
# 1. 打包（可复现；同一源码重复构建摘要相同，包亦可自我重建）
python3 -B ops.py build dist/water-ops.pyz
#   -> {"artifact": "dist/water-ops.pyz", "sha256": "<64 hex>"}

# 2. 复制发布：把单个包文件复制到另一普通目录即可运行，无需源码与 PYTHONPATH
cp dist/water-ops.pyz /any/plain/dir/ && cd /any/plain/dir
python3 -B water-ops.pyz run init work/water sample-water
python3 -B water-ops.pyz run load work/water raw-1 /path/samples/initial.json
python3 -B water-ops.pyz run clean work/water clean-1 2
python3 -B water-ops.pyz run daily work/water daily-1
#   ... 既有 load/clean/daily/ack/read/trim 调用不变 ...

# 3. 备份（REPO 不存在则创建；重复调用返回原已提交结果）
python3 -B water-ops.pyz ops backup work/water repo day1
#   -> {"id": "day1", "network": "sample-water", "cut": {"raw":3,"clean":2,"daily":2}}

# 4. 目标恢复（原库与源码可同时丢失；DEST 不存在或为同网络非运行业务根）
python3 -B water-ops.pyz ops restore repo day1 restored
#   -> {"id": "day1", "root": "restored", "cut": {"raw":3,"clean":2,"daily":2}}
python3 -B water-ops.pyz run read restored        # 可核对恢复结果

# 5. 恢复后继续加工与增量备份（增量基点就是恢复点 ID）
python3 -B water-ops.pyz run load restored raw-9 new-batch.json
python3 -B water-ops.pyz run clean restored clean-9 2
python3 -B water-ops.pyz run daily restored daily-9
python3 -B water-ops.pyz ops backup restored repo day2

# 6. 回收（退役已提交恢复点并实际释放无引用载荷；重复退役幂等）
echo '["day1"]' > drop.json
python3 -B water-ops.pyz ops prune repo drop.json
#   -> {"retired": ["day1"], "reclaimed_bytes": <实际删除载荷字节数>}
```

## 仓库布局与证据定位

任务的接纳、成功与引用证据全部交付在 REPO 内：

```
repo/
  repo.json                  仓库格式标记
  objects/<sha256>           内容寻址载荷对象（不可变；tmp-* 为可回收残件）
  points/<ID>.json           已提交恢复点清单：cut、捕获上界、接纳下界、增量基点、
                             对象摘要、帧索引(seq/clock/内容摘要)、回执索引（永不删除）
  tasks/<ID>/accept.json     接纳记录：先于任何业务载荷读取落盘，固定各库 head 下界、
                             网络与运维分支身份、增量基点
  tasks/<ID>/capture-N.json  每次捕获证据：来源身份、各库捕获上界、接纳下界、
                             候选帧 seq/clock/内容摘要、候选回执、截点结果
  tasks/<ID>/state.json      引用证据：任务持有的对象摘要（GC 存活依据）
  tasks/<ID>/commit.json     成功证据：已提交结果
  restores/<token>.json      进行中恢复的存活保护
  roots/<branch>.json        恢复分支锚点（恢复后增量基点引用）
  prune/retired.json         已退役恢复点 ID
```

## 一致性设计要点

- **先接纳后读载荷**：`accept.json` 持久化（固定各库 head 为接纳下界）先于任何业务
  载荷读取；随后按库各开一致读事务捕获 `(基点捕获上界, 当前head]` 的新增帧与回执，
  首备读取当时保有历史及全部回执。不停写、不关闭裁剪。
- **逐分量最大因果闭合截点**：候选集 = 本任务实际捕获帧 + 所选增量祖先已保存帧；
  可行截点要求各库所选帧 clock 逐分量不超过截点；可行向量对逐分量取最大封闭，
  自最大候选向量向下迭代即得唯一逐分量最大截点，且不早于接纳下界；无解则重捕获。
  无无限写入时，各库当前 head 向量必然闭合并达到下界，故必取得进展。
- **历史保护协调**：接纳任务在源库 `ops-protect.json` 登记保护下界（基点+1），
  `trim` 钩子只删不受保护且超出 KEEP 的帧；提交后释放，裁剪恢复原有行为。
- **分支身份**：源根 `ops.json` 持久化运维分支；恢复为目标写入新分支身份与
  `base={point:ID, vector:cut}`，恢复后增量基点就是该 ID；链式装配按子任务所用
  基向量限幅，恢复点之后另一分支的同 seq 帧与回执不会带入。
- **锁与并发**：仅元数据临界区（接纳、对象发布、退役判定、恢复保护登记）持
  `locks/gc.lock`；载荷读写、捕获、恢复落盘均在锁外。任务级 `task.lock` 保证同 ID
  只提交一个结果；目标级锁协调同一 DEST；不同任务/目标/回收互不串行化。
- **崩溃安全**：所有状态文件先写临时文件、fsync、原子 rename；对象先落盘 fsync，
  再在临界区内 rename 并登记引用。任意持久化边界被杀后按持久状态续跑；半包不会发布。
- **恢复验证**：对象 sha256、载荷与清单帧索引/回执索引逐条核对、截点帧闭合性、
  回执 1..cut 完备性全部通过后，才写入目标；写毕再以业务入口重开校验。
  损坏/缺块在替换目标前拒绝，非本命令产生的已有目标在验证失败时不受损。
- **回收**：退役集合持久化后，存活集 = 未退役点及其增量祖先链 + 已接纳未提交任务
  的基点链与持有对象 + 进行中恢复保护链 + 恢复分支锚点链；仅删除存活集外对象，
  按实际删除的载荷文件计 `reclaimed_bytes`。

## 上线回归证据（本轮真实运行输出）

`python3 -B checks.py`（即清单 `verify`）重算原业务断言并实际执行以下场景，
关键证据（每次运行重新计算，包摘要在源码不变时恒定）：

```json
{
  "package_sha256": "a8ef58da2cdbcb839d86ba466f25f737a211e69f307e29b51391ba738f7ae41b",
  "cuts": {"p1": {"raw":3,"clean":2,"daily":2}, "p2": {"raw":5,"clean":3,"daily":3},
           "p2b": {"raw":6,"clean":4,"daily":4}, "p3": {"raw":6,"clean":4,"daily":4}},
  "cut_selection": "nonclosed (3,3,3) rejected; (4,3,3) is the component-wise maximum; acceptance lower bound enforced",
  "increment": {"p1_raw_frames": [0,1,2,3], "p2_raw_frames": [5], "p2_base": "p1"},
  "trim_protection": "frames 5-7 survived trim during an accepted backup; keep=1 retention applied after commit",
  "kill_tests": "SIGKILL at accept/object/manifest; tasks resumed from persistent state and committed",
  "concurrency": "c1/c2/dup ran in parallel; duplicate ID returned one result; cross-network ID reuse rejected: TASK_CONFLICT",
  "disaster_restore": {"cut": {"raw":5,"clean":3,"daily":3}, "read_matches_source": true},
  "restored_branch_base": {"point": "p2", "vector": {"raw":5,"clean":3,"daily":3}},
  "branch_isolation": "p3 and p2b share cut (6,4,4) but keep their own seq-6 receipts (raw-r vs raw-x)",
  "corruption": "corrupt and missing payloads rejected before target replacement; pre-existing target verified untouched",
  "prune": {"orphan_reclaimed": 280, "ancestor_retired_reclaimed": 0,
            "unreferenced_reclaimed": 818, "race": "restore accepted before retirement completed"}
}
```

证据要点：

- **可复现与自举**：同一源码两次 `build` 摘要相同；发布包 `ops build` 重建自身
  得到同一摘要（构建不动点为上述 sha256）。
- **真实进程终止**：`OPS_CRASH_AT=accept|object|manifest|restore-verify|restore-write|
  prune-delete` 在对应持久化边界向自身发送真实 SIGKILL（进程返回码 -9），重试后
  任务按持久状态完成；`manifest` 边界被杀的任务重试时不再重复捕获。
- **并发**：不同 ID 备份与恢复并行完成；同 ID 并发返回同一已提交结果；同任务号
  异请求（另一网络）拒绝为 `TASK_CONFLICT`。
- **灾备恢复**：删除源库并不再使用源码后，仅凭发布包副本与仓库恢复，`run read`
  输出与源端灾前一致；旧请求重放返回原回执，`REQUEST_CONFLICT` 仍生效；恢复点
  之后沿该点增量续跑，分支回执互不串扰。
- **回收**：空退役列表回收被杀任务残件（280 字节）；退役祖先点回收 0 字节且后代
  仍可恢复；退役无引用点实际释放其载荷（818 字节，等于对象文件大小）；进行中
  恢复在点被同时退役时仍成功；退役 ID 再备份/恢复分别报 `TASK_RETIRED`/`POINT_RETIRED`。

`python3 -B demo.py`（即清单 `demo`）用真实发布包完整演示：打包 → 业务加工 →
全量/增量备份 → 源库与源码丢失 → 仅凭包副本与仓库恢复 → 原回执重放 → 续跑 →
恢复分支增量备份 → 退役与拒绝，全部输出为真实命令结果。
