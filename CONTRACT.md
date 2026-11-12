# 三库水质加工项目：现有契约与调用格式

## 运行环境与现有入口

部署验收环境是 macOS 的普通用户目录、本机 APFS，Python 3.9 或以上及其标准库 SQLite；arm64 与 x86_64 都运行同一份纯 Python 源码。不需要编译器、第三方依赖、联网、账号或常驻系统服务。解释器由验收机提供，不要求捆绑解释器。允许使用普通文件、SQLite、子进程和用户文件锁；不要求对突然断电的硬件行为作保证，但必须处理真实进程被终止后的持久状态。运行中各方都通过本项目入口访问库，不考虑恶意绕过入口修改数据库。

已有业务调用（每一行是独立 argv，不是 shell 脚本）：

python3 -B run.py init work/water sample-water
python3 -B run.py load work/water raw-1 samples/initial.json
python3 -B run.py clean work/water clean-1 2
python3 -B run.py daily work/water daily-1
python3 -B run.py load work/water raw-2 samples/correction.json
python3 -B run.py clean work/water clean-2 2
python3 -B run.py daily work/water daily-2
python3 -B run.py ack work/water review-2
python3 -B run.py read work/water
python3 -B run.py trim work/water raw 1

init 的 root 必须不存在；network 是非空业务网络名称。其余调用 root 已存在。root 包含 network.json 及 raw.sqlite、clean.sqlite、daily.sqlite，运行时可有 SQLite WAL/SHM 文件。三个库分别提交，没有共同事务。load、clean、daily、ack、trim 可以在不同进程执行，单个业务写事务由 SQLite 排他协调。原有 SQLITE_BUSY 可由调用方重试。

run.py 和 entry.py 都有 main()。已有 python3 -B entry.py run demo 可以直接运行。entry.py 只负责组件分派，当前仓库只有 run 组件，不含 ops 实现。

所有业务成功调用 stdout 为一个 JSON 对象。load、clean、daily、ack 返回 {"node":节点,"seq":整数,"request":请求号}。read、init 返回节点名到 snapshot 的映射；trim 返回该节点当前 snapshot。已处理的 ValueError/OSError 输出 {"error":错误说明} 并退出 2。demo 用内存库输出实际修订前后的完整状态，不写文件。

## 数据、帧与回执

load 输入为 UTF-8 JSON 数组，每项恰含 id、rev、station、day、value、deleted。id、station 是非空字符串，rev 是正整数，day、value 是整数，deleted 是布尔值；布尔值不算整数。同批 id 不重复。按 id 保留最高 rev，低修订忽略，同修订同内容不变，同修订不同内容抛 REVISION_CONFLICT 并整批回滚。墓碑留在 raw 中。clean 读取一个 raw 快照，过滤墓碑，将 value 乘整数 factor；daily 读取一个 clean 快照按 station、day 求 total、count。ack 记录采集端已审定某个 daily 快照，原始观测行不改变，但推进 raw 帧并带入 daily 的依赖。

每个库有 identity(singleton,network,node,head)、frames(seq,clock,data)、receipts(request,digest,seq)。frames.data 是该节点在此帧的完整行映射，不是增量。初始 seq=0、data={}，clock 三个分量均为 0。每个新请求提交推进本库 seq 一次，即使内容不变。clock 为累计依赖向量：合并上一帧及本次读取的外库快照向量，再将自身分量设为新 seq。snapshot={"seq":整数,"clock":{"raw":整数,"clean":整数,"daily":整数},"rows":行映射}。data 和 clock 保存为 canonical JSON 文本。

request 在节点内唯一。receipt.digest 是 pipeline.digest(payload)，payload 由现有方法定义。相同 request、相同 payload 返回第一次的回执，优先于任何重新计算；不同 payload 抛 REQUEST_CONFLICT。回执与帧、head 同事务持久化。不同请求即使业务行相同，也有各自帧及回执。

trim NODE KEEP 保留该库最后 KEEP 个帧，不删除回执；KEEP 至少为 1。已裁掉的 snapshot 抛 HISTORY_GONE。不能要求业务改用不裁剪模式。可为运维添加元数据、索引及历史保护协调钩子，保护结束后 trim 仍须删除不再受保护且超出 KEEP 的历史；不得改业务计算、请求绑定及提交语义。

samples.py 与 samples/ 两份样例内容一致。初始 factor=2 时 east/day0 为 total=30,count=2，west/day0 为 total=14,count=1。应用 correction 并重新 clean、daily 后，east/day0 为 10/1，west/day1 为 -6/1，west/day0 消失。追加 ack 后 raw 的 clock 为 {"raw":3,"clean":2,"daily":2}，行内容仍是修订后的原始观测。checks.py 保留了这些断言、回滚和裁剪后回执重放断言。

## 题面所用的精确定义

一次捕获是对某个库的一致读视图，可取得多个仍存在的帧及相应回执；不是把读到的不同帧临时拼成一帧。捕获证据须记录来源身份、每库捕获上界、接纳下界和候选帧的 seq/clock/内容摘要，可自行选择证据文件布局。

候选集由该任务实际捕获的帧和所选增量祖先已经保存的帧构成。恢复截点 c=(raw,clean,daily) 必须为每个库选择一个候选帧，且该帧的 clock 在所有分量上都不超过 c。所谓逐分量最大，是该可行向量逐分量不小于同一候选集的其他可行向量，不能用总和最大代替，也不能凭空回放制造候选帧。接纳下界是任务首次持久化接纳记录时，已分别读取并固定的各库 head；备份不可反复沿用旧恢复点而不推进。任务接纳记录必须先于该任务任何业务载荷的读取或复制落盘；不能将接纳延迟到载荷复制完以后。新任务可重新捕获，无无限写入时必须取得进展；不承诺在无限对抗性调度下有固定完成时限。

checks.py 中的真实交错是 raw=3 已捕获，随后 raw=4、clean=3、daily=3 提交；daily=3 的 raw 依赖为 4。三个库单独完整且可打开，组合仍不合法。ack 又使类型依赖形成 raw→clean→daily→raw，固定一次复制顺序并不构成一致性保证。

源端新增是相对该任务选定的、已经提交的增量基点各库捕获上界而言，尚未备份的帧及回执。已完成基点的数据不允许重新全量读出再过滤；可以读取 identity、运维记录、索引元数据和仓库依赖清单，不能从祖先载荷重扫旧业务数据来规避限制。首次全备可以读取当时保有的历史及全部回执。恢复和完整性校验需要读取祖先载荷，不受备份源端增量读取限制。

对应截点的恢复结果包括选定各库的完整 rows、clock、head，及该分支截至各库所选 seq 的全部原始 receipts；晚于截点的回执不得带入。恢复可只保留所选 head 帧，等价于正常 trim(1)，不得只保存 daily 结果或重算后生成全新的回执。恢复后的旧 request 必须返回原回执，新的加工可继续接在这些 seq 后。network 不变；运维分支身份可新增，不能将恢复点之后另一分支的同 seq 数据或回执带过来。

## 运维调用和返回格式

以下是题面指定入口的参数及结果格式，不是现有实现。ID 是仓库内永久唯一的非空任务号；路径都是普通路径，不是 URL。各命令成功退出 0 并只输出一个 JSON 对象，失败退出非零且输出 {"error":非空说明}；不规定错误码词表或内部文件名。允许额外 JSON 字段。

python3 -B ops.py build OUT
返回 {"artifact":OUT,"sha256":发布包字节的SHA-256小写十六进制}。OUT 是单文件 Python 可运行发布包：python3 -B OUT run <既有业务参数> 和 python3 -B OUT ops <上述运维参数>。构建输入是相同源码及固定发布元数据时，字节及摘要相同；不得把构建目录、时间或业务库混入。相同包应能复制到另一普通目录，在无源码及无 PYTHONPATH 注入的环境运行。无需捆绑解释器或制作安装器。

python3 -B ops.py backup ROOT REPO ID
返回 {"id":ID,"network":网络名称,"cut":{"raw":整数,"clean":整数,"daily":整数}}。REPO 不存在则创建。新 ID 绑定 ROOT 的业务网络和运维分支身份，而不是绑定会不断变化的 head；重复调用返回原已提交结果，未提交任务按其持久状态继续。改用另一网络或分支复用 ID 必须拒绝。source root 路径搬迁不改变已持久化身份。任务的接纳、成功与引用证据须可通过仓库中交付的证据定位，布局自行设计。

python3 -B ops.py restore REPO ID DEST
返回 {"id":ID,"root":DEST,"cut":与恢复点相同的向量}。DEST 是不存在的路径或不在运行的现有业务根目录；不要求替换运行中的库。任一非本命令错误产生的已有目标都不得在验证失败时受损。成功结果在后续 run.py read DEST 中可核对。重复执行可恢复其未完成操作；同时操作同一 DEST 可通过目标级协调解决。不同任务（包括同源不同 ID 的备份）、不同恢复目标以及回收之间不能由跨载荷 I/O 的全局锁串行化。源库和原源码可以在恢复前完全不可用，恢复只依赖发布包和备份仓库。恢复后的增量基点就是 ID，不能默认取仓库最新点。

python3 -B ops.py prune REPO DROP.json
DROP.json 是明确要退役的已提交恢复点 ID 的 JSON 数组，例如 ["old-point"]；未列出的恢复点，包括并发新发布点，必须保留。返回 {"retired":已退役ID的排序数组,"reclaimed_bytes":本次实际删除载荷的字节数}。重复退役须幂等。退役一个祖先恢复点不等于允许删除后代需要的载荷；已接纳恢复也不能因其点被同时退役而读失败。退役后的 ID 不得重新用于备份。空数组只做安全回收。回收字节按实际载荷文件计，不得以删除无关日志、伪造计数或永久跳过所有对象代替；对象和增量格式自行设计。

## 上线回归与交付边界

.pointwise-runtime.json 的 setup 是 argv 数组列表，verify/demo 是单个 argv 数组。保留原业务断言；最终 verify 应实际重算业务和交付的上线证据，demo 应展示真实发布包恢复结果，不只是打印成功。交付说明给出可直接重跑的打包、复制发布、备份、目标恢复、恢复后继续加工及回收入口，并关联实际包摘要、截点、恢复输出和异常证据。具体并发、杀进程时序及证据布局由实施者设计；必须包含真实进程终止，不能把异常退出本身当作正确性证明。本轮实现运维交付，不新写另一套加工器，不提供通用文件备份或文件同步产品。
