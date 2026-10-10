# Watch 变更通知修复总结

> 日期：2026-10-10 ｜ 分支：`fix/watch-notify`（基于 `apisix3` @ aa9f23a）｜ 状态：**已合并进 `apisix3`**（`1773363` fast-forward）
> 变更规模：12 个代码文件，+750 / −170；影响全部 4 个驱动（MySQL / PostgreSQL / SQLite / Redis）
> 修复分两批：第一批解决「收不到通知 + 伪 gap」（症状 1/2）；第二批解决「prev_kv 无条件附带导致的『冗余一条』显示」（症状 3，见 §2.3）

## 1. 问题背景

rhino 以数据库（kine 风格追加日志表）为存储层，对外模拟 etcd v3 服务；APISIX 将配置写入 rhino。典型使用场景：

```bash
# 终端 1：监听 APISIX 路由变化
etcdctl watch /apisix/routes --prefix
# 终端 2：APISIX 已连到 rhino，在 Admin 界面（或 curl）修改路由
```

暴露三个症状：

1. **watch 收不到通知**：修改路由后（变更确实写入了 rhino），`etcdctl watch` 长时间没有任何输出。
2. **日志报错**：`ERROR rhino::drivers::mysql: skipping revision gap at 1299, current event rev=1312`。
3. **watch 结果"多一条冗余"**：通知恢复后，同一次修改 etcdctl 打印出**两个** key/value 块（前一块是旧值），与原生 etcd 相同命令的输出对比"多了一条"。

## 2. 根因分析

### 2.1 症状一：watch 通知链路失效（三个叠加缺陷）

**(A) gRPC Watch 路径丢弃 `range_end`**

`etcdctl watch /apisix/routes --prefix` 的 gRPC `WatchCreateRequest` 携带 `key=/apisix/routes`、`range_end=prefixEnd("/apisix/routes")=/apisix/routeu`。旧实现 `Backend::watch(key, revision)` 直接丢弃 `range_end`，且驱动的 live 过滤规则是：

```rust
// 旧代码：key 以 '/' 结尾 → 前缀匹配；否则 → 精确相等
if check_prefix { e.kv.key.starts_with(&prefix) } else { e.kv.key == prefix }
```

`/apisix/routes` 不以 `/` 结尾 → 只认精确相等 → `/apisix/routes/probe1` 等事件全部被过滤丢弃，客户端永远收不到。

**(B) REST `/v3/watch` 解析的字段名不匹配**

APISIX 的 lua-resty-etcd 经 etcd grpc-gateway 走 REST watch，请求体为 snake_case：

```json
{"create_request": {"key": "...", "range_end": "...", "start_revision": "..."}}
```

旧解析只读取驼峰 `createRequest`，找不到该键就退化为「裸请求体」→ 所有字段解析为空（空 key、无 range_end）→ watch 了一个空键，同样什么都收不到。

**(C) `prev_kv` 序列化名不符**

etcd grpc-gateway 以 protojson `UseProtoNames` 输出 snake_case（`prev_kv`），lua-resty-etcd 删除事件处理也读 `event.prev_kv`；旧代码输出驼峰 `prevKv`，带 prevKv 的事件客户端取不到旧值。

### 2.2 症状二：init_dir 引发的伪 revision gap

kine 日志表的 revision 就是自增 `id`，poll 回路按「rev+1, rev+2…」连续性消费事件。而：

- APISIX 每次启动都会**无条件**重写 13 个资源目录的 `init_dir` 占位行（`apisix/cli/etcd.lua`），这是 etcd v2 的目录语义兼容；
- rhino 的 `after()` 查询会**显式过滤** value 为 `init_dir` 的行（APISIX 兼容补丁，避免占位行污染事件流）。

于是 poll 看到的是「一段隐藏的行跨度」，旧代码把它误判为「revision 丢行」，进入自愈循环：

```
发现跳跃 → 记录 skip → sleep 后 fill() 尝试重建
         → 该 id 的行其实已存在，INSERT IGNORE 无效
         → 超过 1s 仍填不上 → ERROR "skipping revision gap at N"
         → 落穿接受后续事件（自愈，但事件延迟 1–2 秒）
```

用户真实库（MySQL `apisix`.`kine`）的数据链完全吻合：

| id | 内容 | 可见性 |
|---|---|---|
| 1298 | rhino 启动时的游标 = MAX(id) | — |
| 1299–1311 | APISIX 启动写入的 13 行 init_dir | 对 `after()` 隐藏 |
| 1312 | 用户界面修改路由 | 第一条可见事件 → 触发误报 |

### 2.3 症状三：`prev_kv` 无条件附带（"冗余一条"）

驱动层 `row_to_event` / watch 回放对**所有** update/delete 事件都填入 `prev_kv`（即日志行的 `old_value` 列），而 etcd 的语义是：仅当客户端在 `WatchCreateRequest.prev_kv=true`（etcdctl `--prev-kv`）时才附带旧值。

于是同一次修改在 etcdctl 默认（不带 `--prev-kv`）下被打印为**两个** key/value 块——第一块是该事件的 `prev_kv`（旧值），第二块才是新值。用户对比原生 etcd 与 rhino、相同命令看到的"两条记录"实为**一个事件 + 不应出现的 prev_kv**，并非历史回放重复（沙箱实验证实：fresh watch 无历史回放；用户粘贴的两块内容分别对应真实库 id 1342（旧值，作为 prev_kv 打出）与 1356（新值））。

注意与 §2.1(C) 的区别：(C) 是 prev_kv **键名**不对（`prevKv` vs `prev_kv`），本节是 prev_kv **该不该出现**不受请求控制——两者叠加时 (C) 的修复反而让多余的 prev_kv 在 REST/etcdctl 输出里更可见了。

## 3. 修复内容

| 文件 | 改动 |
|---|---|
| `src/backend.rs` | `Backend::watch` 增加 `range_end` 参数；新增 `key_in_watch_range`（etcd 字节区间语义：`[start, range_end)`、`range_end == "\0"` 表示 `>= start` 开区间、空 `range_end` 为单键，并保留「key 以 `/` 结尾视为前缀」的 rhino 兼容约定）；新增 `common_prefix` 作为 SQL 预过滤的公共前缀 |
| `src/server/watch.rs` | gRPC Watch：将 `WatchCreateRequest.range_end` 透传给 backend；读取 `create.prev_kv` 并新增共享映射函数 `event_to_proto(e, want_prev_kv)`——仅当请求要求时才输出 `prev_kv`；新增 2 个门控单测 |
| `src/server/mod.rs` | `create_watch` 全链路透传 `range_end` 与 `prev_kv`；REST/gRPC 共用 `event_to_proto`，删除重复的内联事件构造 |
| `src/server/gateway.rs` | REST watch：`create_request`（snake_case）/ `createRequest`（驼峰）/ 裸请求体三种包裹均可解析；解析 `prev_kv`/`prevKv` 标志并透传；`event_to_json` 改为 protojson 语义——`prev_kv` 未设置时**整体省略该键**（设置时输出 snake_case `prev_kv`）；新增 2 个单测 |
| `src/drivers/mysql` `postgres` `sqlite` | ① `watch()` 按区间过滤：SQL 侧用 `LIKE common_prefix%` 只做粗筛（避免 SQL 排序规则与 etcd 字节序不一致），Rust 侧 `key_in_watch_range` 精确收敛；② poll 回路新增 `revisions_present(from, to)`（`SELECT COUNT(*) ... WHERE id >= ? AND id < ?`）作为 gap 分支的**第一条件**：id 全部存在只是被 `after()` 隐藏时，静默推进游标（trace `advancing past N hidden revisions ...`）；真正的丢行才走原有 skip/fill/ERROR 自愈；③ `Notify` 由每实例独立改为 `Arc<Notify>` 共享，且 `notified()` future 在查询**之前**注册，消除「查询在途时写入的唤醒丢失」（此前最多退化到 1s 轮询间隔） |
| `src/drivers/redis` | 同上；`revisions_present` 用 `ZCOUNT`；watch 历史回放与 live 流均使用 `key_in_watch_range` 过滤并去重 |
| `tests/*` | 适配 watch 新签名；新增区间语义测试 `test_watch_range_end_filters_events`（上界独享）；`backend.rs` 单元测试覆盖 etcdctl `--prefix`、APISIX `watchdir`（`/apisix/`→`/apisix0`）、开区间 `"\0"`、多字节前缀安全、以及「公共前缀粗筛不丢行」性质 |

设计取舍：修复不改变「过滤 init_dir」这一既有兼容行为（避免占位行污染客户端事件流），而是让 poll 回路能区分「隐藏但存在」与「真丢行」，代价为一次 `COUNT(*)` 查询。

## 4. 验证

### 4.1 单元 / 集成测试（worktree）

- `cargo test --lib`：16/16 通过（含 prev_kv 门控 4 条新守卫：`event_to_proto` 剥离/保留各 1 条、REST `event_to_json` 省略键 1 条、`parse_watch_create_request` 读取 prev_kv 标志 1 条）
- `cargo test --test sqlite_backend`：30 通过 + 2 个 pre-existing 失败（与本次修复无关，修复前即存在）

### 4.2 端到端演练（MySQL 沙箱 `rhino_watch_test`，未触碰真实库）

**Drill A —— 旧二进制复现**（同用户场景）：

- `etcdctl watch /apisix/routes --prefix` 输出为空（与症状 1 一致）
- 日志：`ERROR rhino::drivers::mysql: skipping revision gap at 3, current event rev=16`
  （沙箱 id 位移，机制与用户线上 `gap at 1299 rev=1312` 完全一致）

**Drill B —— 修复版同场景**：

- 容器重启写入 init_dir（id 17–29）后修改路由（id 30），日志：

  ```
  TRACE ...: revision gap: expected 17, got 30
  TRACE ...: advancing past 13 hidden revisions before rev 30
  ```

  即时完成、无 ERROR、无 1–2 秒停顿（对比修复前必现 `ERROR skipping revision gap`）；全程日志 0 条 ERROR。
- `etcdctl` 正常收到事件（含 POST 自动 id 路由）：

  ```
  PUT
  /apisix/routes/probe2
  {"id":"probe2","uri":"/probe2",...}
  PUT
  /apisix/routes/00000000000000000031
  {"id":"00000000000000000031","uri":"/probe3",...}
  ```

- `/apisix/gen_id`（rev 31）正确地**未**投递——区间过滤生效（它不属于 `/apisix/routes` 前缀）。

**Drill C —— prev_kv 门控（症状三，沙箱 12379 端口，克隆库）**：

| 用例 | 请求 | 结果 |
|---|---|---|
| V1 | `etcdctl watch` 默认（不带 `--prev-kv`），修改一次路由 | 仅打印**一个** key/value 块（新值）——与原生 etcd 行为一致 |
| V2 | `etcdctl watch --prev-kv` | 打印 prev（旧值）+ 新值，符合请求 |
| V3a | REST `/v3/watch`，请求体不带 `prev_kv` | 事件 JSON **无** `prev_kv` 键 |
| V3b | REST `/v3/watch`，`{"create_request":{"prev_kv":true,...}}` | 事件 JSON 含 `prev_kv`，旧值正确 |

另证实：沙箱 fresh watch（`start_revision=0`）不会回放历史事件——用户所见"第二条"确为同一事件的 prev_kv，而非重复通知。

## 5. 生效方式

修复必须运行**新构建的二进制**才生效（重启 rhino）：

1. 代码已合并进 `apisix3`（`1773363`，fast-forward）；在主仓库重新构建：
   `cargo build`，产物位于 `target/debug/rhino-server`；
2. 用新二进制重启 rhino 即生效（旧二进制仍会复现全部三个症状）。

## 6. 变更清单

```
src/backend.rs              | 135 ++++++++++++++++++++++++++++++++++-
src/drivers/mysql/mod.rs    | 112 ++++++++++++++++++++++-------
src/drivers/postgres/mod.rs | 113 +++++++++++++++++++++++-------
src/drivers/redis/mod.rs    |  59 +++++++++++-----
src/drivers/sqlite/mod.rs   |  81 ++++++++++++++++-----
src/server/gateway.rs       | 167 +++++++++++++++++++++++++++++++++++++++++---
src/server/mod.rs           |  38 +++-------
src/server/watch.rs         | 133 +++++++++++++++++++++++++++--------
tests/mysql_backend.rs      |   4 +-
tests/postgres_backend.rs   |   4 +-
tests/redis_backend.rs      |   8 +--
tests/sqlite_backend.rs     |  66 +++++++++++++++--
12 files changed, 750 insertions(+), 170 deletions(-)
```

注：本分支非 rustfmt 风格，请勿运行 `cargo fmt`（会产生无关格式化噪音）。
