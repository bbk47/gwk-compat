# gwk-compat

跨实现的端到端**兼容性 + 稳定性**测试工具，用于验证 [`gwk`](https://github.com/xuxihai123/gwk)（Go 实现）与 `gwkjs`（Node/TypeScript 实现）在真实隧道流量下的互通性。

核心目的是验证两端多路复用层的互操作：Go 端基于 `hashicorp/yamux`，Node 端基于 [`@bbk47/yamux`](https://github.com/bbk47/yamux)。本工具以"启动真实进程、跑真实 TCP 隧道"的方式，确保它们在小包、大包（跨帧分片）、以及高并发长压测下都能正确、稳定地互通。

> 本仓库只包含测试脚本与方案说明，**不包含任何二进制**。被测的 `gwk` / `gwkjs` 由脚本在运行时自行编译（或复用已编译产物）。

---

## 方案（Scheme）

### 总体架构

工具针对 server × client 的每一种实现组合，搭建一条完整的真实链路：

```
         echo 回显服务 (127.0.0.1:localPort)
              ▲
              │ 解隧道后转发到本地目标
        ┌─────┴──────┐  yamux over raw TCP   ┌────────────┐
        │  gwk server │  (控制连接 :controlPort) │  gwk client │
        │  (gwkd)     │ ◀───────────────────── │  (gwk)     │
        └─────┬──────┘                         └────────────┘
              │ 对外暴露隧道入口 :remotePort
              ▼
        测试客户端（本工具）发起 echo 往返
```

每个场景的执行步骤：

1. 启动一个本地 **echo 回显服务**（隧道的转发目标）。
2. 以指定实现启动真实的 **gwk server 进程**（`gwkd`）。
3. 以指定实现启动真实的 **gwk client 进程**（`gwk`），建立一条到 echo 服务的 TCP 隧道。
4. 跑一轮**正确性冒烟测试**（小包 + 大包往返）。
5. 跑一轮**高并发长压测**（N 条持久连接循环收发，持续 DURATION 秒），采集时延 / 吞吐 / 错误率指标。
6. 清理进程并释放端口，进入下一个场景。

### 测试矩阵（Matrix）

默认跑全部 4 种组合，覆盖同实现与跨实现两类场景：

| 场景 | server | client | 主要验证点 |
|------|--------|--------|-----------|
| `go-go` | Go | Go | Go 基线（hashicorp/yamux 自身） |
| `js-js` | Node | Node | Node 基线（@bbk47/yamux 自身） |
| `go-js` | Go | Node | **跨实现**：Go 服务端 ↔ JS 客户端 |
| `js-go` | Node | Go | **跨实现**：JS 服务端 ↔ Go 客户端 |

`go-js` 与 `js-go` 是真正考验 yamux 线格式 / 状态机互通的关键场景；`go-go`、`js-js` 作为对照基线，用于判断偶发问题是"实现兼容性"还是"环境抖动"。

### 实现源码定位

被测实现来自两个独立仓库，工具在运行时编译它们：

- **gwk (Go)**：`go build` 出 `gwkd`（server）与 `gwk`（client）。
- **gwkjs (Node)**：`npm run build`（tsc → `lib/`），运行 `lib/cli.js`。

通过参数或环境变量指定源码目录（否则按约定路径自动探测）：

```bash
node run-compat.mjs --gwk-dir=/path/to/gwk --gwkjs-dir=/path/to/gwkjs
# 或
GWK_DIR=/path/to/gwk GWKJS_DIR=/path/to/gwkjs node run-compat.mjs
```

自动探测顺序：本仓库的同级目录 `../gwk`、`../gwkjs` → 上一级 → 当前工作目录及其上级。找不到会给出明确报错与已搜索路径。

---

## 测试内容（What is tested）

### 1. 正确性冒烟（smoke）

在隧道就绪后，用单次往返验证不同大小 payload 的字节完整性：

- `16 B` —— 最小包
- `1 KB`
- `64 KB`
- `1 MB` —— **超过单次 socket read 的大帧，强制触发 yamux 跨 chunk 分片重组**

任一大小回显字节不一致或超时即判定 smoke 失败。1 MB 这档专门用于暴露"数据帧跨读丢失/死锁"这类历史问题。

### 2. 高并发稳定性压测（load）

- N 条（默认 50）**持久连接**并发，循环执行 echo 请求/响应，持续 DURATION 秒（默认 30s）。
- 每条消息默认 `4096 B`，头部写入 `worker id + seq`，回显后逐字节比对，可检测**数据损坏 / 乱序 / 串流**。
- 任一请求超时（默认 8s）、回显不匹配或连接异常都会被计为 `fail`，并触发自动重连（计入 `reconnects`）。
- 采集指标：总请求数、失败数、重连数、req/s、吞吐 Mb/s、平均时延、p50 / p90 / p99 / max。

### 判定标准

每个场景需满足：smoke 通过 **且** 压测期间 `requests > 0` **且** `failures == 0`。
全部场景通过则整体 `RESULT: PASS`。

> 偶发的 `request timeout`（max 时延贴近 8s 阈值）通常是高并发下的 GC/调度抖动，可在 `go-go` 基线场景同样出现——这类属于环境抖动而非兼容性问题。降低并发或放宽超时复跑即可区分。

---

## 使用方法

需要本机具备 **Go**（编译 gwk）与 **Node.js ≥ 18**（运行 gwkjs 与本工具）。

```bash
# 默认：4 个场景，各 50 并发 × 30s，4KB 消息
node run-compat.mjs

# 常用脚本
npm run compat          # 同上
npm run compat:quick    # 10s × 20 并发，快速验证
npm run compat:js       # 只跑涉及 JS 的场景 (js-js, go-js, js-go)
```

### 参数

| 参数 / 环境变量 | 默认值 | 说明 |
|----------------|--------|------|
| `--duration=` / `DURATION` | `30` | 每个场景压测时长（秒） |
| `--concurrency=` / `CONCURRENCY` | `50` | 并发持久连接数 |
| `--msg-size=` / `MSG_SIZE` | `4096` | 压测单条消息字节数 |
| `--matrix=` / `MATRIX` | `go-go,js-js,go-js,js-go` | 要跑的场景组合 |
| `--skip-build` / `SKIP_BUILD=1` | 关闭 | 复用上次编译产物（`.bin/` + `gwkjs/lib`） |
| `--gwk-dir=` / `GWK_DIR` | 自动探测 | gwk (Go) 源码目录 |
| `--gwkjs-dir=` / `GWKJS_DIR` | 自动探测 | gwkjs (Node) 源码目录 |

示例：

```bash
# 只验证跨实现，跑 60s、100 并发、64KB 大消息
node run-compat.mjs --matrix=go-js,js-go --duration=60 --concurrency=100 --msg-size=65536

# 复用已编译产物，快速重跑
node run-compat.mjs --skip-build
```

### 退出码

- `0` —— 全部场景通过
- `1` —— 有场景失败（详见报告末尾的错误明细与日志摘要）
- `2` —— 致命错误（如定位不到实现、编译失败）

---

## 输出示例

```
=================== COMPATIBILITY + STABILITY REPORT ===================
config: concurrency=50  duration=30s  msg=4096B

scenario  server  client  smoke  requests  fail  recon  req/s   Mb/s     avg ms  p50   p99   max
--------  ------  ------  -----  --------  ----  -----  ------  -------  ------  ----  ----  -----
go-go     go      go      OK     485,544   0     0      16,185  1,011.6  3.01    2.07  5.02  ...
js-js     js      js      OK     464,925   0     0      15,498    968.6  3.19    2.03  7.97  ...
go-js     go      js      OK     486,901   0     0      16,230  1,014.4  3.07    1.98  6.29  ...
js-go     js      go      OK     451,375   0     0      15,046    940.4  3.38    1.92  6.29  ...

RESULT: PASS — all scenarios stable, zero failed requests.
```

运行产生的临时配置与各进程日志保存在系统临时目录（`gwk-compat-*`），报告末尾会打印具体路径，便于失败时排查。

## License

ISC
