# Yisync 实现细节

本文档给一个从零开始读代码的人使用。它不只记录“做了什么”，还解释这些代码为什么这样拆、消息怎样流动、状态怎样推进、失败怎样恢复。

相关文档：

- [README.md](README.md)：怎么构建、怎么运行、当前状态总览。
- [protocol.md](protocol.md)：线上协议字段和协议规则。
- [todo.md](todo.md)：后续待办事项。
- 本文档：代码实现细节和读代码路线。

## 0. 先理解这件事

Yisync 当前是一个 C++20 同步原型。它要做的事情是：

```text
A 端 Sender 读源目录
B 端 Receiver 上报目标目录状态
Sender 比较两边目录差异
Sender 把缺失或追加的数据发给 Receiver
Receiver 写入目标目录
断线后 Sender 不依赖本地持久化状态，而是重新看 Receiver 的 MANIFEST
```

当前不是完整产品，重点是把核心链路跑通：

- 小文件同步。
- 大文件分块同步。
- 多 TCP line 分发 chunk。
- 限速和背压。
- 断线重连和未确认 chunk 重排。
- Receiver 端 chunk checkpoint 和重启恢复。
- Receiver 端后台 disk writer，避免 event loop 被 fsync / rename / CRC32C 卡住。
- 目录、子目录、空目录、软链接同步。

## 1. 最小运行方式

构建：

```bash
cmake --build build-cpp20
```

跑单进程综合 demo：

```bash
./build-cpp20/yisync_demo
```

跑真实 A/B 进程：

```bash
./build-cpp20/yisync_node receiver \
  --host 127.0.0.1 \
  --base-port 19000 \
  --lines 2 \
  --root /tmp/yisync_receiver
```

另一个终端：

```bash
./build-cpp20/yisync_node sender \
  --host 127.0.0.1 \
  --base-port 19000 \
  --lines 2 \
  --source-root /tmp/yisync_source
```

模拟数据也可以：

```bash
./build-cpp20/yisync_node sender \
  --host 127.0.0.1 \
  --base-port 19000 \
  --lines 2 \
  --size 153600
```

测试断线重连：

```bash
./build-cpp20/yisync_node sender \
  --host 127.0.0.1 \
  --base-port 19000 \
  --lines 2 \
  --size 2097152 \
  --drop-line-once 1
```

## 2. 核心词汇

### Sender / Receiver

```text
Sender   = A 端，源端，读取源目录并发送消息。
Receiver = B 端，目标端，上报目录状态并写目标目录。
```

代码里：

- Sender 进程在 [src/yisync_sender_app.cpp](src/yisync_sender_app.cpp)。
- Receiver 进程在 [src/yisync_receiver_app.cpp](src/yisync_receiver_app.cpp)。
- 入口在 [src/yisync_node.cpp](src/yisync_node.cpp)。

### Stream

`stream` 表示一个同步目录。一个 stream 内必须严格保持文件边界顺序：

```text
entry 1 没完成，entry 2 不能对外可见
entry 2 没完成，entry 3 不能对外可见
```

当前约定：

- 默认 stream id 是 `9001`。
- 如果 `--source-root` 下有数字子目录，例如 `1/`、`2/`，它们会被当成 stream id。
- 默认 stream 写到 receiver root。
- 其他 stream 写到 receiver root 下的 `<stream_id>/`。

例子：

```text
source-root/
  a.txt
  sub/b.txt

=> stream 9001
=> receiver-root/a.txt
=> receiver-root/sub/b.txt
```

```text
source-root/
  1/a.txt
  2/b.txt

=> stream 1 写 receiver-root/1/a.txt
=> stream 2 写 receiver-root/2/b.txt
```

### seq 和 order_seq

代码里有两个顺序号，容易混：

```text
seq       = append 模式用，CREATE / DATA 严格递增。
order_seq = chunk 模式用，FILE_BEGIN / FILE_COMMIT 的文件级顺序。
```

小文件和 append 走 `seq`。

大文件 chunk 走 `order_seq`，同一个文件内部的 `CHUNK` 可以乱序，但 `FILE_BEGIN` 和 `FILE_COMMIT` 必须按 `order_seq`。

### MANIFEST

`MANIFEST` 是 Receiver 告诉 Sender：“我现在目录里有什么”。

连接建立后，Receiver 会扫描目标目录并发送 `MANIFEST`。Sender 收到后比较源目录和目标目录，决定：

- 已一致，跳过。
- Receiver 缺文件，创建。
- Receiver 文件较短，append 续传。
- Receiver 有未完成 chunk，跳过已 checkpoint chunk，只发缺失 chunk。
- Receiver 内容冲突，停止。

### HEARTBEAT 和 NACK

成功路径不逐条 ACK。Receiver 用 `HEARTBEAT` 批量告诉 Sender：

```text
next_seq           下一个期待的 seq/order_seq
file_id            当前文件
offset             当前已接受的 offset
durable_offset     已 fsync 的 offset
recv_window_bytes  Receiver 当前窗口
received_chunks    已收到的 chunk
missing_ranges     Receiver 观察到的 chunk 缺口
```

失败路径用 `NACK`。

### checkpoint 和 durable

`received` 不等于 `durable`：

```text
received = 已经写入 .tmp 或 page cache，适合低延迟流控。
durable  = 已经通过 fsync / .meta checkpoint，可用于重启恢复。
```

Receiver chunk 模式维护两个 bitmap：

```text
received     已收到并写入临时文件。
checkpointed 已写入 .meta，进程重启后能恢复。
```

## 3. 代码地图

### 协议层

| 文件 | 作用 |
| --- | --- |
| [include/yisync_protocol.hpp](include/yisync_protocol.hpp) | wire 消息结构、枚举、Frame、CRC32C 接口 |
| [src/yisync_protocol.cpp](src/yisync_protocol.cpp) | frame 编解码、消息 body 编解码、CRC32C |

这层只做线上协议，不应该放 sender/receiver 状态机。

### 同步和 manifest 层

| 文件 | 作用 |
| --- | --- |
| [include/yisync_sync.hpp](include/yisync_sync.hpp) | manifest scan、diff、chunk 策略接口 |
| [src/yisync_sync.cpp](src/yisync_sync.cpp) | 扫描目录、读取 `.meta`、比较 manifest、文件 checksum |

这层负责“本地目录状态”和“比较差异”。

### 源目录层

| 文件 | 作用 |
| --- | --- |
| [include/yisync_source.hpp](include/yisync_source.hpp) | `SourceDirectory`、watcher 抽象 |
| [src/yisync_source.cpp](src/yisync_source.cpp) | 源目录扫描、范围读取、全文件 checksum、polling watcher |

这层负责从源目录读真实文件。

### Receiver 写盘状态机

| 文件 | 作用 |
| --- | --- |
| [include/yisync_receiver.hpp](include/yisync_receiver.hpp) | `ReceiverStream`、`ChunkedReceiverStream` 接口 |
| [src/yisync_receiver.cpp](src/yisync_receiver.cpp) | CREATE/DATA、FILE_BEGIN/CHUNK/FILE_COMMIT 的写盘逻辑 |

这层负责把协议消息应用到本地文件系统。

### Scheduler

| 文件 | 作用 |
| --- | --- |
| [include/yisync_scheduler.hpp](include/yisync_scheduler.hpp) | token bucket、多线路调度接口 |
| [src/yisync_scheduler.cpp](src/yisync_scheduler.cpp) | 限速、窗口、in-flight、线路健康评分 |

这层负责“哪条 TCP line 能发、发多少”。

### 网络层

| 文件 | 作用 |
| --- | --- |
| [include/yisync_transport.hpp](include/yisync_transport.hpp) | 阻塞 transport 抽象和 memory/TCP 接口 |
| [src/yisync_transport.cpp](src/yisync_transport.cpp) | memory transport、阻塞 TCP transport |
| [include/yisync_async.hpp](include/yisync_async.hpp) | event loop、异步 TCP frame connection |
| [src/yisync_async.cpp](src/yisync_async.cpp) | `poll` event loop、非阻塞 TCP、frame 拆包组包 |

当前真实 sender/receiver 进程使用异步 TCP。

### Node 应用层

| 文件 | 作用 |
| --- | --- |
| [include/yisync_node_common.hpp](include/yisync_node_common.hpp) | node 常量、命令行参数、构造消息工具函数 |
| [src/yisync_node_common.cpp](src/yisync_node_common.cpp) | 参数解析、chunk 构造、DATA 构造、fsync helper |
| [include/yisync_node_apps.hpp](include/yisync_node_apps.hpp) | `run_sender()` / `run_receiver()` 声明 |
| [src/yisync_sender_app.cpp](src/yisync_sender_app.cpp) | Sender 进程主逻辑 |
| [src/yisync_receiver_app.cpp](src/yisync_receiver_app.cpp) | Receiver 进程主逻辑 |
| [src/yisync_node.cpp](src/yisync_node.cpp) | main 入口，按 mode 调 sender 或 receiver |
| [src/main.cpp](src/main.cpp) | 单进程 demo 和验证用例 |

## 4. 协议层怎么工作

### MessageHeader

在 [include/yisync_protocol.hpp](include/yisync_protocol.hpp)：

```text
MessageHeader:
  magic       u32
  version     u8
  msg_type    u8
  header_len  u16
  body_len    u32
```

固定 12 字节。TCP 是字节流，不保留消息边界，所以每个 frame 必须带 `body_len`，接收端才能知道一条消息什么时候收完整。

### MessageType

当前消息：

```text
Hello
Manifest
Create
Data
Heartbeat
Nack
FileBegin
Chunk
FileCommit
```

已经删除：

- `Ping`
- `Goaway`
- `flags`
- `CreateMode`
- `ChecksumScope`

### Message variant

代码里用：

```cpp
using Message = std::variant<Hello,
                             Manifest,
                             Create,
                             Data,
                             FileBegin,
                             Chunk,
                             FileCommit,
                             Heartbeat,
                             Nack>;
```

发送时：

```text
Message -> encode_message() -> Frame -> encode_frame() -> Bytes
```

接收时：

```text
Bytes -> decode_frame() -> Frame -> decode_message() -> Message
```

实现位置：

- `encode_message()`
- `decode_message()`
- `encode_frame()`
- `decode_frame()`

都在 [src/yisync_protocol.cpp](src/yisync_protocol.cpp)。

### checksum

`FileChecksum`：

```text
algo
offset
len
value
```

范围校验和整文件校验统一表达：

```text
范围校验: offset = 某个位置, len = 范围长度
整文件:   offset = 0, len = file_size
```

当前实际实现的是 CRC32C，使用 Google `crc32c`。

## 5. Manifest 和 diff

相关文件：

- [include/yisync_sync.hpp](include/yisync_sync.hpp)
- [src/yisync_sync.cpp](src/yisync_sync.cpp)

### ManifestEntry

`ManifestEntry` 表示已经在最终目录中可见的 entry：

```text
file_id
order_seq
kind
name
link_target
size
checksum
```

`kind` 有三种：

```text
RegularFile
Directory
Symlink
```

`name` 是 stream root 下的安全相对路径，例如：

```text
1.file
sub/2.file
emptydir
sub/link_to_root
```

Receiver 会拒绝：

- 空路径。
- 绝对路径。
- 包含 `..` 的路径。

### file_id 怎么来

当前兼容旧格式：

```text
1.file -> file_id = 1
2.file -> file_id = 2
```

对子目录、目录、软链接等，使用相对路径生成稳定 file_id。

注意：现在文件顺序不靠 `file_id + 1`，而靠 `order_seq`。

### scan_manifest_stream()

`scan_manifest_stream()` 做两件事：

1. 扫描最终目录，生成 `entries`。
2. 扫描 `.yisync_tmp/*.meta`，生成 `incomplete_chunks`。

最终文件进入：

```text
ManifestStream.entries
```

未完成 chunk 进入：

```text
ManifestStream.incomplete_chunks
```

### IncompleteChunkFile

`IncompleteChunkFile` 是 Receiver 重启后恢复大文件 chunk 的关键：

```text
order_seq
file_id
name
final_size
chunk_size
chunk_count
file_checksum
prev_file_id
prev_final_size
prev_checksum
received_chunks
```

这里的 `received_chunks` 不是低延迟 heartbeat 里的 received，而是 `.meta` 中 checkpoint 后的 bitmap。也就是“进程重启后还能相信”的状态。

### diff_stream()

Sender 收到 Receiver `MANIFEST` 后，用 `diff_stream()` 比较 source 和 receiver。

结果：

```text
nullopt:
  已完全一致，不需要传。

SyncStart:
  从某个 file_id 和 offset 开始传。
```

`SyncStart`：

```text
stream_id
start_file_id
start_offset
start_action
```

`StartAction`：

```text
ResumeExisting = Receiver 已有最终文件，从 offset append
CreateMissing  = Receiver 缺 entry，从头创建
```

`StartAction` 不是 wire 协议字段，它只是 Sender 本地 diff 结果。

## 6. SourceDirectory

相关文件：

- [include/yisync_source.hpp](include/yisync_source.hpp)
- [src/yisync_source.cpp](src/yisync_source.cpp)

`SourceDirectory` 是 Sender 读取真实源目录的接口。

常用方法：

```text
scan_manifest()
  扫描源目录，得到 ManifestStream。

files()
  返回源目录中可同步的 SourceFile 列表。

read_range(file_id, offset, len)
  从真实文件读取一段内容，用于 DATA 或 CHUNK。

full_checksum(file_id)
  计算整文件 CRC32C，用于大文件 commit 校验。
```

`SourceFile`：

```text
file_id
path
manifest
```

### watcher 抽象

`ISourceWatcher` 已经定义：

```text
poll() -> vector<WatchEvent>
```

事件类型：

```text
Created
Appended
Modified
Removed
```

当前只有 polling fallback。Linux `inotify` 和 macOS `FSEvents` 还没接。

## 7. ReceiverStream：小文件和 append

相关文件：

- [include/yisync_receiver.hpp](include/yisync_receiver.hpp)
- [src/yisync_receiver.cpp](src/yisync_receiver.cpp)

`ReceiverStream` 处理：

```text
CREATE
DATA
```

它适合：

- 缺失小文件。
- 目录创建。
- 软链接创建。
- 已有最终文件 append 续传。

### ReceiverStreamState

```text
active
expected_seq
current_file_id
current_offset
next_create_file_id
```

关键点：

- `expected_seq` 是下一个应该收到的 `CREATE/DATA` 序号。
- `current_file_id` 是当前正在写的文件。
- `current_offset` 是当前文件已经写到哪里。

### apply(Create)

`ReceiverStream::apply(const Create&)` 做这些检查：

```text
stream_id 正确
seq == expected_seq
kind 是 RegularFile / Directory / Symlink
name 是安全相对路径
如果有 prev_file_id，则前一个文件必须存在、size/checksum 匹配
目标路径必须不存在
```

通过后：

```text
RegularFile: 创建空文件
Directory:   创建目录
Symlink:     创建软链接本身
expected_seq += 1
current_file_id = create.file_id
current_offset = 0
current_path = 目标路径
```

### apply(Data)

`ReceiverStream::apply(const Data&)` 做这些检查：

```text
stream_id 正确
seq == expected_seq
file_id == current_file_id
当前 entry 必须是 RegularFile
offset == 目标文件当前大小
raw_len == payload 解压后长度
DATA CRC32C 正确
```

通过后：

```text
以 append 方式写目标文件
expected_seq += 1
current_offset = offset + raw_len
```

### append durable

`DATA` 写完后，不代表已经掉电安全。ReceiverApp 会把该文件 fsync 投递给后台 disk writer。

Heartbeat 中：

```text
offset:
  已接受并写入的 offset。

durable_offset:
  后台 fsync 完成后的 offset。
```

Sender 判断 append 文件完成时，不只看 `next_seq`，还要求：

```text
durable_offset >= source_size
```

## 8. ChunkedReceiverStream：大文件分块

相关文件：

- [include/yisync_receiver.hpp](include/yisync_receiver.hpp)
- [src/yisync_receiver.cpp](src/yisync_receiver.cpp)

`ChunkedReceiverStream` 处理：

```text
FILE_BEGIN
CHUNK
FILE_COMMIT
```

适合：

```text
缺失文件且 final_size > 64KB
```

append 续传即使增量超过 64KB，也不切到 chunk 模式。因为 Receiver 已经有最终文件，安全语义是继续 append，而不是把最终文件搬回 `.yisync_tmp`。

### ActiveFile

`ChunkedReceiverStream::Impl::ActiveFile` 是一个正在接收的大文件：

```text
order_seq
file_id
name
final_size
chunk_size
chunk_count
file_checksum
prev_file_id
prev_final_size
prev_checksum
temp_path
final_path
received bitmap
checkpointed bitmap
received_count
checkpointed_count
pending_checkpoint_bytes
commit_pending
```

其中：

```text
temp_path  = .yisync_tmp/<order_seq>_<file_id>.file.tmp
meta_path  = temp_path + ".meta"
final_path = receiver root 下的安全相对路径
```

### apply(FileBegin)

`FILE_BEGIN` 是大文件的开始。

检查：

```text
stream_id 正确
order_seq == expected_order_seq
final_size 必须进入 chunk 模式
chunk_size == 64KB
chunk_count 正确
目标最终文件不存在
prev_file_id 如果存在，则前一个文件 size/checksum 匹配
```

通过后：

```text
创建 .tmp 临时文件
写一份初始 .meta
active[order_seq] = ActiveFile
```

### apply(Chunk)

`CHUNK` 是文件的一段数据。

检查：

```text
stream_id 正确
order_seq == expected_order_seq
FILE_BEGIN 已经存在
file_id 匹配
chunk_index 在范围内
offset == chunk_index * chunk_size
raw_len 是该 chunk 应有长度
payload CRC32C 正确
```

通过后：

```text
seek 到 offset
写入 .tmp
received[chunk_index] = true
received_count += 1
pending_checkpoint_bytes += raw_len
```

重复 chunk 会被幂等忽略。

### missing_ranges()

Receiver 收到后面的 chunk，但前面有缺口时，会通过 `missing_ranges` 提示 Sender。

例子：

```text
收到 chunk 2
没收到 chunk 0 和 1

missing_ranges = 0-1
```

Sender 收到 missing hint 后，不一定立刻重传。当前策略是：

```text
如果 chunk 从未发送:
  优先发送

如果 chunk 已发送但未确认:
  线路断开或超过基础 RTO 才重传

如果 chunk 已确认:
  忽略
```

### checkpoint()

`checkpoint()` 不直接写盘，它只生成 `ChunkCheckpointTask`。

原因：

```text
event loop 不能做 fsync
fsync 可能很慢
```

所以流程是：

```text
event loop:
  ChunkedReceiverStream::checkpoint()
  得到 ChunkCheckpointTask
  投递给 ReceiverApp::DiskWriter

writer thread:
  ChunkedReceiverStream::write_checkpoint_task(task)
```

`.meta` 写盘顺序：

```text
fsync .tmp
写 .meta.writing
fsync .meta.writing
rename .meta.writing -> .meta
fsync .yisync_tmp
```

这样 `.meta` 不会写一半覆盖旧状态。

### FILE_COMMIT 后台化

`FILE_COMMIT` 以前是强 barrier：drain writer、校验整文件、rename、fsync，都在 event loop 路径里。现在已经拆成后台化。

现在流程：

```text
event loop:
  prepare_commit()
  只检查 stream/order/file/chunk 完整性
  生成 ChunkCommitTask
  投递给 DiskWriter

writer thread:
  write_commit_task()
  校验整文件 CRC32C
  写最终 checkpoint
  rename .tmp -> final_path
  fsync final file
  fsync final parent directory
  删除 .meta
  fsync .yisync_tmp

event loop:
  poll_chunk_commit()
  finish_commit()
  expected_order_seq 前进
  发送最终 HEARTBEAT
```

关键接口：

```text
prepare_commit(const FileCommit&, ChunkCommitTask&)
write_commit_task(const ChunkCommitTask&)
finish_commit(const ChunkCommitResult&)
abort_commit(order_seq)
```

为什么 `expected_order_seq` 要等后台完成后再推进？

因为 `expected_order_seq` 代表这个文件已经安全提交。后台 commit 没完成前，最终文件还没 rename/fsync 完，不能告诉 Sender “完成了”。

### 重启恢复

Receiver 启动时会：

```text
扫描最终目录 entries
扫描 .yisync_tmp/*.meta
找到对应 .tmp
恢复 ActiveFile
恢复 received/checkpointed bitmap
expected_order_seq 回到最小未完成 order_seq
```

恢复后 Sender 重连，Receiver 发送 MANIFEST，里面带 `incomplete_chunks`。Sender 根据 `.meta` 中的 checkpointed chunk 跳过已完成部分，只发送缺失 chunk。

## 9. ReceiverApp：B 端进程怎么跑

相关文件：

- [src/yisync_receiver_app.cpp](src/yisync_receiver_app.cpp)

`ReceiverApp` 是真实 receiver 进程。

### run()

启动时：

```text
创建 receiver root
按 --lines 监听多个 TCP 端口
启动 checkpoint timer
启动 heartbeat timer
启动超时 timer
进入 EventLoop::run()
```

### on_accept()

每条 TCP line 接入后：

```text
创建 AsyncFrameConnection
注册 on_message / on_error / on_close
start(loop)
send_manifest(line_id)
```

这就是“连接建立后，B 端先发当前目录信息”。

### send_manifest()

`send_manifest()` 会：

```text
找出所有 stream root
每个 stream 调 scan_manifest_stream()
合成 Manifest
send_message(line_id, Manifest)
```

默认 stream 是 receiver root。数字目录也会当成 stream root。

### StreamReceivers

`StreamReceivers` 是每个 stream 的 Receiver 端上下文：

```text
stream_id
root
append ReceiverStream
chunk ChunkedReceiverStream
append durable 状态
chunk commit pending 状态
```

为什么一个 stream 里有 append 和 chunk 两套 receiver？

因为小文件和 append 用 `ReceiverStream`，大文件用 `ChunkedReceiverStream`。同一个目录里可能小文件和大文件混合，所以需要协调它们看到的已提交状态。

### DiskWriter

`ReceiverApp::DiskWriter` 是后台写盘线程。

特点：

```text
bounded SPSC ring queue
单生产者 = event loop
单消费者 = writer thread
不用 mutex
不用 condition_variable
```

它执行：

- chunk checkpoint `.meta` 写盘。
- append 文件 fsync。
- chunk commit。

队列满时当前是 fail-fast，后续要接背压。

### heartbeat 聚合

Receiver 不对每条成功消息立刻发 ACK。

普通 `DATA/CHUNK`：

```text
queue_heartbeat()
每 50ms flush_all_heartbeats()
```

状态边界：

```text
CREATE      立即 flush
FILE_BEGIN  立即 flush
FILE_COMMIT 后台 commit 完成后 flush
```

### apply_create()

```text
append_receiver.apply(create)
失败 -> NACK
成功 -> reset append durable context
刷新 chunk receiver 已提交状态
发送 HEARTBEAT
```

### apply_data()

```text
append_receiver.apply(data)
失败 -> NACK
成功 -> maybe_enqueue_append_fsync()
queue HEARTBEAT(offset, durable_offset)
刷新 chunk receiver 已提交状态
```

### apply_begin()

```text
chunk_receiver.apply(begin)
失败 -> NACK
成功 -> HEARTBEAT(begin ready)
```

### apply_chunk()

```text
chunk_receiver.apply(chunk)
失败 -> NACK
成功 -> HEARTBEAT(received_chunks, missing_ranges)
如果 pending_checkpoint_bytes >= 4MB，调度 checkpoint_now()
```

### checkpoint_now()

```text
遍历所有 stream
对每个 chunk receiver 调 checkpoint()
把每个 ChunkCheckpointTask 投递给 DiskWriter
队列满 -> fail-fast
```

### apply_commit()

```text
如果同 stream 已有 commit pending:
  同一个 commit 重复到达 -> 忽略，等后台完成
  不同 commit -> NACK

prepare_commit()
失败 -> NACK
成功 -> 生成 ChunkCommitTask
投递 DiskWriter
启动 commit poll timer
```

### poll_chunk_commit()

```text
后台 commit 失败:
  abort_commit()
  如果原 line 还在，发送 NACK
  fail receiver

后台 commit 成功:
  finish_commit()
  refresh append receiver committed state
  如果原 line 还在，发送最终 HEARTBEAT
  schedule_quiet_stop()
```

注意：如果 commit 后原 TCP line 已断开，Receiver 不会往关闭连接发 heartbeat。Sender 重连后会重新拿 MANIFEST 或重发 commit。

## 10. SenderApp：A 端进程怎么跑

相关文件：

- [src/yisync_sender_app.cpp](src/yisync_sender_app.cpp)

`SenderApp` 是真实 sender 进程。

### run()

启动时：

```text
构建 source streams
连接每条 TCP line
启动 10ms tick
启动 30s timeout
进入 EventLoop::run()
```

### build_source_streams()

如果没有 `--source-root`：

```text
生成模拟数据
创建一个默认 stream 9001
创建一个 FileSendTask
```

如果有 `--source-root`：

```text
扫描 source-root 下的数字子目录
如果有数字子目录:
  每个数字子目录是一个 stream
否则:
  source-root 是默认 stream 9001

每个 stream:
  SourceDirectory.scan_manifest()
  SourceDirectory.files()
  每个 SourceFile -> FileSendTask
```

### FileSendTask

`FileSendTask` 是 Sender 侧“一个 entry 怎么发送”的状态。

重要字段：

```text
stream_id
order_seq
file_id
kind
name
link_target
source_size
source_checksum
range_checksum
real_source
source_root
simulated_data
```

chunk 相关字段：

```text
chunk_mode
chunk_count
chunk_order
chunk_acked
chunk_sent
chunk_line
chunk_send_tick
chunk_attempts
chunk_priority
begin_sent
begin_ready
commit_sent
resume_from_incomplete
```

append 相关字段：

```text
append_needs_create
append_data_needed
append_create_sent
append_create_ready
append_data_sent
append_offset
append_next_offset
append_current_data_len
append_create_seq
append_data_seq
append_done_next_seq
```

### StreamSendState

`StreamSendState` 是 Sender 侧“一个 stream 的发送状态”：

```text
stream_id
root
source_directory
source_manifest
tasks
current_task
next_append_seq
manifest_applied
complete
```

同一个 stream 一次只推进一个 `current_task`，保证目录内严格一致。

### 连接和重连

每条 line 有：

```text
id
endpoint
connection
connector
connected
connecting
reconnect_scheduled
reconnect_attempts
```

连接失败或断开时：

```text
scheduler.on_line_disconnected()
把该 line 上未确认 chunk 标回未发送
begin/commit 如果发在该 line 且还没确认，也标回未发送
按指数退避 schedule_reconnect()
```

### on_manifest()

Receiver 发来 MANIFEST 后：

```text
对每个 StreamSendState 调 apply_stream_manifest()
```

### apply_stream_manifest()

流程：

```text
找到 receiver 对应 stream 的 manifest
diff_stream(source_entries, receiver_entries)

如果已经一致:
  stream complete

如果需要 ResumeExisting:
  走 append plan

如果需要 CreateMissing 且 task 是 chunk:
  apply_chunk_resume()

否则:
  apply_append_plan()
```

### apply_chunk_resume()

用于断线或进程重启后大文件续传：

```text
plan_chunk_resume_from_manifest()
如果最终文件已经完整:
  advance_stream_task()
如果 receiver 有 incomplete chunk:
  checkpointed chunk 标为 acked
  只发送缺失 chunk
否则:
  重新发送 FILE_BEGIN
```

### append 发送

`apply_append_plan()` 先计算：

```text
是否需要 CREATE
从哪个 offset 开始 DATA
DATA 分几段
每段最大 64KB
最终 append_done_next_seq 是多少
```

`send_append_if_possible()` 负责真正发送：

```text
如果需要 CREATE 且没发送:
  try_send_ordered(Create)
  等 heartbeat next_seq 前进

如果 CREATE ready 且需要 DATA:
  读取 source payload
  构造 Data
  try_send_ordered(Data)
  等 heartbeat next_seq 和 durable_offset
```

### chunk 发送

`send_file_begin()`：

```text
构造 FILE_BEGIN
通过 scheduler 选 line
发送
等待 begin ready heartbeat
```

`schedule_chunk_work()`：

```text
如果 begin 未 ready:
  尝试发送 FILE_BEGIN

循环选择下一个未发送 chunk:
  优先 chunk_priority
  再按 chunk_order
  读取 payload
  make_chunk_from_payload()
  scheduler.try_acquire()
  发送 CHUNK
  记录 chunk_sent / chunk_line / chunk_send_tick / attempts

如果全部 chunk acked:
  send_commit_if_possible()
```

`send_commit_if_possible()`：

```text
构造 FILE_COMMIT
通过 scheduler 选 line
发送
等待 heartbeat.next_seq > order_seq
```

### on_heartbeat()

Sender 收到 HEARTBEAT 后：

```text
scheduler.on_heartbeat()
找到对应 stream 和 current task
```

append 模式：

```text
CREATE heartbeat 到达 -> append_create_ready = true
DATA heartbeat 到达 -> append_next_offset 前进
durable_offset >= source_size -> 文件完成
advance_stream_task()
```

chunk 模式：

```text
begin ready heartbeat -> begin_ready = true
received_chunks -> 对应 chunk_acked = true
missing_ranges -> 对应 chunk_priority = true
commit_sent 且 next_seq > order_seq -> 文件完成
advance_stream_task()
```

### tick()

每 10ms tick 做：

```text
current_tick_ += 1
scheduler.refill_ticks(1)
检查 line stale / heartbeat timeout
检查 chunk 基础 RTO
schedule_work()
```

当前 RTO 还是固定 tick，不是基于 RTT 动态计算。

## 11. Scheduler：限速、背压、选线

相关文件：

- [include/yisync_scheduler.hpp](include/yisync_scheduler.hpp)
- [src/yisync_scheduler.cpp](src/yisync_scheduler.cpp)

### TokenBucket

每条 line 有一个 token bucket：

```text
tokens_per_tick = 每 10ms 增加多少 token
capacity        = 桶最大容量
tokens          = 当前可用额度
```

只有 token 足够，才允许发送。

### recv_window 和 inflight

除了限速，还要看 Receiver 窗口：

```text
inflight_bytes + message_size <= recv_window_bytes
```

`inflight_bytes` 表示已经发出去但还没被 Receiver heartbeat 确认的数据。

### LineState

每条 line 维护：

```text
connected
healthy
stale
missed_heartbeat_ticks
consecutive_failures
last_completed_bytes
pending sends
```

### try_acquire()

Sender 发送前先问 scheduler：

```text
SendRequest -> try_acquire() -> optional<SendGrant>
```

如果返回空，说明现在不能发，等待下一次 tick 或 heartbeat。

如果返回 `SendGrant`：

```text
line_id = 选中的 line
bytes   = 本次占用额度
```

### on_heartbeat()

heartbeat 到达后：

```text
释放 pending send
降低 inflight
更新 recv_window
标记 line healthy
清 stale
```

### line 断开

line 断开后：

```text
scheduler 清理该 line pending / inflight
Sender 把该 line 上未 ack 的 chunk 重新标记为未发送
```

这样 chunk 可以换另一条健康线路重发。

## 12. 网络层和 event loop

相关文件：

- [include/yisync_async.hpp](include/yisync_async.hpp)
- [src/yisync_async.cpp](src/yisync_async.cpp)

### EventLoop

`EventLoop` 是一个简单的 `poll` 循环：

```text
watch_fd(fd, events, callback)
update_fd(fd, events)
unwatch_fd(fd)
call_later(delay, callback)
run()
stop()
```

它同时处理：

- TCP fd 可读可写。
- 定时器。

### AsyncFrameConnection

它包装一个非阻塞 TCP fd。

发送：

```text
send(Message)
  encode_frame()
  放入 write_queue
  POLLOUT 时 flush_writes()
```

接收：

```text
POLLIN
  read_available()
  parse_frames()
  decode_message()
  message_callback(message)
```

TCP 是流，所以 `parse_frames()` 会用 header 的 `body_len` 等完整 frame 收齐后才 decode。

### AsyncTcpListener

Receiver 用它监听多条 TCP line。

```text
listen_async_tcp()
listener.start(loop, on_accept)
```

### async_connect_tcp()

Sender 用它异步拨号。

连接成功：

```text
on_connected(line_id, connection)
```

失败：

```text
schedule_reconnect(line_id)
```

## 13. 完整流程：小文件

假设源目录有一个小文件：

```text
source/a.txt size = 10KB
```

流程：

```text
Receiver 启动
Sender 连接 Receiver
Receiver 发送 MANIFEST
Sender diff 发现 a.txt 缺失
Sender 生成 FileSendTask，chunk_mode = false
Sender 发送 CREATE
Receiver 创建空文件
Receiver heartbeat next_seq 前进
Sender 发送 DATA
Receiver 校验 CRC32C、offset、seq，append 写入
Receiver 投递 append fsync 到 DiskWriter
DiskWriter fsync 完成
Receiver heartbeat durable_offset 前进
Sender 判断文件完成
```

关键代码：

```text
Sender:
  apply_stream_manifest()
  apply_append_plan()
  send_append_if_possible()
  on_heartbeat()

Receiver:
  apply_create()
  ReceiverStream::apply(Create)
  apply_data()
  ReceiverStream::apply(Data)
  maybe_enqueue_append_fsync()
  poll_append_fsync()
```

## 14. 完整流程：大文件 chunk

假设源目录有一个大文件：

```text
source/big.bin size = 160KB
chunk_size = 64KB
chunk_count = 3
```

流程：

```text
Receiver 发送 MANIFEST
Sender diff 发现 big.bin 缺失
Sender 生成 chunk FileSendTask
Sender 发送 FILE_BEGIN
Receiver 创建 .tmp 和初始 .meta
Receiver heartbeat begin ready
Sender 将 chunk 0/1/2 交给 scheduler
Scheduler 分配到不同 TCP line
Receiver 按 chunk_index 乱序写 .tmp
Receiver heartbeat received_chunks / missing_ranges
Sender 标记 chunk acked
所有 chunk acked 后 Sender 发送 FILE_COMMIT
Receiver event loop prepare_commit()
Receiver 投递 ChunkCommitTask 到 DiskWriter
DiskWriter 校验整文件 CRC32C、checkpoint、rename、fsync
Receiver poll_chunk_commit()
Receiver finish_commit()
Receiver heartbeat next_seq 前进
Sender 判断 big.bin 完成
```

关键代码：

```text
Sender:
  apply_chunk_resume()
  send_file_begin()
  schedule_chunk_work()
  send_commit_if_possible()
  on_heartbeat()

Receiver:
  apply_begin()
  ChunkedReceiverStream::apply(FileBegin)
  apply_chunk()
  ChunkedReceiverStream::apply(Chunk)
  apply_commit()
  ChunkedReceiverStream::prepare_commit()
  ChunkedReceiverStream::write_commit_task()
  poll_chunk_commit()
```

## 15. 完整流程：断线重连

断线时，Sender 不依赖本地持久化状态。

运行中 line 断开：

```text
AsyncFrameConnection on_close/on_error
SenderApp::on_line_unavailable()
scheduler.on_line_disconnected()
该 line 上未确认 chunk 重新标记为未发送
schedule_reconnect()
```

重连成功后：

```text
Receiver 发送新的 MANIFEST
Sender 重新 diff
如果 Receiver 已有完整最终文件:
  跳过
如果 Receiver 有 incomplete chunk:
  跳过 .meta 中已 checkpoint chunk
  只发缺失 chunk
如果 Receiver 只有较短最终文件:
  append 续传
```

这就是“A 端不持久化同步状态”的核心。

## 16. 文件系统写盘策略

### append 文件

```text
DATA 到达
ReceiverStream 写目标文件
event loop 投递 fsync task
DiskWriter fsync 文件
event loop 收到完成
durable_offset 前进
```

### chunk checkpoint

```text
CHUNK 到达
写 .tmp
received bitmap 更新
定时或累计 4MB 后 checkpoint
DiskWriter:
  fsync .tmp
  写 .meta.writing
  fsync .meta.writing
  rename .meta
  fsync .yisync_tmp
```

### chunk commit

```text
FILE_COMMIT 到达
event loop 轻量检查并入队
DiskWriter:
  CRC32C 整文件校验
  最终 checkpoint
  rename .tmp -> final file
  fsync final file
  fsync final parent directory
  remove .meta
  fsync .yisync_tmp
event loop 推进 expected_order_seq
```

## 17. 安全约束和不变量

这些规则不能随便破坏。

### 路径安全

所有来自网络的 `name` 都必须是安全相对路径：

```text
不能空
不能是绝对路径
不能包含 ..
```

否则 Receiver 可能写出 root 外。

### 同 stream 顺序

同一个 stream 内：

```text
append: expected_seq 必须严格前进
chunk:  FILE_BEGIN/FILE_COMMIT 必须按 expected_order_seq
```

不同 stream 可以并行。

### received 不等于 durable

不要把 heartbeat `received_chunks` 当成掉电可恢复状态。

真正可恢复的是：

```text
MANIFEST.incomplete_chunks.received_chunks
```

它来自 `.meta`。

### commit 完成前不能推进 next_seq

`FILE_COMMIT` 入队后台 writer 后，不能立刻发完成 heartbeat。必须等：

```text
CRC32C OK
rename OK
fsync OK
.meta cleanup OK
```

然后才能 `expected_order_seq += 1`。

### 队列满不能假装成功

DiskWriter 队列满时，如果继续推进内存状态，会造成：

```text
内存认为 checkpoint/commit 已完成
磁盘其实没有持久化
进程重启后状态丢失
```

所以当前 fail-fast。

## 18. 当前已经完成

功能上已经有：

- wire 协议原型。
- 协议瘦身。
- CRC32C。
- manifest scan/diff。
- 真实 `--source-root` reader。
- 目录、子目录、空目录、软链接同步。
- 小文件 `CREATE/DATA`。
- append 续传。
- 大文件 `FILE_BEGIN/CHUNK/FILE_COMMIT`。
- chunk 乱序接收。
- chunk missing hint。
- chunk checkpoint 和重启恢复。
- sender 消费 `MANIFEST.incomplete_chunks`。
- 多 stream。
- 多 TCP line。
- token bucket 限速和 recv window 背压。
- TCP line 自动重连。
- 后台 SPSC DiskWriter。
- append fsync 后 durable_offset 推进。
- chunk commit 后台化。

## 19. 当前还没做

### 配置

还没有配置文件。现在大部分参数来自命令行或常量：

```text
kLineBudgetBytes
kLineWindowBytes
kReceiverCheckpointInterval
kReceiverHeartbeatInterval
kReceiverCommitPollInterval
kDiskWriterQueueCapacity
```

### watcher

只有 polling fallback，没接：

- Linux inotify。
- macOS FSEvents。
- 长期运行时新增文件 / append 事件进入发送队列。

### DiskWriter 指标和背压

还没统计：

- 队列深度。
- 排队延迟。
- fsync 延迟。
- commit CRC32C 耗时。
- rename / fsync 耗时。

队列满现在是 fail-fast，后续应该接 scheduler 背压或降速。

### 重传策略

已有：

- missing hint。
- line 断开后 chunk 重排。
- 基础 RTO。

还缺：

- RTT 动态 RTO。
- NACK 后自动重新拉 MANIFEST。
- NACK 后自动 re-diff。
- BadChecksum / BadCommit / SizeConflict 的恢复策略。
- 重传次数上限。

### QoS

还没做：

- 控制消息优先。
- 小文件优先。
- 重传优先。
- stream 权重。
- 与 scheduler 的 token/window/inflight 策略统一。

### UDP / QUIC

接口有预留，但没有实现：

- UDP adapter。
- QUIC stream/datagram adapter。
- UDP/QUIC 下 frame 边界和重传责任。

### 压缩和更多校验

还没做：

- LZ4。
- Zstd。
- MD5。
- 压缩前后校验策略。

### 非 append 语义

还没做：

- 删除。
- 重命名。
- 原地修改。
- 已存在目录树结构变更 diff。
- 软链接 target 变化后的更新策略。
- rsync delta。

## 20. 建议读代码顺序

如果完全不熟，建议按这个顺序读：

1. [include/yisync_protocol.hpp](include/yisync_protocol.hpp)

   先看所有线上消息长什么样。

2. [src/yisync_protocol.cpp](src/yisync_protocol.cpp)

   看 frame 和消息怎么编码解码。

3. [include/yisync_sync.hpp](include/yisync_sync.hpp) 和 [src/yisync_sync.cpp](src/yisync_sync.cpp)

   看 manifest 怎么扫描、diff 怎么判断从哪里传。

4. [include/yisync_receiver.hpp](include/yisync_receiver.hpp) 和 [src/yisync_receiver.cpp](src/yisync_receiver.cpp)

   看 Receiver 如何应用 CREATE/DATA/CHUNK/COMMIT。

5. [include/yisync_scheduler.hpp](include/yisync_scheduler.hpp) 和 [src/yisync_scheduler.cpp](src/yisync_scheduler.cpp)

   看限速、窗口和线路评分。

6. [include/yisync_async.hpp](include/yisync_async.hpp) 和 [src/yisync_async.cpp](src/yisync_async.cpp)

   看 event loop 和异步 TCP。

7. [src/yisync_receiver_app.cpp](src/yisync_receiver_app.cpp)

   看 B 端如何把协议、receiver 状态机、disk writer 和 heartbeat 拼起来。

8. [src/yisync_sender_app.cpp](src/yisync_sender_app.cpp)

   看 A 端如何构造发送计划、处理 MANIFEST、调度 chunk、处理 heartbeat 和断线。

9. [src/main.cpp](src/main.cpp)

   看单进程 demo 里各个模块如何被测试。

## 21. 后续重构优先级

当前最应该先拆 SenderApp。

原因：

```text
配置解析会影响 sender plan
watcher 会影响 sender plan
QoS 会影响 sender send queue
重传策略会影响 sender chunk state
```

如果不先拆，后面功能继续加进去会越来越难维护。

建议拆法：

1. 拆 `FileSendTask` / `StreamSendState` 到 sender plan 模块。
2. 拆 manifest diff -> task list。
3. 拆 chunk resend / missing hint 状态。
4. 把真实文件读取和模拟数据读取收敛到 reader 接口。
5. 再拆 ReceiverApp 里的 DiskWriter、HeartbeatAggregator、StreamReceiverMap。
