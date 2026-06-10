# Yisync 协议说明

本文档只描述线上协议和状态规则。具体代码怎么实现、每个类怎么协作，见 [detail.md](detail.md)。项目如何构建和运行，见 [readme.md](readme.md)。

## 1. 协议原则

当前协议遵守下面几条原则：

- Sender 是源端，Receiver 是目的端。
- Sender 不持久化同步进度。
- Receiver 的最终目标目录是恢复依据。
- `.yisync_tmp` 只保存当前进程内的 chunk 临时文件，不进入重启恢复协议。
- 连接建立后，Sender 先发送 `Manifest1`。
- Receiver 根据 `Manifest1` 和自己的目标目录做 diff。
- Receiver 返回 `Manifest2`，告诉 Sender 每个 stream 是否已同步、从哪个文件和 offset 开始传。
- 成功路径不逐条返回单独 `ACK`，Receiver 用 `HEARTBEAT` 汇报进度、窗口，并作为批量 ACK。
- 失败路径返回 `NACK`，Sender 先从当前进程发送缓冲区找原包重发。
- 同一个 stream 内文件边界严格有序。
- 不同 stream 可以并行。
- 小文件和 append 使用 `CREATE + DATA`。
- 大文件使用 `FILE_BEGIN + CHUNK + FILE_COMMIT`。
- chunk 可以乱序到达，但 commit 必须按文件顺序完成。
- chunk 默认大小是 `64KB`；配置可以改，但 `Hello` 协商要求两端一致。

## 2. 协议里没有什么

这些字段已经明确不在当前 wire protocol 里：

| 不存在的字段 | 原因 |
| --- | --- |
| `plan_id` | 当前只需要 `manifest_id` 关联一次 `Manifest1` 和 B 端 `Manifest2` |
| `flags` | 当前没有明确语义，先删除 |
| `PING` / `GOAWAY` | 当前用 TCP close、heartbeat timeout、NACK 表达连接状态 |
| `CreateMode` | `CREATE` 语义固定为目标路径必须不存在 |
| `ChecksumScope` | `FileChecksum.offset + FileChecksum.len` 已经能表达范围或整文件 |
| `IncompleteChunkFile` | 当前需求不要 chunk 持久化，`Manifest1` 不表达未 commit chunk |

`StartAction` 也不属于协议字段。它是 `yisync_sync` 里 diff 后的本地动作，用来表示 Sender 应该续传已有文件还是创建缺失文件。

## 3. 基础编码

所有整数当前使用小端编码。

基础类型：

| 类型 | 编码 |
| --- | --- |
| `u8` | 1 字节 |
| `u16` | 2 字节，小端 |
| `u32` | 4 字节，小端 |
| `u64` | 8 字节，小端 |
| `string` | `u32 length` 后接原始字节 |
| `bytes` | `u32 length` 后接原始字节 |
| `vector<T>` | `u32 count` 后接连续元素 |

当前代码没有做 varint，也不做 bit packing。`MessageType`、`Role`、`Compression`、`ChecksumAlgo`、`EntryKind`、`NackReason` 都按 `u8` 编码。

bit packing 评估结论：

- 当前主要开销来自路径字符串、checksum、payload、chunk 数据，不来自几个枚举字段。
- bit 级压缩会让 decoder、fuzz、协议兼容和调试复杂化。
- 现阶段保留 `u8` 枚举更清晰，后续只有在 profile 证明控制消息成为瓶颈时再重新评估。

## 4. Frame Header

每条消息外层都有统一 header：

```text
MessageHeader {
  magic:      u32
  version:    u8
  msg_type:   u8
  header_len: u16
  body_len:   u32
}
```

当前固定值：

```text
magic      = 0x59495359
version    = 1
header_len = 12
```

TCP 是字节流，不保留消息边界。网络层必须按 `header_len + body_len` 还原完整 frame。

## 5. 枚举

### MessageType

| 值 | 名称 |
| --- | --- |
| `1` | `Hello` |
| `2` | `Manifest1` |
| `3` | `Create` |
| `4` | `Data` |
| `5` | `Heartbeat` |
| `6` | `Nack` |
| `7` | `FileBegin` |
| `8` | `Chunk` |
| `9` | `FileCommit` |
| `10` | `Manifest2` |

### Role

| 值 | 名称 |
| --- | --- |
| `1` | `Sender` |
| `2` | `Receiver` |

### Compression

| 值 | 名称 | 当前状态 |
| --- | --- | --- |
| `0` | `None` | 已使用 |
| `1` | `Lz4` | 预留，未实现 |
| `2` | `Zstd` | 预留，未实现 |

### ChecksumAlgo

| 值 | 名称 | 当前状态 |
| --- | --- | --- |
| `0` | `None` | 可表达不校验 |
| `1` | `Crc32c` | 已实现，使用 Google CRC32C |
| `2` | `Md5` | 预留，未实现 |

### EntryKind

| 值 | 名称 | 语义 |
| --- | --- | --- |
| `1` | `RegularFile` | 普通文件 |
| `2` | `Directory` | 目录 |
| `3` | `Symlink` | 软链接本身 |

### NackReason

| 值 | 名称 |
| --- | --- |
| `1` | `BadSession` |
| `2` | `BadSeq` |
| `3` | `BadOffset` |
| `4` | `BadChecksum` |
| `5` | `BadFileOrder` |
| `6` | `BadCreate` |
| `7` | `FileExists` |
| `8` | `PrevFileIncomplete` |
| `9` | `SizeConflict` |
| `10` | `ChecksumMismatch` |
| `11` | `UnsupportedCompression` |
| `12` | `DecodeError` |
| `13` | `IoError` |
| `14` | `BadChunk` |
| `15` | `BadCommit` |

### Manifest2Action

| 值 | 名称 | 语义 |
| --- | --- | --- |
| `0` | `InSync` | 该 stream 已一致，不需要传 |
| `1` | `ResumeExisting` | Receiver 已有较短最终文件，从 offset 继续 DATA |
| `2` | `CreateMissing` | Receiver 缺 entry，从该 entry 开始创建或 chunk 同步 |

## 6. 通用结构

### FileChecksum

```text
FileChecksum {
  algo:   u8
  offset: u64
  len:    u64
  value:  bytes
}
```

语义：

- `algo = None` 表示不校验。
- `offset + len` 表示校验范围。
- 整文件校验就是 `offset = 0` 且 `len = file_size`。
- 当前实际落地使用 `Crc32c`。

### ManifestEntry

```text
ManifestEntry {
  file_id:     u64
  seq:         u64
  kind:        u8
  name:        string
  link_target: string
  size:        u64
  checksum:    FileChecksum
}
```

`name` 是 stream root 下的相对路径，使用 `/` 分隔。Receiver 必须拒绝：

- 空路径
- 绝对路径
- 包含 `..` 的路径
- 逃出 stream root 的路径

`Symlink` 的 `link_target` 保存 `readlink()` 得到的字符串。协议复制软链接本身，不跟随目标。

## 7. 消息定义

### HELLO

方向：`Sender <-> Receiver`

```text
Hello {
  node_id:                 string
  role:                    u8
  min_version:             u16
  max_version:             u16
  feature_flags:           u64
  required_feature_flags:  u64
  chunk_size:              u32
  max_inflight_bytes:      u64
  supported_compression:   vector<u8>
  supported_checksum:      vector<u8>
}
```

用途：

- 表明节点身份。
- 表明角色。
- 表明能力集合。
- 双方必须在同一条 line 上先完成 `Hello` 协商，业务消息才能进入上层。

当前严格协商规则：

```text
peer.role 必须是期望角色
[min_version, max_version] 必须和本端有交集
双方 required_feature_flags 必须都包含在 common_features 中
Compression::None 必须双方支持
ChecksumAlgo::Crc32c 必须双方支持
chunk_size 必须一致
```

当前 feature bit：

| bit | 名称 | 语义 |
| --- | --- | --- |
| `0` | `Manifest12` | 使用 `Manifest1 / Manifest2` 启动 diff |
| `1` | `DirectoryEntry` | 支持目录 entry |
| `2` | `SymlinkEntry` | 支持软链接 entry |
| `3` | `ChunkTransfer` | 支持 `FILE_BEGIN / CHUNK / FILE_COMMIT` |
| `4` | `HeartbeatAck` | 支持 `HEARTBEAT` 批量 ACK 和背压 |
| `5` | `MissingRanges` | 支持 chunk 缺口提示 |
| `6` | `DynamicRto` | 支持基于 RTT 的 RTO |

当前 required feature：

```text
Manifest12 | ChunkTransfer | HeartbeatAck
```

协商成功后，Sender 才把该 line 标成 `negotiated=true`，scheduler 才能选择这条 line 发送数据或控制消息。协商失败时该 line 会被标成 protocol error / unhealthy。

### Manifest1

方向：`Sender -> Receiver`

```text
Manifest1 {
  manifest_id: u64
  streams: vector<Manifest1Stream>
}

Manifest1Stream {
  stream_id:          u64
  root:               string
  entries:            vector<ManifestEntry>
}
```

语义：

- `entries` 是 Sender 源目录当前扫描到的目录项。
- 未 commit 的 chunk 临时文件不进入 `Manifest1`。
- Sender 在 TCP line 建立后发送 `Manifest1`。
- Receiver 收到后扫描目标目录并生成 `Manifest2`。

### Manifest2

方向：`Receiver -> Sender`

```text
Manifest2 {
  manifest_id: u64
  streams: vector<Manifest2Stream>
}

Manifest2Stream {
  stream_id:      u64
  action:         u8
  start_file_id:  u64
  start_offset:   u64
}
```

语义：

- `manifest_id` 对应 Sender 发来的 `Manifest1`。
- 每个 stream 返回一个起点。
- `InSync` 表示该 stream 已一致。
- `ResumeExisting` 表示从已有最终文件的 `start_offset` 继续 `DATA`。
- `CreateMissing` 表示从 `start_file_id` 对应 entry 开始创建；如果该 entry 是大文件，则走 `FILE_BEGIN/CHUNK/FILE_COMMIT`。

### CREATE

方向：`Sender -> Receiver`

```text
Create {
  stream_id:        u64
  seq:              u64
  file_id:          u64
  kind:             u8
  name:             string
  link_target:      string
  final_size:       u64
  prev_file_id:     u64
  prev_final_size:  u64
  prev_checksum:    FileChecksum
}
```

语义：

- 创建一个新的 manifest entry。
- 目标路径必须不存在。
- 同时作为上一个文件的完成屏障。
- `Directory` 创建目录后立即完成。
- `Symlink` 创建软链接后立即完成。
- `RegularFile` 创建空文件，后续同 `seq` 的 `DATA` 继续写入到 `final_size`。

Receiver 校验：

```text
seq == expected_seq
name 是安全相对路径
目标路径不存在
final_size 与后续 DATA 的 final_size 一致
prev_file_id / prev_final_size / prev_checksum 能证明上一个 entry 已完成
```

通过后：

```text
current_file_id = file_id
current_offset = 0
current_final_size = final_size
目录、软链或空文件: expected_seq += 1
普通非空文件: 等 DATA 写满 final_size 后再推进 expected_seq
```

### DATA

方向：`Sender -> Receiver`

```text
Data {
  stream_id:      u64
  seq:            u64
  file_id:        u64
  offset:         u64
  final_size:     u64
  raw_len:        u32
  payload_len:    u32
  compression:    u8
  checksum_algo:  u8
  checksum:       bytes
  payload:        bytes
}
```

语义：

- append-only 数据块。
- `offset` 必须等于 Receiver 当前文件大小。
- `raw_len` 是解压后的原始长度。
- 当前实现只支持 `Compression::None`。
- 当前实际校验使用 CRC32C。

Receiver 校验：

```text
seq == expected_seq
file_id == current_file_id
offset == local_file_size
offset + raw_len <= final_size
payload_len == payload.size()
raw_len == payload.size() when compression is None
checksum(payload) 匹配
```

通过后：

```text
append payload
current_offset = offset + raw_len
如果 current_offset == final_size: expected_seq = max(expected_seq, seq + 1)
```

当前 `ResumeExisting` 走 `DATA` 续传，DATA 按 64KB 分段发送：

```text
DATA(seq=N, offset=40KB, raw_len=64KB)
DATA(seq=N, offset=104KB, raw_len=16KB)
```

### FILE_BEGIN

方向：`Sender -> Receiver`

```text
FileBegin {
  stream_id:        u64
  seq:              u64
  file_id:          u64
  name:             string
  final_size:       u64
  chunk_size:       u64
  chunk_count:      u64
  file_checksum:    FileChecksum
  prev_file_id:     u64
  prev_final_size:  u64
  prev_checksum:    FileChecksum
}
```

语义：

- 为一个大文件建立 chunk 接收上下文。
- 当前缺失文件大小大于 `64KB` 时进入 chunk 模式。
- `chunk_size` 是 wire 字段，当前默认值是 `64KB`。
- 如果配置修改 `chunk_size`，两端 `Hello.chunk_size` 必须一致，否则该 line 协商失败。
- Receiver 在 `.yisync_tmp` 下创建临时文件和内存 bitmap。

Receiver 校验：

```text
seq == expected_seq
final_size > 64KB
chunk_size > 0
chunk_count == ceil(final_size / chunk_size)
name 是安全相对路径
前一个 entry 已完成并通过校验
```

### CHUNK

方向：`Sender -> Receiver`

```text
Chunk {
  stream_id:      u64
  seq:            u64
  file_id:        u64
  chunk_index:    u64
  offset:         u64
  raw_len:        u32
  payload_len:    u32
  compression:    u8
  checksum_algo:  u8
  checksum:       bytes
  payload:        bytes
}
```

语义：

- 发送一个大文件 chunk。
- 同一个 `seq/file_id` 内 chunk 可以乱序到达。
- `chunk_index` 是幂等键。Receiver 已收到过同一个 chunk 时，可以忽略重复数据。

Receiver 校验：

```text
seq == active seq
file_id == active file_id
chunk_index < chunk_count
offset == chunk_index * chunk_size
raw_len == 当前 chunk 应有长度
payload_len == payload.size()
checksum(payload) 匹配
```

通过后：

```text
write payload to .tmp at offset
mark received[chunk_index] = true
queue heartbeat
```

### FILE_COMMIT

方向：`Sender -> Receiver`

```text
FileCommit {
  stream_id: u64
  seq:       u64
  file_id:   u64
}
```

语义：

- 提交一个 chunk 文件。
- 只有所有 chunk 都收到后才能成功。
- 当前实现把重 IO 放进后台 disk writer。

Receiver 提交流程：

```text
event loop 轻量校验
生成 commit task
后台 writer 校验整文件 CRC32C
后台 writer rename .tmp 到最终路径
后台 writer fsync 最终文件和父目录
event loop 收到完成结果
expected_seq += 1
发送最终 HEARTBEAT
```

### HEARTBEAT

方向：`Receiver -> Sender`

```text
Heartbeat {
  stream_id:          u64
  next_seq:           u64
  file_id:            u64
  offset:             u64
  durable_offset:     u64
  recv_window_bytes:  u64
  received_chunks:    vector<ReceivedChunk>
  missing_ranges:     vector<MissingChunkRange>
}

ReceivedChunk {
  seq:          u64
  file_id:      u64
  chunk_index:  u64
}

MissingChunkRange {
  seq:                u64
  file_id:            u64
  first_chunk_index:  u64
  last_chunk_index:   u64
}
```

字段语义：

| 字段 | 语义 |
| --- | --- |
| `next_seq` | Receiver 已完成的下一个 stream 内文件操作 seq |
| `file_id` | 当前文件 |
| `offset` | 当前文件已写入 offset |
| `durable_offset` | append 文件已经 fsync 完成的 offset |
| `recv_window_bytes` | Receiver 当前愿意接收的窗口 |
| `received_chunks` | 本周期收到并写入临时文件的 chunk，用于释放 in-flight 和 Sender 发送缓冲 |
| `missing_ranges` | Receiver 观察到的缺口范围，用于提示选择性重传 |

`HEARTBEAT` 同时是批量 ACK：

```text
next_seq:
  确认同一 stream 内 seq < next_seq 的 ordered 消息

received_chunks:
  确认这些 chunk 已被 Receiver 状态机接受

recv_window_bytes:
  反馈 Receiver 当前窗口，用于背压
```

Receiver 对 chunk 会累积确认。当前实现达到 20 个 chunk 确认，或者定时 heartbeat 到期，就 flush 一次 `HEARTBEAT`。

`HEARTBEAT.received_chunks` 是低延迟流控信号，只用于当前连接内释放 in-flight、删除 Sender 发送缓冲和辅助选择性重传，不代表掉电级持久。

### NACK

方向：`Receiver -> Sender`

```text
Nack {
  stream_id:          u64
  got_seq:            u64
  expected_seq:       u64
  file_id:            u64
  offset:             u64
  expected_file_id:   u64
  expected_offset:    u64
  reason:             u8
  detail:             string
}
```

Sender 收到 `NACK` 后的顺序：

```text
1. 按 stream_id / file_id / seq / offset / chunk_index 查当前进程发送缓冲区
2. 如果找到原 Message，通过 network/scheduler 重新发送
3. 如果找不到且 reason 可恢复，进入 Manifest1 / Manifest2 恢复流程
4. 如果 reason 不可恢复，或者恢复次数超过上限，最终失败
```

重发仍然必须经过 line 选择、令牌桶限速和 Receiver window 背压。

语义：

- Receiver 拒绝某条消息。
- Sender 不直接切换到 Manifest 恢复，先查当前进程发送缓冲区重发原包。
- `BadChecksum`、`BadCommit`、`SizeConflict`、`ChecksumMismatch`、`BadSeq`、`BadOffset`、`BadChunk` 属于可恢复 reason。
- 发送缓冲 miss 或重试超限后，Sender 重新发送 `Manifest1`，Receiver 重新 diff，再决定是否续传。

错误码兼容策略：

- `NackReason` 是 `u8`，新增 reason 只能追加新值，不能复用旧值。
- 收到未知 `NackReason` 时，Sender 应当按不可恢复错误处理，记录原始数字和 detail。
- 可恢复 reason 必须在 Sender 代码里显式列出，不能默认所有未知错误都可恢复。
- `UnsupportedCompression / DecodeError / IoError` 默认不可恢复，避免无限重试破坏目标状态。

## 8. Stream 顺序规则

### 统一 seq

`seq` 是同一个 stream 内的文件操作顺序号。小文件、目录、软链、大文件 chunk 都共用这一套顺序。

小文件或小 append：

```text
CREATE(seq=1)
DATA(seq=1, offset=0)
CREATE(seq=2)
```

同一个文件如果有多段 DATA，也仍然共用同一个 `seq`，靠 `offset/final_size` 判断进度：

```text
DATA(seq=1, offset=0)
DATA(seq=1, offset=65536)
```

chunk 模式也使用同一个 `seq`：

```text
FILE_BEGIN(seq=3)
CHUNK(seq=3, chunk_index=2)
CHUNK(seq=3, chunk_index=0)
CHUNK(seq=3, chunk_index=1)
FILE_COMMIT(seq=3)
```

规则：

- `FILE_BEGIN` 必须先到。
- 同一文件内 `CHUNK` 可以乱序。
- `FILE_COMMIT` 必须等所有 chunk 都收到。
- commit 成功后 `expected_seq = max(expected_seq, seq + 1)`。
- 下一个文件才能进入可见状态。

## 9. Manifest Diff 规则

Receiver 收到 Sender 的 `Manifest1` 后，用自己的目标目录 manifest 对比，并返回 `Manifest2`。

| 情况 | Receiver 返回 |
| --- | --- |
| Receiver 缺少 entry | `CreateMissing(start_file_id, 0)` |
| `Directory` 已存在且一致 | 继续比较下一个 entry |
| `Symlink` 已存在且 `link_target` 一致 | 继续比较下一个 entry |
| 普通文件 `B.size < A.size` | `ResumeExisting(file_id, B.size)` |
| 普通文件 `B.size == A.size` 且 checksum 一致 | 继续比较下一个 entry |
| 全部一致 | `InSync` |
| identity 不一致、checksum 不一致、B 更大 | `NACK` |

chunk 模式只看最终文件状态：

| 情况 | Receiver 返回 / Sender 动作 |
| --- | --- |
| 最终文件已存在且 checksum 一致 | `InSync` 或继续比较后续 entry |
| 最终文件不存在 | `CreateMissing`，Sender 从 `FILE_BEGIN` 开始重新发送完整大文件 |
| 最终文件大小或 checksum 冲突 | Receiver 返回 `NACK` |

## 10. 断线重连规则

断线后：

```text
旧连接关闭
旧连接上的 in-flight 状态作废
当前 Sender 进程重排该 line 上未确认发送
Sender 按退避重连
Sender 重新发送 Manifest1
Receiver 重新 diff 并返回 Manifest2
```

为什么可以不持久化 Sender 状态：

- 已经 commit 的文件会出现在 Receiver 最终目录里。
- append 文件的大小和 checksum 可以从 Receiver 目录重新扫描得到。
- 未 commit 的大文件 chunk 不恢复；如果 Sender/Receiver 进程重启，Receiver 会在 `Manifest2` 中要求大文件从 `FILE_BEGIN` 重新传。

## 11. 多线路调度规则

多线路调度不改变 wire message，只决定某条消息发到哪条 line。

每条 line 维护：

```text
tokens
bucket_capacity
recv_window_bytes
inflight_bytes
connected
healthy
stale
consecutive_failures
pending sends
```

发送前必须满足：

```text
line connected
line negotiated
line healthy
line not stale
tokens >= encoded_message_size
inflight_bytes + encoded_message_size <= recv_window_bytes
```

收到 `HEARTBEAT` 后：

```text
更新 recv_window_bytes
按 next_seq 或 received_chunks 释放 pending sends
Sender 删除已确认的发送缓冲
降低 inflight_bytes
更新 line health
```

当前限速 tick 是 `10ms`。默认每条 line 每个 tick 补充 `96KB` token，bucket 容量也是 `96KB`。这些是当前 demo 参数，不是协议字段。

line health 对接：

```text
TCP connected:
  connected=true
  negotiated=false
  healthy=false

Hello negotiation ok:
  negotiated=true
  healthy=true
  recv_window_bytes = peer.max_inflight_bytes

Hello negotiation failed:
  negotiated=false
  healthy=false
  stale=true
  clear in-flight

heartbeat timeout / disconnect:
  stale 或 disconnected
  pending sends 转 LostSend
```

scheduler 只会选择同时满足 `connected && negotiated && healthy && !stale` 的 line。

## 12. 扩展和兼容规则

当前 decoder 对 trailing bytes 是严格拒绝的。因此跨版本兼容不能依赖“旧端忽略未知字段”，必须通过 `Hello` 先协商版本和 feature。

字段扩展策略：

- 新字段只能追加在消息 body 尾部，不能插入旧字段中间。
- 新字段必须挂在新的 feature bit 后面。
- 发送端只有在双方都声明对应 feature 后，才允许发送带新字段的新语义。
- 修改现有字段语义必须提升 protocol version。
- 删除字段、复用 enum 值、改变整数宽度都视为不兼容变更。

消息扩展策略：

- 新 `MessageType` 只能追加新值。
- 发送新 `MessageType` 前必须通过 feature negotiation 确认对端支持。
- 收到未知 `MessageType` 当前会 decode fail，network 层把它当协议错误关闭 line。

## 13. 当前实现限制

- 默认 chunk 大小是 `64KB`，可通过配置修改；同一条连接两端必须协商一致。
- 缺失文件大小大于 `64KB` 时进入 chunk 模式。
- `ResumeExisting` append 续传当前仍使用 `DATA` 分段；把 append 剩余增量大于 `64KB` 的场景接入 chunk commit 语义仍是待办。
- 当前只实现 `Compression::None`。
- 当前只实现 CRC32C。
- `Hello` 已经驱动 line 是否可进入 scheduler，但还没有真正启用 LZ4/Zstd/MD5 等可选能力。
- 发送缓冲区缺失或重试失败后，当前会自动重新发送 `Manifest1`，Receiver 重新生成 `Manifest2`；该路径已有集成测试覆盖。
- 协议字段明确暂不做 bit packing。
- 没有删除、重命名、原地修改、rsync delta 语义。
