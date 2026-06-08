# Yisync 协议说明

本文档只描述线上协议和状态规则。具体代码怎么实现、每个类怎么协作，见 [detail.md](detail.md)。项目如何构建和运行，见 [readme.md](readme.md)。

## 1. 协议原则

当前协议遵守下面几条原则：

- Sender 是源端，Receiver 是目的端。
- Sender 不持久化同步进度。
- Receiver 的目标目录和 `.yisync_tmp` checkpoint 是恢复依据。
- 连接建立后，Receiver 先发送 `MANIFEST`。
- Sender 根据自己的源目录和 Receiver 的 `MANIFEST` 做 diff。
- 成功路径不逐条返回 `ACK`，Receiver 用 `HEARTBEAT` 汇报进度和窗口。
- 失败路径返回 `NACK`，Sender 必须暂停当前 stream，不能继续硬推。
- 同一个 stream 内文件边界严格有序。
- 不同 stream 可以并行。
- 小文件和 append 使用 `CREATE + DATA`。
- 大文件使用 `FILE_BEGIN + CHUNK + FILE_COMMIT`。
- chunk 可以乱序到达，但 commit 必须按文件顺序完成。

## 2. 协议里没有什么

这些字段已经明确不在当前 wire protocol 里：

| 不存在的字段 | 原因 |
| --- | --- |
| `plan_id` | 发送计划只属于 Sender 本地，断线后重新看 `MANIFEST` 和重新 diff |
| `flags` | 当前没有明确语义，先删除 |
| `PING` / `GOAWAY` | 当前用 TCP close、heartbeat timeout、NACK 表达连接状态 |
| `CreateMode` | `CREATE` 语义固定为目标路径必须不存在 |
| `ChecksumScope` | `FileChecksum.offset + FileChecksum.len` 已经能表达范围或整文件 |

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

当前代码没有做 varint，也没有做 bit packing。`MessageType`、`Role`、`Compression`、`ChecksumAlgo`、`EntryKind`、`NackReason` 都按 `u8` 编码。

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
| `2` | `Manifest` |
| `3` | `Create` |
| `4` | `Data` |
| `5` | `Heartbeat` |
| `6` | `Nack` |
| `7` | `FileBegin` |
| `8` | `Chunk` |
| `9` | `FileCommit` |

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
  order_seq:   u64
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

### IncompleteChunkFile

```text
IncompleteChunkFile {
  order_seq:       u64
  file_id:         u64
  name:            string
  final_size:      u64
  chunk_size:      u64
  chunk_count:     u64
  file_checksum:   FileChecksum
  prev_file_id:    u64
  prev_final_size: u64
  prev_checksum:   FileChecksum
  received_chunks: vector<u64>
}
```

这个结构只出现在 `MANIFEST` 里。它表达 Receiver 的 `.yisync_tmp/*.meta` 中已经 checkpoint 的 chunk 状态。

注意这里的 `received_chunks` 是持久恢复状态，不是低延迟 heartbeat 状态。它只应该表达重启后还能相信的 chunk bitmap。

## 7. 消息定义

### HELLO

方向：`Sender <-> Receiver`

```text
Hello {
  node_id:                 string
  role:                    u8
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

当前实现里，`Hello.chunk_size` 只是能力字段。真正接收侧 chunk 模式目前只接受 `64KB` chunk，也就是 `kDefaultChunkSizeBytes`。

### MANIFEST

方向：`Receiver -> Sender`

```text
Manifest {
  manifest_id: u64
  streams: vector<ManifestStream>
}

ManifestStream {
  stream_id:          u64
  root:               string
  entries:            vector<ManifestEntry>
  incomplete_chunks:  vector<IncompleteChunkFile>
}
```

语义：

- `entries` 是已经出现在最终目标目录里的完整目录项。
- `incomplete_chunks` 是未 commit 但可以从 checkpoint 恢复的大文件接收上下文。
- Receiver 在 TCP line 建立后发送真实 `MANIFEST`。
- Sender 收到后用自己的源目录 manifest 做 diff。

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
- `RegularFile` 创建空文件，后续 `DATA` 继续写入。

Receiver 校验：

```text
seq == expected_seq
name 是安全相对路径
目标路径不存在
prev_file_id / prev_final_size / prev_checksum 能证明上一个 entry 已完成
```

通过后：

```text
expected_seq += 1
current_file_id = file_id
current_offset = 0
```

### DATA

方向：`Sender -> Receiver`

```text
Data {
  stream_id:      u64
  seq:            u64
  file_id:        u64
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
payload_len == payload.size()
raw_len == payload.size() when compression is None
checksum(payload) 匹配
```

通过后：

```text
append payload
expected_seq += 1
current_offset = offset + raw_len
```

append 增量即使超过 `64KB`，也按多条连续 `DATA` 发送：

```text
DATA offset=40KB  raw_len=64KB
DATA offset=104KB raw_len=16KB
```

### FILE_BEGIN

方向：`Sender -> Receiver`

```text
FileBegin {
  stream_id:        u64
  order_seq:        u64
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
- 当前实现要求 `chunk_size == 64KB`。
- Receiver 在 `.yisync_tmp` 下创建临时文件和内存 bitmap。

Receiver 校验：

```text
order_seq == expected_order_seq
final_size > 64KB
chunk_size == 64KB
chunk_count == ceil(final_size / chunk_size)
name 是安全相对路径
前一个 entry 已完成并通过校验
```

### CHUNK

方向：`Sender -> Receiver`

```text
Chunk {
  stream_id:      u64
  order_seq:      u64
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
- 同一个 `order_seq/file_id` 内 chunk 可以乱序到达。
- `chunk_index` 是幂等键。Receiver 已收到过同一个 chunk 时，可以忽略重复数据。

Receiver 校验：

```text
order_seq == active order_seq
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
  order_seq: u64
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
后台 writer 写最终 checkpoint
后台 writer rename .tmp 到最终路径
后台 writer fsync 最终文件和父目录
后台 writer 删除 .meta
event loop 收到完成结果
expected_order_seq += 1
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
  order_seq:    u64
  file_id:      u64
  chunk_index:  u64
}

MissingChunkRange {
  order_seq:          u64
  file_id:            u64
  first_chunk_index:  u64
  last_chunk_index:   u64
}
```

字段语义：

| 字段 | 语义 |
| --- | --- |
| `next_seq` | append 模式下 Receiver 下一条期望 seq |
| `file_id` | 当前文件 |
| `offset` | 当前文件已写入 offset |
| `durable_offset` | append 文件已经 fsync 完成的 offset |
| `recv_window_bytes` | Receiver 当前愿意接收的窗口 |
| `received_chunks` | 本周期收到并写入临时文件的 chunk，用于释放 in-flight |
| `missing_ranges` | Receiver 观察到的缺口范围，用于提示选择性重传 |

`HEARTBEAT.received_chunks` 是低延迟流控信号，不代表掉电级持久。

`MANIFEST.incomplete_chunks.received_chunks` 是 checkpoint 状态，代表重启后可恢复。

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

语义：

- Receiver 拒绝某条消息。
- Sender 收到后必须停止当前 stream 的硬推。
- 推荐动作是重新获取 `MANIFEST`，重新 diff，再决定是否续传。

## 8. Stream 顺序规则

### append 顺序

append 模式使用 `seq`：

```text
CREATE(seq=1)
DATA(seq=2)
DATA(seq=3)
CREATE(seq=4)
```

Receiver 只接受：

```text
seq == expected_seq
```

### chunk 顺序

chunk 模式使用 `order_seq`：

```text
FILE_BEGIN(order_seq=1)
CHUNK(order_seq=1, chunk_index=2)
CHUNK(order_seq=1, chunk_index=0)
CHUNK(order_seq=1, chunk_index=1)
FILE_COMMIT(order_seq=1)
```

规则：

- `FILE_BEGIN` 必须先到。
- 同一文件内 `CHUNK` 可以乱序。
- `FILE_COMMIT` 必须等所有 chunk 都收到。
- commit 成功后 `expected_order_seq += 1`。
- 下一个文件才能进入可见状态。

## 9. Manifest Diff 规则

Sender 收到 Receiver `MANIFEST` 后，用自己的 source manifest 对比。

| 情况 | Sender 动作 |
| --- | --- |
| Receiver 缺少 entry | 从该 entry 开始创建或 chunk 同步 |
| `Directory` 已存在且一致 | 跳过 |
| `Symlink` 已存在且 `link_target` 一致 | 跳过 |
| 普通文件 `B.size < A.size` | 从 `B.size` 继续 append |
| 普通文件 `B.size == A.size` 且 checksum 一致 | 跳过 |
| identity 不一致、checksum 不一致、B 更大 | 停止并报错 |

chunk 恢复优先看 `incomplete_chunks`：

| 情况 | Sender 动作 |
| --- | --- |
| incomplete 的 `final_size/chunk_count/file_checksum` 和源文件一致 | 复用接收上下文，只发缺失 chunk |
| 最终文件已存在且 checksum 一致 | 跳过 |
| 没有最终文件，也没有 incomplete | 从 `FILE_BEGIN` 开始 |
| incomplete 和源文件冲突 | 停止并报错 |

## 10. 断线重连规则

断线后：

```text
旧连接关闭
旧 in-flight 状态作废
Sender 重排该 line 上未确认 chunk
Sender 按退避重连
Receiver 新连接建立后重新发送 MANIFEST
Sender 重新 diff
```

为什么可以不持久化 Sender 状态：

- 已经 commit 的文件会出现在 Receiver 最终目录里。
- 未 commit 但已 checkpoint 的 chunk 会出现在 `MANIFEST.incomplete_chunks` 里。
- append 文件的大小和 checksum 可以从 Receiver 目录重新扫描得到。

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
line healthy
line not stale
tokens >= encoded_message_size
inflight_bytes + encoded_message_size <= recv_window_bytes
```

收到 `HEARTBEAT` 后：

```text
更新 recv_window_bytes
按 next_seq 或 received_chunks 释放 pending sends
降低 inflight_bytes
更新 line health
```

当前限速 tick 是 `10ms`。默认每条 line 每个 tick 补充 `96KB` token，bucket 容量也是 `96KB`。这些是当前 demo 参数，不是协议字段。

## 12. 当前实现限制

- 当前 chunk 大小固定为 `64KB`。
- 只有文件大小大于 `64KB` 的缺失文件进入 chunk 模式。
- append 增量即使大于 `64KB`，也使用多条 `DATA`，不转成 chunk commit。
- 当前只实现 `Compression::None`。
- 当前只实现 CRC32C。
- `Hello` 的能力协商还没有严格驱动后续策略。
- `NACK` 后的自动重新拉 `MANIFEST` 和 re-diff 还没有完整产品化。
- 协议字段还没有做 bit packing。
- 没有删除、重命名、原地修改、rsync delta 语义。
