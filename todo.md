# Yisync TODO

本文档只记录还没完成、还需要评估、或者明确暂不做的事情。已经完成的功能见 [readme.md](readme.md)，代码细节见 [detail.md](detail.md)，协议字段见 [protocol.md](protocol.md)。

## 1. Sender

- [ ] 设计 append 剩余增量大于 64KB 时是否切到 chunk commit 语义；当前 `ResumeExisting` 仍然使用 `DATA` 分段。
- [ ] 将发送推进逻辑进一步收敛为明确的 task state machine，减少 `SenderApp` 中跨函数修改 `FileSendTask` 字段。

## 2. Network 和 QoS

- [ ] 将 control queue、小文件、重传 chunk 合并成统一优先级队列。
- [ ] 控制消息优先，例如 `Manifest1`、重发触发消息、恢复消息。
- [ ] 小文件优先，减少大文件 chunk 对小文件延迟的影响。
- [ ] 重传 chunk 优先，降低缺口长时间存在的概率。
- [ ] 支持目录或 stream 权重。
- [ ] 将优先级队列与 scheduler 的 token、window、in-flight 策略合并。
- [ ] 定义饥饿保护，避免大文件或低优先级 stream 永远发不出去。

## 3. UDP / QUIC / AREON

当前 network 层已有 `Protocol` / `LineEndpoint` 接口预留，但真实 adapter 还没有实现。

- [ ] 实现 UDP adapter。
- [ ] 实现 QUIC stream adapter。
- [ ] 实现 AREON adapter。
- [ ] 评估 QUIC datagram 是否适合 chunk。
- [ ] 定义 UDP / QUIC / AREON 下的 frame 边界。
- [ ] 定义 UDP / QUIC / AREON 下的重传责任边界，避免和当前 Sender 发送缓冲重复或冲突。

## 4. Disk Writer 和 Commit 性能

当前 append fsync 和 chunk commit 已经从 event loop 热路径移到 `SpscDiskWriter`，但还缺生产化指标和极端场景评估。

- [ ] 暴露 disk writer 队列深度指标。
- [ ] 统计 disk writer 排队延迟。
- [ ] 统计 append fsync 耗时。
- [ ] 统计 chunk commit 中 CRC32C、rename、fsync 的耗时。
- [ ] 队列满时接入 scheduler 背压或降速策略，而不是简单 fail-fast。
- [ ] 明确 writer 线程异常后的进程处理策略。
- [ ] 评估极端大文件下单 writer 串行 commit 对整体吞吐的影响。
- [ ] 评估多个 stream 同时 commit 时是否需要多 writer 或分区 writer。
- [ ] 给 P99 < 5ms 增加指标验证，而不是只靠结构设计。
- [ ] 明确 commit 失败后的恢复策略。

## 5. 压缩和更多校验

当前 wire protocol 只预留枚举，真实实现只有 `Compression::None` 和 CRC32C。

- [ ] 实现 LZ4。
- [ ] 实现 Zstd。
- [ ] 实现 MD5。
- [ ] 设计压缩前数据与压缩后数据的校验策略。
- [ ] 明确压缩失败、解压失败、校验失败时的 `NackReason`。

## 6. 产品化和可观测性

- [ ] 整理关键 metrics 输出格式，保证 Sender / Receiver / network / disk writer 的字段一致。
- [ ] 为最终失败路径补充更清晰的用户可读错误摘要。
- [ ] 为长期运行场景补充滚动日志或日志级别控制。
- [ ] 增加更大规模目录树和更大文件的性能压测脚本。

## 7. 当前明确暂不做

这些不是当前业务场景需要的语义，先不要实现：

- [ ] 删除。
- [ ] 重命名。
- [ ] 原地修改。
- [ ] 已存在目录树的结构变更 diff。
- [ ] 软链接 target 变化后的更新策略。
- [ ] rsync delta。
- [ ] 配置热更新；配置只在启动时读取，修改后必须重启。
- [ ] chunk 掉电级持久恢复；Receiver 的 `.yisync_tmp` 只服务当前进程内乱序 chunk 接收。

## 8. 文档维护规则

- [ ] 每改协议字段，同步更新 [protocol.md](protocol.md)。
- [ ] 每改构建、运行参数、配置或代码地图，同步更新 [readme.md](readme.md)。
- [ ] 每做较大模块重构，同步更新 [detail.md](detail.md)。
- [ ] 每完成一个 TODO，同步更新本文档。
