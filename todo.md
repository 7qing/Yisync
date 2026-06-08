# Yisync TODO

本文档只记录还没完成的事情。已经完成的功能和代码细节见 [detail.md](detail.md)，协议字段见 [protocol.md](protocol.md)，构建运行入口见 [readme.md](readme.md)。

## 1. 配置解析

- [ ] 支持多目录配置。
- [ ] 支持多 stream 配置。
- [ ] 支持多 TCP line / 多 IP 配置。
- [ ] 支持每条 line 的限速阈值、窗口大小、重连参数。
- [ ] 支持 chunk size、heartbeat interval、checkpoint interval、checkpoint bytes 等运行参数。
- [ ] 明确配置热更新是否支持，不支持时要明确重启生效。

## 2. 真实 Watcher

- [ ] Linux 接入 `inotify`。
- [ ] macOS 接入 `FSEvents`。
- [ ] 保留当前 polling fallback。
- [ ] 新增文件事件接入 Sender 发送队列。
- [ ] append 事件接入 Sender 发送队列。
- [ ] 支持长期运行，而不是只在启动时 `--source-root` 一次性扫描。
- [ ] 处理 watcher 事件丢失后的全量 rescan 策略。

## 3. SenderApp 重构

当前 `src/yisync_sender_app.cpp` 仍然偏大，后续配置、watcher、QoS 都会继续压到这里。

- [ ] 拆出 sender plan 模块，承载 `FileSendTask` 和 `StreamSendState`。
- [ ] 拆出 manifest diff 到 task list 的逻辑。
- [ ] 拆出 chunk resend / missing hint 状态。
- [ ] 拆出 append 续传状态。
- [ ] 将真实文件 reader 和模拟数据 reader 收敛到统一接口。
- [ ] 把 line reconnect 和发送队列从业务逻辑中剥离。

## 4. ReceiverApp 重构

当前 `src/yisync_receiver_app.cpp` 里同时有网络处理、stream map、heartbeat 聚合、disk writer。

- [ ] 将 SPSC disk writer 抽到独立模块。
- [ ] 将 heartbeat 聚合器抽到独立模块。
- [ ] 将 stream receiver map 管理抽到独立模块。
- [ ] 明确 append receiver 与 chunk receiver 的协调边界。
- [ ] 将 commit completion poller 抽成明确组件。
- [ ] 给 ReceiverApp 增加更清晰的错误上报路径。

## 5. TCP Line 管理

- [ ] 抽 sender line 连接、重连、发送封装。
- [ ] 抽 receiver line accept、连接状态、发送封装。
- [ ] 统一 line state 日志。
- [ ] 统一 heartbeat timeout 处理。
- [ ] 为 UDP / QUIC adapter 留出同一套 line/connection 状态接口。

## 6. Disk Writer 生产化

当前已有自己实现的 bounded SPSC disk writer，checkpoint、append fsync 和 chunk commit 已经从 event loop 热路径移出。

- [ ] 暴露队列深度指标。
- [ ] 统计排队延迟。
- [ ] 统计 checkpoint 耗时。
- [ ] 统计 append fsync 耗时。
- [ ] 统计 commit 里的 CRC32C、rename、fsync 耗时。
- [ ] 队列满时接入 scheduler 背压或降速策略，而不是简单 fail-fast。
- [ ] 明确 writer 线程异常后的进程处理策略。

## 7. Commit 性能

当前已经完成：

- [x] 拆分 commit 阶段。
- [x] 整文件 CRC32C 校验后台化。
- [x] rename / fsync 从 event loop 移到后台 writer。
- [x] event loop 只做轻量准备和 completion poll。

还要继续做：

- [ ] 评估极端大文件下单 writer 串行 commit 对整体吞吐的影响。
- [ ] 评估多个 stream 同时 commit 时是否需要多 writer 或分区 writer。
- [ ] 给 P99 < 5ms 增加指标验证，而不是只靠结构设计。
- [ ] 明确 commit 失败后的恢复策略。

## 8. 重传和错误恢复

当前已有 missing hint、断线重排、基础 RTO。

- [ ] 基于 RTT 动态计算 RTO。
- [ ] NACK 后自动重新拉取 `MANIFEST`。
- [ ] NACK 后自动 re-diff。
- [ ] 对 `BadChecksum`、`BadCommit`、`SizeConflict` 等错误定义恢复策略。
- [ ] 增加重传次数上限。
- [ ] 增加最终失败上报。
- [ ] 区分正常乱序、丢包、线路断开导致的缺块。

## 9. QoS 和优先级

当前已有 chunk priority 的基础状态，但还不是完整 QoS。

- [ ] 控制消息优先。
- [ ] 小文件优先。
- [ ] 重传优先。
- [ ] 支持目录或 stream 权重。
- [ ] 与 scheduler 的 token、window、in-flight 策略合并。
- [ ] 定义饥饿保护，避免大文件或低优先级 stream 永远发不出去。

## 10. UDP / QUIC

当前 transport 接口有预留，但真实 adapter 还没有实现。

- [ ] 实现 UDP adapter。
- [ ] 实现 QUIC stream adapter。
- [ ] 评估 QUIC datagram 是否适合 chunk。
- [ ] 定义 UDP/QUIC 下的 frame 边界。
- [ ] 定义 UDP/QUIC 下的重传责任边界。
- [ ] 与现有 line health 和 scheduler 状态对接。

## 11. 压缩和更多校验

- [ ] 实现 LZ4。
- [ ] 实现 Zstd。
- [ ] 实现 MD5。
- [ ] 设计压缩前数据与压缩后数据的校验策略。
- [ ] 明确压缩失败、解压失败、校验失败时的 NACK reason。

## 12. 协议生产化

- [ ] 严格实现 version / capability negotiation。
- [ ] 定义可扩展字段策略。
- [ ] 定义错误码兼容策略。
- [ ] 评估 bit packing，决定是否值得把部分枚举或布尔压到 bit 级。
- [ ] 明确未来协议升级时的兼容规则。
- [ ] 给每个消息增加更系统的 fuzz / decode 测试。

## 13. 非 Append 语义

当前主要支持新增文件和 append。

- [ ] 删除。
- [ ] 重命名。
- [ ] 原地修改。
- [ ] 已存在目录树的结构变更 diff。
- [ ] 软链接 target 变化后的更新策略。
- [ ] rsync delta。

## 14. 测试和可观测性

- [ ] 增加真实 A/B 进程集成测试脚本。
- [ ] 增加断线重连自动化测试。
- [ ] 增加 receiver 进程重启后 chunk 恢复测试。
- [ ] 增加目录、空目录、软链接同步测试。
- [ ] 增加多 stream 并发测试。
- [ ] 增加限速和背压行为测试。
- [ ] 增加关键指标日志或 metrics 输出。

## 15. 文档同步规则

- [ ] 每改协议字段，同步更新 [protocol.md](protocol.md)。
- [ ] 每改构建、运行参数、代码地图，同步更新 [readme.md](readme.md)。
- [ ] 每完成一个 TODO，同步更新本文档。
- [ ] 每做较大模块重构，同步更新 [detail.md](detail.md)。

## 建议下一步

优先做 `SenderApp 重构`。Sender 后续要接配置、watcher、QoS、真实长跑队列，如果不先拆，后面会越来越难维护。
