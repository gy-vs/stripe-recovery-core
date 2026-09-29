# stripe-recovery-core

供 Python 程序嵌入的纠删数据条带内核。调用方提交一条字节流，得到一个**可持久保存的对象描述（descriptor）**和一组写入**自己提供的存储**的分片；读取方拿到的是**经过完整性确认的对象内容**，而不是一次数学解码的原始输出。

非目标：不做文件同步客户端，不提供命令行产品，不绑定任何云存储账户。

## 为什么不是 zfec 的 encode/decode 改名导出

裸的纠删解码（如 zfec）不做任何认证：

- 把两次编码的分片混在一起 decode，会返回**不属于任一次输入**的字节，且不报错；
- 改一个分片里的一个字节，decode 同样"成功"，内容是垃圾。

本内核把"解码出字节"和"内容被确认"严格分开：每个分片段、每个条带、整个对象都有 SHA-256 记录在 descriptor 里，**任何字节只有在校验通过后才交付给调用方**。上述两类事故在本内核中的行为是：坏片被识别、剔除、用其余好片恢复；好片不足时抛出带明细的 `UnrecoverableError`。永远不会静默返回错误内容。

## 核心概念

```
调用方字节流
   │  put(name, source)           ← 异步流式消费，逐条带编码
   ▼
ObjectDescriptor  ──────────────►  可持久化（to_bytes/from_bytes），提交后其他调用方可读
   │  绑定：原始长度 / k,m / 条带·分片大小 / 代际 generation /
   │        每分片哈希 / 每条带哈希 / 全内容哈希 / manifest 根
   ▼
分片键 = {prefix}/objects/{name}/gen/{generation}/shards/{shard}/{stripe}
```

- **代际（generation）**：每次 `put` 生成全新 generation，分片键按代际隔离。同名再写产生新代际；旧 descriptor 永远读到旧内容。
- **提交可见性**：分片先写，descriptor 先写代际记录、最后写 latest 指针（提交点）。提交前的内容对任何读取者不可见；中途失败会尽力清理暂存分片。
- **完整性依据**：分片段哈希（解码前剔除坏片/外来片）→ 条带哈希（解码后确认）→ 全内容哈希（整对象读取时确认）→ manifest 根（descriptor 自校验，防篡改）。
- **修复（repair）**：只为已提交的代际补写**哈希与 descriptor 一致**的分片，只写该代际的键。修复不改变对象含义；迟到的旧代际修复写不到新内容（键空间隔离）；修复失败体现在 `RepairResult` 中，与对象内容无关。

## 流式与资源上界

- 写入端、读取端都是真实的 `await` 等待：源不产出就等，消费者不取就不动。
- 内存占用只随配置增长，**不随对象大小增长**：
  - 写路径 ≈ `stripe_size + m × shard_size`（一条带明文 + m 个编码片）；
  - 读路径 ≈ `stripe_size + k × shard_size`（一条带 + k 个分片段）；
  - `stripe_size = k × shard_size`，分片大小与条带大小均可配置。
- 范围读取只取覆盖该范围的条带，不会先把所有片读进内存；慢消费者不会迫使内核囤积已恢复内容（无预取，逐条带推进）。
- 读取会话（`ReadSession`）与修复操作（`RepairOperation`）各自独立取消；已交付给消费者的字节不会被任何取消撤回。

## 快速上手

```python
import asyncio
from stripe_recovery_core import InMemoryBackend, KernelConfig, StripeKernel

async def main():
    # 外部存储由调用方提供：任何实现 read/write/delete 三个异步方法的对象
    kernel = StripeKernel(KernelConfig(k=4, m=7, shard_size=256 * 1024),
                          InMemoryBackend())

    # 写入：字节流进，descriptor 出（提交后才对他人可见）
    async def source():
        for i in range(0, 1_000_000, 64_000):
            yield make_chunk(i)          # 你的数据；慢产出会自然限速
    result = await kernel.put("my-object", source())
    persisted = result.descriptor.to_bytes()   # 持久保存它

    # 读取：只要中间一段，流式消费，报告随取随有
    async with kernel.read(result.descriptor, offset=10_000, length=500_000) as session:
        async for chunk in session:
            await send_downstream(chunk)       # 你的慢消费者不会撑爆内存
    report = session.report
    assert report.fully_confirmed              # 本次返回已满足完整性要求
    print(report.shards_used, report.shards_unavailable, report.confirmed_ranges)

    # 缺片修复：独立操作，可取消，绝不改变对象含义
    repair = kernel.repair(result.descriptor)
    repair_result = await repair.run()
    print(repair_result.status, repair_result.pieces_repaired)

    # 同名重写：旧 descriptor 仍读旧内容
    newer = await kernel.put("my-object", b"next generation content")
    old_bytes = await kernel.read_bytes(result.descriptor)   # 仍是旧内容

asyncio.run(main())
```

接入真实存储只需实现协议（见 `stripe_recovery_core.backend.StorageBackend`）：

```python
class MyBackend:
    async def read(self, key: str) -> bytes | None: ...   # 不存在返回 None；抛异常=存储故障
    async def write(self, key: str, data: bytes) -> None: ...
    async def delete(self, key: str) -> None: ...
```

## 结果与错误的语义

| 情形 | 表现 |
|---|---|
| 成功 | `WriteResult` / `ReadReport` / `RepairResult`，字段可核对 |
| 好片不足（含被剔除的坏片） | `UnrecoverableError`，`detail` 含条带号与每片失败原因 |
| 外部存储故障 | `StorageError`（与"分片不存在"严格区分） |
| 内容/描述与完整性依据冲突 | `IntegrityError` / `DescriptorError`，失败关闭，不交付字节 |
| 主动取消 | `OperationCancelled`（读）/ `RepairStatus.CANCELLED`（修复） |
| 调用契约错误 | `UsageError` |

每个错误都有 `.kind`（`ErrorKind` 枚举）。修复的终态是 `COMPLETED / PARTIAL / CANCELLED`：`PARTIAL` 会列出 `unrecoverable_stripes` 与 `write_failures`——这表示**存储还没补齐**，而不是对象内容变了。

统计与资源：`kernel.stats`（累计计数：写入对象数、编码字节、交付字节、解码条带、修复片数……）与 `kernel.resource_report()`（活动中的 put/read/repair、在途分片操作、持有的条带缓冲）。操作结束或取消后全部归零，测试可直接断言。

## 配置

`KernelConfig`：`k`（数据片数）、`m`（总片数，1 ≤ k ≤ m ≤ 256）、`shard_size`（每分片每条纹字节数）、`read_chunk_size`、`max_in_flight_piece_ops`（存储并发上限）、`repair_stripe_concurrency`、`key_prefix`、`codec_id`。

纠删数学是可插拔的：默认内置纯 Python Cauchy Reed-Solomon（GF(2⁸)，系统性矩阵，任意 k 片可恢复，编码确定性——重编码逐字节一致）。生产环境如需更高吞吐，可注入封装 zfec 等成熟库的 codec（实现 `StripeCodec` 协议：`codec_id` + `encode` + `decode`），descriptor 会记录 codec 标识，不匹配的 kernel 拒绝读取。

## 运行验证

```bash
pip install -e .[test]          # 或：pip install pytest 后直接用仓库
python -m pytest tests/         # 50 项验证
SRC_RUN_HUGE=1 python -m pytest tests/test_streaming.py   # 追加 300 MiB 端到端项
```

验证覆盖：1003 字节 4-of-7 往返与逐字节确定性重编码；描述符持久化与防篡改；缺片/坏片/跨代际混片（检测→剔除→恢复或失败关闭，绝不返回外来字节）；存储故障与不可恢复的区分；同名多写的代际隔离；未提交内容不可见；迟到修复不覆盖新内容；读写修复并发一致性；读/修复独立取消；慢生产者/慢消费者背压；16 MiB（及可选 300 MiB）对象的按块写入、中部读取、内存上界与资源回收。

## 已知边界

- descriptor 内含逐条带清单，大小随条带数线性增长（1 MiB 条带下 256 MiB 对象约 130 KB）；超大对象可后续把清单外置到存储、descriptor 只留根哈希。
- 内置纯 Python codec 面向正确性与零依赖；GB 级写入建议注入 C 实现的 codec。
- 存储后端需保证单次 `write` 返回后同键可读（最终一致存储需要调用方自行处理读写 quorum）。
