# stripe-recovery-core

供 Python 程序嵌入的**纠删数据条带内核**。它在成熟纠删库（[zfec](https://pypi.org/project/zfec/)，GF(2⁸) 上的 Reed–Solomon 变体）之上，负责 zfec 本身不负责的部分：对象描述符、分片代际（generation）、提交可见性、**经确认的内容**、异步存储适配、流式范围读取，以及可取消的缺片修复。

它不是文件同步客户端，也不是命令行产品——只有库和可运行的验证。

## 为什么需要它：解码成功 ≠ 内容可信

`zfec.decode` 只证明“这 k 个块满足纠删码方程”，不证明它们来自哪一次编码。这正是观察到的两个现象的根因：

- 把**不同次编码**的片混在一起，只要凑够 k 份，zfec 照样解出字节；
- 改一片中的**一个字节**，数学解码通常不会报错。

本内核的返回内容必须逐层通过密码学核对，zfec 的原始输出永远不会直接交给调用方：

```
share block  ──content digest──▶ 位置内容必须等于条纹清单中记录的摘要
stripe manifest ──stripe root──▶ 必须等于描述符记录的该条纹根
object descriptor ──self digest──▶ 描述符自身摘要（即 generation id）
```

清单把“第 j 份片应该是什么内容”固定在某个描述符下；描述符摘要绑定全部条带根、对象名、编码参数与真实长度。不同次编码的片即使字节相同也只代表相同的块，一旦放到错误的位置、或属于不被该描述符引用的清单，就在内容摘要这一步被拒绝。末条纹零填充在返回前也会校验，真实长度只存在于描述符中。

哈希使用 BLAKE2b-256（标准库 `hashlib`），无额外密码学依赖。

## 安装

```bash
pip install .
# 开发/验证
pip install -e .
```

运行验证（功能场景，秒级）：

```bash
python verify.py
# 大对象（默认 300 MiB，需要约 n/k ≈ 1.75 倍对象大小的片块磁盘空间）
python verify.py --big --mib 300
# 单元测试
python -m unittest discover -t . -s tests -p 'test_*.py'
SRC_BIG=1 python -m unittest tests.test_big_object
```

## 快速开始

```python
import asyncio
from stripe_recovery_core import Kernel, KernelConfig, MemoryStorage, ReadStatus

async def main():
    storage = MemoryStorage()          # 也可实现自己的 AsyncStorage（S3/GCS/…）
    kernel = Kernel(storage, KernelConfig(block_size=4096))

    # 写入：流式、带背压；finish() 返回前对象不可见
    w = kernel.new_writer("report-2026-09", k=4, m=3)
    await w.write(first_chunk)
    await w.write(second_chunk)
    desc = await w.finish()

    # 其他调用者按已提交的对象名打开（总是拿到最新一次提交）
    desc = await kernel.open("report-2026-09")

    # 中间范围读取，不先收齐整个对象
    reader = kernel.read_range(desc, start=10_000_000, end=10_000_500)
    async for chunk in reader:
        await upstream.send(chunk)          # 真实的流式输出 + 背压
    report = await reader.report()
    assert report.status is ReadStatus.CONFIRMED
    report.used_shares        # 实际参与恢复的 (条纹, 片号)
    report.missing_shares     # 存储报告缺失
    report.corrupt_shares     # 存在但内容摘要不符
    report.confirmed_end      # 已确认到的绝对字节边界

    # 独立可取消的修复（只补缺，不改对象含义）
    h = kernel.repair(desc)
    repair_report = await h.run()

    await kernel.aclose()
```

## 公开概念

| 概念 | 含义 |
| --- | --- |
| `CodingParams(k, m, block_size)` | k 份数据 + m 份校验，n=k+m；任取 k 份已确认片可恢复；片块固定 `block_size` 字节（末条纹零填充） |
| `ObjectDescriptor` | 不可变、自证的对象描述符：对象名、k/m/block_size、真实长度、每条纹根、自身摘要。`descriptor.generation` 即其摘要十六进制 |
| `ObjectWriter` | `write(bytes)` 流式写入（有背压）、`finish()` 提交、`aclose()` 放弃；未提交的内容不可被 `open` 看见 |
| `RangeReader` | 异步迭代器，输出经过确认的字节块；`report()` 给出状态、已确认范围与逐片会计 |
| `RepairHandle` | 独立的修复扫描，可单独取消，返回每条纹结果；读时修复由内部 `RepairCoordinator` 后台跟踪 |
| `AsyncStorage` | 调用方实现的四个异步方法：`get / put / put_if_absent / delete`，不绑定任何云账户 |

存储键布局：

```
head/<name-hash>                   最新已提交描述符
gen/<generation>/desc              该代描述符
gen/<generation>/s/<i>/manifest    该代第 i 条纹的清单（n 个内容摘要）
blob/<content-digest>              内容寻址的片块（可被含相同块的代共享）
```

## 关键语义

**经确认的对象内容。** 只有通过清单/条带根/描述符三层校验的字节才会出现在迭代器中；非确认路径不返回任何“解码结果”。

- `confirmed`：全部请求字节已通过描述符绑定的哈希核对。
- `unrecoverable`：已确认片不足 k 份（存储是干净的 NotFound）。
- `unverifiable`：数学上解出来了，但重建块/重编码清单与描述符根不符，或尾部填充非零——字节被扣留。
- `storage_failure`：调用方存储后端报错，完整性尚无结论。
- `cancelled`：操作被取消。

**提交可见性与代际隔离。** `finish()` 先写片块与代内清单，再写描述符，最后更新 `head`；未完成时 `open` 找不到对象。同一对象名再次提交产生不同 generation；持有旧描述符（或用 `open(name, at_generation=...)`）永远读到旧内容。旧代的迟到修复只能写它自己 `gen/<old>/` 下的键以及它所引用的 `blob`，物理上碰不到新一代。

**缺片读取。** 范围读取只遍历与范围相交的条纹；部分范围若完全落在系统片中，直接读取所需数据片，不触发解码、不碰校验片。`ReadReport` 列出使用、缺失、损坏、取失败的片，以及 `confirmed_end`。

**读时修复。** `read_range(..., repair_on_read=True)` 在确认一个条纹后后台补回缺失片；修复与读取分属不同任务：关闭读取不会撤回已经交付的确认字节，也不会取消修复；修复只执行 generation 范围内的 `put_if_absent`，失败只体现在修复结果里，绝不重写描述符或 head，绝不表现为“对象内容变了”。

**流式与背压。** 写端按条纹编码，在途编码字节受 `max_inflight_bytes` 限制，慢存储会让 `write()` 真实等待；读端用字节有界队列（`max_output_bytes`），慢消费者会让恢复协程等待。内存占用随 `block_size`、k/m 与预算增长，**不随对象大小增长**。

**大对象。** 300 MiB 对象（k=4，m=3，block=64 KiB）的实测：分块写入约 10 s，写期间 RSS 增长约 7 MiB；从对象 40% 处读 4 MiB 耗时约 0.1 s，仅触及相交条纹；关闭读取后活动资源归零。见 `python verify.py --big`。

**取消与资源。** 读取取消（`aclose`）只取消该读取；修复取消（`RepairHandle.cancel`）只取消该修复。`Kernel.resources()` 随时返回活动 writer/reader/repair 数、在途存储字节与缓存编码器数；每个 writer/reader/repair 都带可核对的 stats。

## 存储后端契约

```python
class AsyncStorage(Protocol):
    async def get(self, key: str) -> bytes: ...        # 不存在抛 NotFound
    async def put(self, key: str, value: bytes) -> None: ...
    async def put_if_absent(self, key: str, value: bytes) -> bool: ...
    async def delete(self, key: str) -> None: ...
```

`put_if_absent` 必须具备原子 CAS 语义（修复安全依赖它）；参考实现 `FileStorage` 用 `O_CREAT|O_EXCL` 模拟。后端非 `NotFound` 异常会被归一化为存储失败，与不可恢复/不可确认明确区分。

## 目录

```
stripe_recovery_core/   库代码（描述符、编码、恢复、读写、修复、存储）
tests/                  单元/场景测试（大对象测试需 SRC_BIG=1）
examples/               嵌入示例
verify.py               可直接运行的端到端验证
```

## 已验证的一致性结论

- 1003 字节、k=4/m=3：删任意 3 份均可恢复；修复重写的片与全新 zfec 编码逐字节一致；删 4 份得到明确的不可恢复结论。
- 同对象名两次编码后混入跨代片：读取返回 `unrecoverable`，不返回任何字节；旧描述符仍读旧内容。
- 单片翻转 1 字节：报告明确指出损坏片，冗余足够时仍返回确认内容，超容差时返回类型化状态而非垃圾字节。
- 多次提交、并行读修复、取消竞争之后：返回内容、描述符与实际存储保持一致（`verify.py`，45 项检查）。

## 许可

MIT。
