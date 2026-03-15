# 异步版本使用说明

## 概述

`GetAggTradesData_ms_async.py` 是 `GetAggTradesData_ms.py` 的异步版本，使用 `asyncio` 和 `aiohttp` 实现并发下载，可以大幅提升大时间范围数据的下载效率。

## 性能优势

### 为什么异步版本更快？

1. **并发请求**：将时间范围分割成多个子时间段，同时发起多个HTTP请求
2. **非阻塞IO**：等待网络响应时，可以处理其他请求，不浪费时间
3. **适合IO密集型任务**：下载数据主要是网络IO操作，异步非常适合

### 适用场景

- ✅ **大时间范围下载**（如多天、多周）：优势明显，可提升 3-10 倍速度
- ✅ **高并发限制**：通过 `max_concurrent` 参数控制并发数，避免触发API限流
- ⚠️ **小时间范围**（几分钟）：优势不明显，可能因为额外开销反而稍慢

## 安装依赖

异步版本使用 `binance-connector` 库（与同步版本相同），无需额外依赖：

```bash
pip install -r requirements.txt
```

**注意**：异步版本通过 `ThreadPoolExecutor` 在线程池中运行 `binance.spot` 的同步调用，因此不需要 `aiohttp`。

## 使用方法

### 基本用法

```python
from GetAggTradesData_ms_async import GetAggTradesData_ms_async
from datetime import datetime

# 创建异步工具实例
gatd = GetAggTradesData_ms_async(
    symbol='BTCUSDT',
    max_concurrent=10  # 并发数，默认10（可根据API限制调整）
)

# 按时间范围下载
start = datetime(2025, 11, 1, 0, 0, 0, 0)
end = datetime(2025, 11, 4, 23, 59, 59, 999000)

df = gatd.download_by_time_range(
    start, 
    end,
    split_duration_hours=1  # 每1小时为一个时间段，并发下载
)
```

### 参数说明

#### `__init__` 参数
- `symbol`: 交易对，默认从配置文件读取
- `max_concurrent`: 最大并发请求数，默认10
  - 建议值：5-20（根据网络和API限制调整）
  - 太小：并发优势不明显
  - 太大：可能触发API限流（429错误）

#### `download_by_time_range` 参数
- `start_time`: 起始时间（datetime对象或时间戳毫秒）
- `end_time`: 结束时间（datetime对象或时间戳毫秒）
- `save_format`: 保存格式，'feather' 或 'csv'，默认从配置文件读取
- `split_duration_hours`: 分割时间段的小时数，默认1小时
  - 建议值：0.5-2小时（根据数据密度调整）
  - 太小：时间段过多，并发开销增加
  - 太大：单个时间段内数据过多，仍需要多次分页

## 性能对比测试

运行对比测试脚本：

```bash
python compare_async_vs_sync.py
```

这会同时测试同步版本和异步版本，并显示：
- 耗时对比
- 性能提升倍数
- 数据完整性验证

## 工作原理

1. **时间分割**：将大时间范围分割成多个子时间段（默认每1小时一段）
2. **并发请求**：使用 `ThreadPoolExecutor` + `asyncio` 在线程池中并发运行 `binance.spot` 调用
3. **线程安全**：每个线程使用独立的 `Client` 实例，确保线程安全
4. **并发控制**：使用 `asyncio.Semaphore` 控制并发数，避免触发API限流
5. **自动分页**：每个时间段内部仍需要分页获取（因为每批最多1000条）
6. **数据合并**：将所有时间段的数据合并，按时间排序

**技术细节**：
- 使用 `binance.spot.Spot` 客户端（与同步版本保持一致）
- 通过 `asyncio.run_in_executor()` 在线程池中运行同步的 `binance.spot` 调用
- 每个线程创建独立的 `Client` 实例，避免线程安全问题

## 注意事项

1. **API限流**：如果遇到 429 错误（请求过于频繁），可以：
   - 减小 `max_concurrent`（如改为5）
   - 增大 `split_duration_hours`（如改为2小时）
   - 在配置文件中增加 `request_interval`

2. **数据完整性**：
   - 异步版本会确保数据在指定时间范围内
   - 自动按时间排序
   - 与同步版本获取的数据应该一致

3. **网络环境**：
   - 网络延迟高时，异步版本优势更明显
   - 网络非常快时，优势可能不明显

## 示例：下载多天数据

```python
from GetAggTradesData_ms_async import GetAggTradesData_ms_async
from datetime import datetime, timedelta

gatd = GetAggTradesData_ms_async(
    symbol='BTCUSDT',
    max_concurrent=10  # 10个并发请求
)

# 下载最近7天的数据
end_time = datetime.now()
start_time = end_time - timedelta(days=7)

print(f"开始下载 {start_time} 到 {end_time} 的数据...")

df = gatd.download_by_time_range(
    start_time,
    end_time,
    split_duration_hours=1  # 每1小时一段，共约168段，并发下载
)

print(f"下载完成！共 {len(df)} 条记录")
```

## 与同步版本的对比

| 特性 | 同步版本 | 异步版本 |
|------|---------|---------|
| 小时间范围（几分钟） | 较快 | 稍慢（额外开销） |
| 大时间范围（多天） | 较慢（顺序请求） | **快 3-10 倍** |
| 代码复杂度 | 简单 | 稍复杂 |
| 依赖 | binance-connector | binance-connector（相同） |
| API客户端 | binance.spot | binance.spot（相同） |
| API限流处理 | 自动重试 | 自动重试 + 并发控制 |

## 建议

- **小时间范围**（< 1小时）：使用同步版本
- **大时间范围**（> 1天）：使用异步版本
- **不确定时**：运行 `compare_async_vs_sync.py` 测试一下

