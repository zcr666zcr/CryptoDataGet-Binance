# 币安交易数据获取工具说明文档

## 概述

本工具集提供了从币安API获取各类交易数据的功能，包括深度数据、成交数据、历史成交数据等。所有工具都支持配置化使用，参数可通过`config.py`统一配置。

---

## 模块说明

### 1. GetDepthData - 深度数据（订单簿）

**功能**：获取币安订单簿的买卖盘数据（实时数据，不支持历史查询）

**主要方法**：
- `get_depth_by_limit(limit)`: 按深度数量获取当前订单簿
- `download_and_save(limit, save_format)`: 下载并保存数据

**使用示例**：
```python
from GetDepthData import GetDepthData

gdd = GetDepthData(symbol='BTCUSDT')
df = gdd.download_and_save(limit=5000)  # 获取5000档深度数据
```

**逻辑说明**：
- 调用币安`depth`接口获取当前订单簿快照
- 返回买盘(bids)和卖盘(asks)数据
- 数据包含价格、数量、时间戳等信息

---

### 2. GetTradesData - 近期成交数据

**功能**：获取币安最近的成交数据（仅支持最近数据，不支持时间范围查询）

**主要方法**：
- `get_trades_by_limit(limit)`: 按笔数获取最近成交数据（最大1000条）
- `download_and_save(limit, save_format)`: 下载并保存数据

**使用示例**：
```python
from GetTradesData import GetTradesData

gtd = GetTradesData(symbol='BTCUSDT')
df = gtd.download_and_save(limit=1000)  # 获取最近1000条成交数据
```

**逻辑说明**：
- 调用币安`trades`接口获取最近成交数据
- 数据包含成交ID、价格、数量、时间等信息
- **注意**：此接口只返回最近的数据，不支持指定时间范围

---

### 3. GetHistoricalTradesData - 历史成交数据

**功能**：获取币安历史成交数据（需要API密钥，支持两种模式）

**主要方法**：
- `get_historical_trades_by_fromid(from_id, limit)`: 模式1-按fromId分页下载
- `get_historical_trades_by_time_range(start_time, end_time, limit)`: 模式2-按时间范围下载
- `download_by_fromid(from_id, limit, save_format)`: 模式1便捷方法
- `download_by_time_range(start_time, end_time, save_format)`: 模式2便捷方法
- `get_yesterday_data(save_format)`: 获取昨天数据
- `get_data_by_date(date, save_format)`: 获取指定日期数据

**使用示例**：
```python
from GetHistoricalTradesData import GetHistoricalTradesData
from datetime import datetime, timedelta

ghtd = GetHistoricalTradesData(symbol='BTCUSDT')

# 模式1：从指定ID开始下载
df1 = ghtd.download_by_fromid(from_id=1000, limit=1000)

# 模式2：按时间范围下载
start_time = datetime(2024, 1, 1, 0, 0, 0)
end_time = datetime(2024, 1, 2, 0, 0, 0)
df2 = ghtd.download_by_time_range(start_time, end_time)

# 便捷方法
df3 = ghtd.get_yesterday_data()
df4 = ghtd.get_data_by_date('2024-01-01')
```

**逻辑说明**：
- **模式1（fromId分页）**：从指定ID开始，按ID递增顺序批量下载数据
- **模式2（时间范围）**：
  1. 使用二分查找定位`end_time`对应的订单ID
  2. 从该ID开始往前批量下载（逐步减小fromId）
  3. 当数据时间 ≤ `start_time`时停止
  4. 最后截断DataFrame，只保留时间范围内的数据
- **注意**：此接口需要API密钥，请在`Settings.py`中配置或通过环境变量提供

---

### 4. GetAggTradesData_ms - 成交归集数据（毫秒级）

**功能**：获取币安成交归集数据，支持毫秒级精度（所有时间字段精确到毫秒）

**主要方法**：
- `get_agg_trades_by_limit(limit)`: 模式1-按笔数获取最近数据
- `get_agg_trades_by_time_range(start_time, end_time, limit)`: 模式2-按时间范围下载
- `download_by_limit(limit, save_format)`: 模式1便捷方法
- `download_by_time_range(start_time, end_time, save_format)`: 模式2便捷方法
- `get_yesterday_data(save_format)`: 获取昨天数据
- `get_data_by_date(date, save_format)`: 获取指定日期数据

**使用示例**：
```python
from GetAggTradesData_ms import GetAggTradesData_ms
from datetime import datetime

gatd = GetAggTradesData_ms(symbol='BTCUSDT')

# 模式1：按笔数下载
df1 = gatd.download_by_limit(limit=1000)

# 模式2：按时间范围下载（支持毫秒）
start = datetime(2025, 11, 3, 23, 58, 58, 999000)
end = datetime(2025, 11, 4, 0, 0, 0, 0)
df2 = gatd.download_by_time_range(start, end)
```

**逻辑说明**：
- **模式1**：调用`agg_trades`接口获取最近N条归集成交数据
- **模式2**：
  1. 使用`startTime`和`endTime`参数指定时间范围
  2. 自动分页获取完整时间范围内的数据
  3. 通过`last_timestamp + 1`作为下一批次的起始时间继续获取
  4. 最后过滤确保数据在时间范围内
- **特点**：所有时间字段格式化到毫秒级精度（`YYYY-MM-DD HH:MM:SS.mmm`）

**异步版本**
- **GetAggTraderData_sync**这个代码里我采用了异步加多线程的方法
---

### 5. GetDailyTradeCount - 每日交易笔数统计

**功能**：获取币安交易对的每日交易笔数统计

**主要方法**：
- `get_24hr_trade_count(symbol)`: 方式1-获取24小时滚动窗口的交易笔数（快速）
- `get_daily_trade_count_by_date(symbol, target_date)`: 方式2-统计指定日期的实际交易笔数（精确）

**使用示例**：
```python
from GetDailyTradeCount import GetDailyTradeCount
from datetime import datetime, timedelta

gdtc = GetDailyTradeCount()

# 方式1：快速获取24小时滚动窗口数据
count1 = gdtc.get_24hr_trade_count('BTCUSDT')

# 方式2：精确统计指定日期
yesterday = datetime.now() - timedelta(days=1)
count2 = gdtc.get_daily_trade_count_by_date('BTCUSDT', yesterday)
```

**逻辑说明**：
- **方式1**：使用`ticker/24hr`接口，返回最近24小时的统计数据（快速但不精确到具体日期）
- **方式2**：通过`aggTrades`接口统计指定日期00:00:00到23:59:59.999的所有交易（精确但较慢）

---

## 配置说明

所有模块的参数都可通过`config.py`统一配置：

- `TIMEZONE`: 时区设置（默认：'Asia/Hong_Kong'）
- `DEFAULT_SAVE_FORMAT`: 默认保存格式（'feather' 或 'csv'）
- `RETRY_SLEEP_TIME`: 请求失败后重试等待时间（秒）
- 各模块的`default_limit`、`default_symbol`等参数

---

## 改进空间

### 1. 批量下载不同Symbol并整合到一个Feather文件

**当前问题**：
- 每个模块每次只能下载一个symbol的数据
- 需要手动循环调用才能下载多个symbol
- 数据分散在多个文件中

**改进方案**：
```python
def batch_download_symbols(symbols, start_time, end_time):
    """
    批量下载多个symbol的数据并整合
    """
    all_data = []
    for symbol in symbols:
        gatd = GetAggTradesData_ms(symbol=symbol)
        df = gatd.get_agg_trades_by_time_range(start_time, end_time)
        if not df.empty:
            all_data.append(df)
    
    # 整合所有数据
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # 保存到单个feather文件
    combined_df.to_feather('all_symbols_data.feather')
    return combined_df
```

**关键点**：
- 使用`pd.concat()`合并多个DataFrame
- 确保所有DataFrame的列结构一致
- 可以考虑按symbol分组存储或添加symbol列便于后续筛选

---

### 2. 批量下载以天为单位的高频交易数据

**当前问题**：
- 需要手动指定每天的日期范围
- 跨多天下载需要多次调用
- 数据分散在多个文件中

**改进方案**：
```python
def batch_download_daily_data(symbol, start_date, end_date):
    """
    批量下载指定日期范围内每天的高频交易数据
    """
    from datetime import datetime, timedelta
    
    gatd = GetAggTradesData_ms(symbol=symbol)
    all_daily_data = []
    
    current_date = start_date
    while current_date <= end_date:
        # 计算当天的开始和结束时间
        day_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = current_date.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        print(f"正在下载 {current_date.strftime('%Y-%m-%d')} 的数据...")
        df = gatd.get_agg_trades_by_time_range(day_start, day_end)
        
        if not df.empty:
            # 添加日期列便于后续分析
            df['日期'] = current_date.strftime('%Y-%m-%d')
            all_daily_data.append(df)
        
        # 移动到下一天
        current_date += timedelta(days=1)
        
        # 避免请求过快
        time.sleep(1)
    
    # 整合所有数据
    if all_daily_data:
        combined_df = pd.concat(all_daily_data, ignore_index=True)
        # 保存到单个feather文件
        filename = f'{symbol}_daily_{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}.feather'
        combined_df.to_feather(filename)
        return combined_df
    
    return pd.DataFrame()
```

**关键点**：
- 循环遍历日期范围，每天调用一次API
- 添加日期列便于后续按日期分组分析
- 考虑添加进度条和错误重试机制
- 可以添加断点续传功能（记录已下载的日期）
- 对于大量数据，可以考虑分批保存或使用数据库存储

**进一步优化**：
- 使用多线程/异步下载提高效率
- 添加数据验证和完整性检查
- 支持增量更新（只下载缺失的日期）
- 添加数据压缩和分区存储（按日期或symbol分区）

