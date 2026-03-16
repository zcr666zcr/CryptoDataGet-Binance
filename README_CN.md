# CryptoDataGet-Binance

币安加密货币市场数据获取工具集 - 基于 Binance API 的高效数据采集方案

## 项目概述

本项目是一个完整的加密货币市场数据收集工具包，专为 Binance 交易所设计。支持获取和存储 K 线（蜡烛图）、期权、订单簿和资金费率数据，采用 Apache Feather 格式实现高效的 pandas DataFrame 序列化。

## 功能模块

### 📊 CoinKline - K线数据获取

现货市场 K 线数据模块，支持多时间粒度：

- **1分钟线**: 高频交易数据
- **1小时线**: 中周期分析数据
- **日线**: 长期趋势数据

**主要脚本**:
```bash
python CoinKline/GetCoinsKline.py
```

### 📈 CoinOptions - 期权数据获取

期权市场数据模块，包含：

- 期权 K 线数据
- 历史成交数据
- 期权链信息

**主要脚本**:
```bash
# 获取期权历史成交数据（串行版本）
python CoinOptions/GetHistoricalTradesData_serial.py
```

### 📚 CoinOrderbook - 订单簿数据获取

订单簿深度数据模块，支持：

- **深度数据**: 买卖盘快照
- **历史成交**: 逐笔成交记录
- **聚合成交**: 合并后的成交数据（支持代理轮换）

**主要脚本**:
```bash
# 聚合成交数据（使用 Clash 代理轮换）
python CoinOrderbook/GetAggTradesData_ms.py

# Feather 文件合并工具
python CoinOrderbook/feather_utils.py
```

### 💰 fundingrate - 资金费率数据

资金费率历史数据模块，覆盖：

- **USDS-M**: USDT 本位合约
- **Coin-M**: 币本位合约
- **Hyperliquid**: 跨平台资金费率

**主要脚本**:
```bash
python fundingrate/GetFundingRateHistory.py
```

## 架构设计

### 模块结构

```
CryptoDataGet-Binance/
├── CoinKline/              # 现货 K 线数据
│   ├── GetCoinsKline.py
│   └── Settings.py
├── CoinOptions/            # 期权数据
│   ├── GetHistoricalTradesData_serial.py
│   └── Settings.py
├── CoinOrderbook/          # 订单簿数据
│   ├── GetAggTradesData_ms.py
│   ├── feather_utils.py
│   └── Settings.py
├── fundingrate/            # 资金费率数据
│   ├── GetFundingRateHistory.py
│   └── Settings.py
└── requirements.txt
```

### 核心设计模式

#### 1. Settings-per-Module

每个模块拥有独立的 `Settings.py`，包含：

- `api_dic`: Binance API 认证信息
- `data_path`: 数据输出目录（绝对路径，如 `E:\Quant\data\...`）
- `CONFIG_PATHS`: 运行时目录配置（日志、临时文件）
- 模块专属配置字典（如 `DEPTH_CONFIG`, `TRADES_CONFIG`）

#### 2. Feather 文件格式

默认使用 Apache Feather 格式存储，优势：

- 高效的 DataFrame 序列化/反序列化
- 跨语言兼容性
- 支持 CSV 作为降级方案

#### 3. 增量数据更新

- 历史成交数据使用 `fromId` 分页机制
- 本地维护最大 ID 跟踪
- 文件名包含时间范围：`{symbol}_historical_trades_{start}_to_{end}.feather`
- `feather_utils.py` 提供文件合并管理工具

#### 4. API 速率限制

所有模块实现请求间隔控制（默认 0.2 秒），避免触发 Binance API 限制（大多数端点 400 请求/分钟）。

## 安装指南

### 环境要求

- Python 3.8+
- Windows/Linux/macOS

### 安装依赖

```bash
pip install -r requirements.txt
```

### 核心依赖

| 包名 | 版本 | 用途 |
|------|------|------|
| binance-connector | >=3.12.0 | 官方 Binance API 客户端 |
| pandas | >=1.5.0 | 数据处理与 Feather 格式 |
| pytz | >=2023.3 | 时区处理（Asia/Hong_Kong） |
| schedule | >=1.2.0 | 定时任务调度 |
| tqdm | >=4.65.0 | 长任务进度条 |

## 配置说明

### API 配置

在每个模块的 `Settings.py` 中配置：

```python
api_dic = {
    'api_key': 'your_api_key_here',
    'api_secret': 'your_api_secret_here'
}
```

> ⚠️ **注意**: 大部分公共市场数据端点无需认证即可访问

### 数据输出路径

在 `Settings.py` 中设置绝对路径：

```python
data_path = "E:\\Quant\\data\\CoinKline"
```

### 代理配置（CoinOrderbook）

`GetAggTradesData_ms.py` 支持 Clash 代理轮换：

```python
CLASH_PROXY_CONFIG = {
    'api_port': 9097,
    # ... 其他配置
}
REQUIRE_UNIQUE_IP = False  # IP 不足时允许复用
```

## 使用示例

### K 线数据获取

```python
from CoinKline.GetCoinsKline import get_kline_data

# 获取 BTCUSDT 1小时数据
data = get_kline_data(
    symbol="BTCUSDT",
    interval="1h",
    limit=1000
)
```

### Feather 文件操作

```python
from CoinOrderbook.feather_utils import (
    read_all_feather_files,
    merge_with_existing_and_save
)
from pathlib import Path

# 读取某币种所有 feather 文件
df = read_all_feather_files(
    Path("E:/Quant/data/CoinOrderbook/现货历史成交数据"),
    "BTCUSDT"
)

# 合并新数据并保存
merge_with_existing_and_save(
    data_dir,
    symbol,
    new_df,
    save_format='feather'
)
```

### 导出为 CSV

```python
# 导出现货数据到 CSV
python export_spot_filtered_to_csv.py

# 合并多个 feather 文件
python merge_feather_files.py
```

## 数据输出结构

数据默认输出到 `E:\Quant\data\`（可在 Settings.py 中配置）：

```
E:/Quant/data/
├── CoinKline/              # 现货 OHLCV 数据
│   ├── 1min/
│   ├── 1h/
│   └── daily/
├── CoinOptions/            # 期权市场数据
│   ├── kline/
│   └── historical_trades/
├── CoinOrderbook/          # 订单簿快照和成交历史
│   ├── depth/
│   ├── trades/
│   └── agg_trades/
└── fundingrate/            # 资金费率历史
    ├── usds_m/
    └── coin_m/
```

## 文件命名规范

- **历史成交数据**: `{symbol}_historical_trades_{YYYYMMDD}_{HHMMSS}_to_{YYYYMMDD}_{HHMMSS}.feather`
- **配置文件**: 运行时创建 `config/{logs,runtime,data,output}/` 目录
- **币种池**: CSV 文件缓存在 `data/symbol_pool/`

## 开发说明

### 运行单元模块

```bash
# K 线数据（每日更新）
python CoinKline/GetCoinsKline.py

# 期权历史成交
python CoinOptions/GetHistoricalTradesData_serial.py

# 订单簿聚合成交（使用 Clash 代理）
python CoinOrderbook/GetAggTradesData_ms.py

# 资金费率历史
python fundingrate/GetFundingRateHistory.py
```

### 工具脚本

```bash
# 按日期合并 feather 文件
python CoinOrderbook/feather_utils.py

# 导出现货数据为 CSV
python export_spot_filtered_to_csv.py

# 合并多个 feather 文件
python merge_feather_files.py
```

## 注意事项

1. **API 限制**: 请遵守 Binance API 速率限制，避免 IP 被封禁
2. **数据存储**: Feather 文件不适合长期归档，建议定期转换为 Parquet 格式
3. **代理使用**: 大量数据获取时建议使用代理池分散请求
4. **时区处理**: 所有时间戳默认使用 Asia/Hong_Kong 时区

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request。

---

**免责声明**: 本项目仅供学习和研究使用，不构成任何投资建议。使用本工具获取的数据进行交易决策风险自负。
