# CryptoDataGet-Binance

A comprehensive Binance market data collection toolkit — efficient crypto data acquisition based on the official Binance API.

---

## Overview

CryptoDataGet-Binance is a modular data pipeline for collecting and storing cryptocurrency market data from Binance. It supports spot klines, options, order book snapshots, trade history, aggregated trades, and funding rates, with Apache Feather as the default storage format for fast pandas DataFrame serialization.

---

## Modules

### CoinKline — Spot Kline Data

Fetches OHLCV candlestick data across multiple timeframes:

- 1-minute (high-frequency)
- 1-hour (mid-frequency)
- Daily (long-term trend)

```bash
python CoinKline/GetCoinsKline.py
```

### CoinOptions — Options Market Data

Collects options market data including:

- Options kline data
- Historical trade records
- Options chain information

```bash
python CoinOptions/GetHistoricalTradesData_serial.py
```

### CoinOrderbook — Order Book & Trade Data

Captures order book and trade data:

- **Depth snapshots** — bid/ask order book
- **Historical trades** — tick-by-tick records
- **Aggregated trades** — merged trade data with proxy rotation support

```bash
# Aggregated trades with Clash proxy rotation
python CoinOrderbook/GetAggTradesData_ms.py

# Merge feather files
python CoinOrderbook/feather_utils.py
```

### fundingrate — Funding Rate History

Collects historical funding rates across:

- **USDS-M** — USDT-margined perpetuals
- **Coin-M** — Coin-margined perpetuals
- **Hyperliquid** — cross-platform funding rates

```bash
python fundingrate/GetFundingRateHistory.py
```

---

## Project Structure

```
CryptoDataGet-Binance/
├── CoinKline/                          # Spot OHLCV data
│   ├── GetCoinsKline.py
│   └── Settings.py
├── CoinOptions/                        # Options market data
│   ├── GetHistoricalTradesData_serial.py
│   └── Settings.py
├── CoinOrderbook/                      # Order book & trade data
│   ├── GetAggTradesData_ms.py
│   ├── feather_utils.py
│   └── Settings.py
├── fundingrate/                        # Funding rate history
│   ├── GetFundingRateHistory.py
│   └── Settings.py
├── export_spot_filtered_to_csv.py      # Export spot data to CSV
├── merge_feather_files.py              # Merge feather files utility
├── check.py
├── Settings.py                         # Global config
└── requirements.txt
```

---

## Installation

**Requirements:** Python 3.8+

```bash
pip install -r requirements.txt
```

| Package | Version | Purpose |
|---|---|---|
| binance-connector | >=3.12.0 | Official Binance API client |
| pandas | >=1.5.0 | Data processing and Feather I/O |
| pytz | >=2023.3 | Timezone handling (Asia/Hong_Kong) |
| schedule | >=1.2.0 | Scheduled task execution |
| tqdm | >=4.65.0 | Progress bars for long-running tasks |

---

## Configuration

### API Keys

Edit `Settings.py` in each module (most public market data endpoints work without authentication):

```python
api_dic = {
    'api_key': 'YOUR_API_KEY',
    'api_secret': 'YOUR_API_SECRET'
}
```

### Data Output Paths

```python
data_path = {
    'result_path_daily': Path(r'E:\Quant\data\CoinKline\daily'),
    'result_path_min':   Path(r'E:\Quant\data\CoinKline\1min'),
    # ...
}
```

### Proxy Configuration (CoinOrderbook)

`GetAggTradesData_ms.py` supports Clash proxy rotation for high-volume downloads:

```python
CLASH_PROXY_CONFIG = {
    'proxy_port': 7897,
    'api_port': 9097,
    'api_secret': 'your_clash_secret',
}
NUM_PROCESSES = 10       # concurrent download threads
REQUIRE_UNIQUE_IP = False  # allow IP reuse if not enough nodes
```

---

## Usage

### Fetch Kline Data

```python
from CoinKline.GetCoinsKline import get_kline_data

data = get_kline_data(symbol="BTCUSDT", interval="1h", limit=1000)
```

### Read & Merge Feather Files

```python
from CoinOrderbook.feather_utils import read_all_feather_files, merge_with_existing_and_save
from pathlib import Path

df = read_all_feather_files(
    Path("E:/Quant/data/CoinOrderbook/historical_trades"),
    "BTCUSDT"
)

merge_with_existing_and_save(data_dir, symbol, new_df, save_format='feather')
```

### Utility Scripts

```bash
python export_spot_filtered_to_csv.py   # export spot data to CSV
python merge_feather_files.py           # merge multiple feather files
```

---

## Data Output Structure

```
E:/Quant/data/
├── CoinKline/
│   ├── 1min/
│   ├── 1h/
│   └── daily/
├── CoinOptions/
│   ├── kline/
│   └── historical_trades/
├── CoinOrderbook/
│   ├── depth/
│   ├── trades/
│   └── agg_trades/
└── fundingrate/
    ├── usds_m/
    └── coin_m/
```

**File naming convention:**
`{symbol}_historical_trades_{YYYYMMDD}_{HHMMSS}_to_{YYYYMMDD}_{HHMMSS}.feather`

---

## Design Notes

- **Per-module Settings** — each module has its own `Settings.py` for independent configuration
- **Incremental updates** — historical trades use `fromId` pagination with local max-ID tracking
- **Rate limiting** — all modules enforce request intervals (default 0.2s) to stay within Binance's 400 req/min limit
- **Feather format** — fast serialization for active research; consider converting to Parquet for long-term archival

---

## License

MIT

---

> **Disclaimer:** This project is for research and educational purposes only. It does not constitute investment advice. Use at your own risk.
