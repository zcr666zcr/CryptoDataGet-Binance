# CryptoDataGet-Binance

币安加密货币数据获取工具集

## 功能模块

- **CoinKline**: K线数据获取
- **CoinOptions**: 期权数据获取
- **CoinOrderbook**: 订单簿数据获取
- **fundingrate**: 资金费率数据获取

## 安装依赖

```bash
pip install -r requirements.txt
```

## 依赖说明

- binance-connector >= 3.12.0
- pandas >= 1.5.0
- pytz >= 2023.3
- schedule >= 1.2.0
- tqdm >= 4.65.0

## 项目说明

本项目用于从币安交易所获取各类加密货币市场数据，包括现货K线、期权、订单簿和资金费率等数据。
