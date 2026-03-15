import os
from pathlib import Path
import pandas as pd
from CoinKline.Settings import data_path

SYMS = ['BNBUSDT', 'BTCUSDT', 'DOGEUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT']

def compute_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def export_with_rsi(src_dir: Path, out_dir: Path, periods: list[int], suffix: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    files = list(src_dir.glob('*.feather'))
    print(f'目录 {src_dir} 发现 {len(files)} 个文件')
    frames = []
    for f in files:
        try:
            df = pd.read_feather(f)
        except Exception as e:
            print(f'读取失败 {f.name}: {e}')
            continue
        if '货币对' not in df.columns:
            if 'symbol' in df.columns:
                df.rename(columns={'symbol': '货币对'}, inplace=True)
            else:
                print(f'跳过 {f.name}: 未找到货币对列')
                continue
        if '开盘时间' not in df.columns or '收盘价' not in df.columns:
            print(f'跳过 {f.name}: 未找到必要列')
            continue
        df = df[df['货币对'].isin(SYMS)]
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        print('无匹配数据')
        return
    all_df = pd.concat(frames, ignore_index=True)
    all_df['开盘时间'] = pd.to_datetime(all_df['开盘时间'])
    all_df['收盘价'] = pd.to_numeric(all_df['收盘价'], errors='coerce')
    def add_rsi(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values('开盘时间').reset_index(drop=True)
        close = group['收盘价']
        for p in periods:
            group[f'RSI_{p}{suffix}'] = compute_rsi(close, p)
        return group
    all_df = all_df.groupby('货币对', group_keys=False).apply(add_rsi)
    if '请忽略' in all_df.columns:
        all_df = all_df.drop(columns=['请忽略'])
    for sym in SYMS:
        g = all_df[all_df['货币对'] == sym]
        if g.empty:
            continue
        out_csv = out_dir / f'spot_{suffix}_{sym}_with_rsi.csv'
        g.to_csv(out_csv, index=False, encoding='utf-8-sig')
        print(f'{sym} 已保存到 {out_csv}')

def main():
    daily_dir = data_path['result_path_daily']
    hour_dir = data_path['result_path_hour']
    out_dir = Path(r'E:\Quant\DMIF')
    export_with_rsi(daily_dir, out_dir, [6, 12, 14, 24], 'd')
    export_with_rsi(hour_dir, out_dir, [12, 36, 60], 'h')

if __name__ == '__main__':
    main()