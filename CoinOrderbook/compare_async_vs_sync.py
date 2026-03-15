"""
对比测试：异步版本 vs 同步版本
用于验证异步版本是否能提升下载效率
"""
import time
from datetime import datetime
from GetAggTradesData_ms import GetAggTradesData_ms
from GetAggTradesData_ms_async import GetAggTradesData_ms_async


def compare_performance():
    """
    对比同步版本和异步版本的性能
    """
    print("=" * 60)
    print("异步版本 vs 同步版本性能对比测试")
    print("=" * 60)
    
    # 测试时间范围（可以选择不同的时间范围测试）
    # 小范围测试（几分钟）
    #start = datetime(2025, 11, 3, 23, 58, 58, 999000)
    #end = datetime(2025, 11, 4, 0, 0, 0, 0)
    
    # 大范围测试（多天，更能体现异步优势）
    start = datetime(2025, 11, 4, 17, 0, 0, 0)
    end = datetime(2025, 11, 4, 23, 59, 59, 999000)
    
    print(f"\n测试时间范围: {start} 到 {end}")
    print(f"交易对: BTCUSDT\n")
    
    # ========== 测试同步版本 ==========
    print("-" * 60)
    print("【同步版本测试】")
    print("-" * 60)
    sync_tool = GetAggTradesData_ms(symbol='BTCUSDT')
    
    start_time = time.time()
    df_sync = sync_tool.get_agg_trades_by_time_range(start, end)
    sync_duration = time.time() - start_time
    
    print(f"\n同步版本耗时: {sync_duration:.2f} 秒")
    print(f"获取数据条数: {len(df_sync)}")
    
    # ========== 测试异步版本 ==========
    print("\n" + "-" * 60)
    print("【异步版本测试】")
    print("-" * 60)
    async_tool = GetAggTradesData_ms_async(symbol='BTCUSDT', max_concurrent=10)
    
    start_time = time.time()
    df_async = async_tool.get_agg_trades_by_time_range(start, end, split_duration_hours=1)
    async_duration = time.time() - start_time
    
    print(f"\n异步版本耗时: {async_duration:.2f} 秒")
    print(f"获取数据条数: {len(df_async)}")
    
    # ========== 性能对比 ==========
    print("\n" + "=" * 60)
    print("【性能对比结果】")
    print("=" * 60)
    
    if sync_duration > 0:
        speedup = sync_duration / async_duration
        print(f"同步版本耗时: {sync_duration:.2f} 秒")
        print(f"异步版本耗时: {async_duration:.2f} 秒")
        print(f"性能提升: {speedup:.2f}x 倍")
        
        if speedup > 1:
            print(f"✅ 异步版本更快！节省了 {sync_duration - async_duration:.2f} 秒")
        elif speedup < 1:
            print(f"⚠️  异步版本稍慢，可能是数据量较小或网络延迟")
        else:
            print(f"➡️  性能相近")
    
    # ========== 数据验证 ==========
    print("\n" + "=" * 60)
    print("【数据验证】")
    print("=" * 60)
    
    if not df_sync.empty and not df_async.empty:
        print(f"同步版本数据条数: {len(df_sync)}")
        print(f"异步版本数据条数: {len(df_async)}")
        
        if len(df_sync) == len(df_async):
            print("✅ 数据条数一致")
        else:
            print(f"⚠️  数据条数不一致，差异: {abs(len(df_sync) - len(df_async))} 条")
        
        # 检查时间范围
        if not df_sync.empty:
            print(f"同步版本时间范围: {df_sync['成交时间'].min()} 到 {df_sync['成交时间'].max()}")
        if not df_async.empty:
            print(f"异步版本时间范围: {df_async['成交时间'].min()} 到 {df_async['成交时间'].max()}")
    else:
        if df_sync.empty:
            print("⚠️  同步版本未获取到数据")
        if df_async.empty:
            print("⚠️  异步版本未获取到数据")
    
    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)


if __name__ == '__main__':
    compare_performance()

