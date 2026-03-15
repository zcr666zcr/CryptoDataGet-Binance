"""
市场数据获取配置文件
包含API密钥、数据路径以及各种数据获取的配置参数
"""
from pathlib import Path

# ========== 项目根目录 ==========
# 获取当前文件所在目录作为项目根目录
PROJECT_ROOT = Path(__file__).parent

# ========== API配置 ==========
api_dic = {
    'api_key': 'nglzGY8LxLP8AGMRn6nPJcsiY26tfqm6jK5CWs7NNfZd2g46VfKVVlEoj4Bo7M3d',
    'api_secret':'RDiiMsLa4WfPPSPBT4B5kyOshIjLMbR9Pyh0ioNxaokoxHDUakfpwYb1G9esbThp'# 替换为您的币安API密钥 # 替换为您的币安API密钥对应的密钥
}

# ========== 数据路径配置 ==========
data_path = {
    'symbol_pool': Path(rf'E:\Quant\data\symbols'),
    'result_path_daily': Path(rf'E:\Quant\data\CoinKline\现货行情数据1d'),
    'result_path_min': Path(rf'E:\Quant\data\CoinKline\现货行情数据1min'),
    'result_path_hour': Path(rf'E:\Quant\data\CoinKline\现货行情数据1h'),
    'result_path_depth': Path(rf'E:\Quant\data\CoinOrderbook\现货深度数据'),
    'result_path_trades': Path(rf'E:\Quant\data\CoinOrderbook\现货成交数据'),
    'result_path_aggtrades': Path(rf'E:\Quant\data\CoinOrderbook\现货成交归集数据'),
    'result_path_historical_trades': Path(rf'E:\Quant\data\CoinOrderbook\现货历史成交数据'),
    'result_path_daily_trade_count': Path(rf'E:\Quant\data\CoinOrderbook\现货交易笔数统计')
}

# ========== 运行时文件目录配置 ==========
# config 目录用于存放程序运行所需和生成的文件（如 xlsx 文件、运行日志等）
CONFIG_DIR = PROJECT_ROOT / 'config'

# 确保 config 目录存在
CONFIG_DIR.mkdir(exist_ok=True)

# config 目录下的子目录
CONFIG_PATHS = {
    'logs': CONFIG_DIR / 'logs',  # 日志文件目录
    'runtime': CONFIG_DIR / 'runtime',  # 运行时临时文件目录
    'data': CONFIG_DIR / 'data',  # 运行时数据文件目录（如 xlsx 等）
    'output': CONFIG_DIR / 'output',  # 程序输出文件目录
}

# 确保所有子目录存在
for sub_dir in CONFIG_PATHS.values():
    sub_dir.mkdir(parents=True, exist_ok=True)

# ========== 通用配置 ==========
# 默认交易对
DEFAULT_SYMBOL = 'BTCUSDT'

# 时区设置
TIMEZONE = 'Asia/Hong_Kong'

# 默认保存格式：'feather' 或 'csv'
DEFAULT_SAVE_FORMAT = 'feather'

# API请求重试配置
RETRY_SLEEP_TIME = 10  # 请求失败后重试等待时间（秒）

# ========== 深度数据配置 (GetDepthData) ==========
DEPTH_CONFIG = {
    'default_limit': 5000,  # 默认深度数量，可选值：5, 10, 20, 50, 100, 500, 1000, 5000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
}

# ========== 成交数据配置 (GetTradesData) ==========
TRADES_CONFIG = {
    'default_limit': 1000,  # 默认返回数量，最大1000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
    'request_interval': 0.2,  # 分页请求时的间隔时间（秒），避免请求过快
    'max_requests': 1000000,  # 按时间范围下载时的最大请求次数，防止无限循环
}

# ========== 成交归集数据配置 (GetAggTradesData) ==========
AGG_TRADES_CONFIG = {
    'default_limit': 1000,  # 默认返回数量，最大1000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
    'request_interval': 0.2,  # 分页请求时的间隔时间（秒），避免请求过快
    'max_requests': 1000000,  # 按时间范围下载时的最大请求次数，防止无限循环
}

# ========== 历史成交数据配置 (GetHistoricalTradesData) ==========
HISTORICAL_TRADES_CONFIG = {
    'default_limit': 1000,  # 默认返回数量，最大1000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
    'request_interval': 0.2,  # 分页请求时的间隔时间（秒），避免请求过快
    'max_requests': 1000000,  # 按时间范围下载时的最大请求次数，防止无限循环
    'max_requests_per_batch': 1000000,  # 按fromId下载时的最大请求次数，防止无限循环
    'default_from_id': 0,  # 默认起始成交ID，如果为None则从最早的可用ID开始
}

# ========== 多线程下载配置 ==========
# 是否要求必须使用不同IP的节点（True：必须不同IP，False：只需节点数量足够）
REQUIRE_UNIQUE_IP = False  # 设置为False时，将跳过IP检测，直接使用前N个节点

# 并发下载进程数（线程数）
NUM_PROCESSES = 10  # 并发下载使用的进程数，应该与Clash中的节点数量匹配

# ========== Clash代理配置 ==========
CLASH_PROXY_CONFIG = {
    'proxy_port': 7897,  # Clash默认HTTP代理端口（通常只有一个）
    'proxy_host': '127.0.0.1',  # Clash代理地址
    'api_port': 9097,  # Clash REST API端口（默认9090，用于切换节点）
    'api_secret': '32145wsad',  # Clash API密钥（如果配置了的话）
}

# ========== Binance API配置 ==========
BINANCE_CONFIG = {
    'base_url': 'https://api.binance.com',
    # 可以添加API Key和Secret（如果需要）
    # 'api_key': 'your_api_key',
    # 'api_secret': 'your_api_secret',
}

# ========== 测试配置 ==========
TEST_CONFIG = {
    'test_endpoint': '/api/v3/ping',  # 测试端点（ping接口，不需要认证）
}