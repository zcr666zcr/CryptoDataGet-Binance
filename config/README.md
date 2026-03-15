# config 目录说明

本目录用于存放程序运行所需和生成的文件。

## 目录结构

```
config/
├── logs/          # 日志文件目录
│   └── 程序名_日期.log
├── runtime/       # 运行时临时文件目录
├── data/          # 运行时数据文件目录（如 xlsx 等）
└── output/        # 程序输出文件目录
```

## 使用说明

### 1. 日志记录

在任何 Python 脚本中使用日志功能：

```python
from logger import get_logger

# 获取日志记录器
logger = get_logger()

# 使用日志
logger.info("这是一条信息日志")
logger.warning("这是一条警告日志")
logger.error("这是一条错误日志")
```

日志文件会自动保存到 `config/logs/` 目录，文件名格式为：`程序名_日期.log`

### 2. 保存数据文件（如 xlsx）

```python
import pandas as pd
from config_utils import save_to_config, get_data_file_path

# 方式1：使用 save_to_config 函数
df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
save_to_config(df, 'report.xlsx', sub_dir='data', format='xlsx')

# 方式2：手动保存
file_path = get_data_file_path('report.xlsx')
df.to_excel(file_path, index=False)
```

### 3. 获取文件路径

```python
from config_utils import (
    get_data_file_path,
    get_runtime_file_path,
    get_output_file_path,
    get_config_path
)

# 获取数据文件路径
data_file = get_data_file_path('report.xlsx')

# 获取运行时文件路径
runtime_file = get_runtime_file_path('temp.txt')

# 获取输出文件路径
output_file = get_output_file_path('result.csv')

# 获取 config 目录下的任意文件路径
config_file = get_config_path('custom/subdir/file.txt')
```

## 注意事项

- 所有目录会在首次使用时自动创建
- 日志文件按日期自动分割，每天一个文件
- 建议将运行时生成的文件保存到对应的子目录中，便于管理


