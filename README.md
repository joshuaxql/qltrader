# QLTrader

A股量化回测框架，API设计参考Zipline风格。

## 功能特性

- 支持CSV格式的A股日线数据回测
- 模拟真实交易：滑点、佣金、仓位管理
- 内置`schedule`定时任务，支持日频/月频调仓
- 数据接口：`data.current()`、`data.history()`、`data.can_trade()`
- 下单接口：`order_shares()`、`order_target_percent()`、`order_percent()`、`order_target_shares()`
- 回测结果可视化与指标统计

## 项目结构

```
qltrader/
├── src/
│   └── qltrader/            # 核心框架包
│       ├── __init__.py      # 包入口，导出公共API
│       ├── config.py        # 配置常量（数据路径等）
│       ├── models.py        # 核心模型（Position, Portfolio, Context）
│       ├── data.py          # 数据访问接口（Data类）
│       ├── orders.py        # 下单函数
│       ├── scheduler.py     # 定时任务调度
│       ├── engine.py        # 回测引擎（QlTrader类）
│       ├── plotting.py      # 可视化函数
│       └── utils.py         # 工具函数（run_backtest, get_price）
├── profit_freq_strategy.py  # 盈利频率因子策略示例
├── download_gm.py           # 聚宽数据下载脚本
├── data/daily/              # 股票日线数据 (CSV)
└── README.md
```

## 快速开始

### 安装依赖

```bash
uv sync
```

### 编写自己的策略

#### 方式一：纯定时任务策略（推荐）
适合定期调仓（如月度换仓）的策略，`handle_data` 参数可选：

```python
NUM_STOCKS = 30

def initialize(context):
    # 设置股票池
    context.set_universe(['sh600000', 'sh600001', 'sh600004'])
    # 每月开盘调仓
    schedule(rebalance, date_rule='month', time_rule='open')

def rebalance(context, data):
    # 获取历史数据
    hist = data.history(context.universe, 20, fields='close')
    # 选股逻辑...
    # 下单
    order_shares('sh600000', 1000)

# 运行回测（不需要传 handle_data）
results = run_backtest(
    start_date='2020-01-01',
    end_date='2025-12-31',
    initialize=initialize,
    capital_base=1000000.0
)

plot_results(results)
```

#### 方式二：日频交易策略
适合需要每日判断的策略，需传入 `handle_data`：

```python
def initialize(context):
    context.set_universe(['sh600000', 'sh600001'])

def handle_data(context, data):
    # 每日收盘前判断
    for sec in context.universe:
        if data.can_trade(sec):
            # 等权持仓
            order_target_percent(context, sec, 1.0 / len(context.universe))

results = run_backtest(
    start_date='2020-01-01',
    end_date='2025-12-31',
    initialize=initialize,
    handle_data=handle_data,
    capital_base=1000000.0
)

plot_results(results)
```

### 导入方式

框架位于 `src/qltrader/` 目录下，提供以下导入方式：

**方式一：直接引用（推荐）**
```python
from src.qltrader import run_backtest, plot_results, order_shares, schedule
```

**方式二：在项目中使用**
在你的策略文件开头添加：
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from qltrader import run_backtest, order_shares, schedule, plot_results
```

**方式三：安装为包**
```bash
# 在项目根目录下
uv pip install -e src/
```
然后可以直接使用：
```python
from qltrader import run_backtest, order_shares, schedule, plot_results
```

## API 参考

### 数据接口

| 方法 | 说明 |
|------|------|
| `data.current(securities, fields)` | 获取当前交易日数据 |
| `data.history(securities, bar_count, fields)` | 获取历史数据 |
| `data.can_trade(securities)` | 检查股票是否可交易 |

### 下单接口

| 方法 | 说明 |
|------|------|
| `order_shares(sec, amount)` | 买入/卖出指定股数 |
| `order_target_shares(sec, target_shares)` | 调仓到目标股数 |
| `order_percent(context, sec, percent)` | 按资金比例下单（增量） |
| `order_target_percent(context, sec, percent)` | 调仓到目标资金比例 |

### 定时任务

```python
schedule(func, date_rule='daily/month', time_rule='open/close')
```

### 回测入口

```python
run_backtest(
    start_date='2020-01-01',      # 开始日期
    end_date='2025-12-31',        # 结束日期
    initialize=initialize,        # 初始化函数（必须）
    handle_data=handle_data,      # 日频策略函数（可选，纯定时任务可不传）
    before_trading_start=None,    # 盘前函数（可选）
    capital_base=1000000.0        # 初始资金
)
```

## 数据格式

CSV文件位于 `data/daily/` 目录，文件命名规则：`sh600000.csv`（上证）、`sz000001.csv`（深证）

字段：`date, open, high, low, close, volume`

## 下载数据

修改 `download_gm.py` 中的 `set_token("your_token")` 为你的掘金Token，然后运行此文件从掘金量化下载数据