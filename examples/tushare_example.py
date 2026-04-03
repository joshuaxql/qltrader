"""
Tushare数据下载使用示例

本示例展示如何使用Tushare下载完整的A股数据（整合到单个CSV文件）。

使用前请确保：
1. 在项目根目录创建 .env 文件
2. 添加 TUSHARE_TOKEN=your_token
或
3. 调用 set_token('your_token') 手动设置
"""

from qltrader import (
    set_token,
    download_data,
    download_batch,
    load_daily_data,
    get_securities_info,
    get_stock_basic,
    get_index_basic,
    get_all_stock,
)

# 如需手动设置Token，可取消下行注释并替换为您的Token
# set_token("your_tushare_token")
# 如果已配置.env文件，模块会自动读取TUSHARE_TOKEN环境变量


def example_download_single_stock():
    """示例：下载单只股票完整数据"""
    print("=" * 60)
    print("示例1：下载单只股票完整数据（所有字段整合到一个文件）")
    print("=" * 60)

    # 下载浦发银行(sh600000)的完整数据
    # 所有数据整合到 data/daily/sh600000.csv 一个文件中
    # 包含：代码、类型、名称、申万行业、开高低收、成交量成交额、每日指标、资金流向
    df = download_data(
        code="sh600000",
        start_date="2024-01-01",
        end_date="2024-01-31",
        include_daily_basic=True,  # 包含每日指标（PE/PB/换手率/市值等）
        include_moneyflow=True,  # 包含资金流向
    )

    if df is not None and len(df) > 0:
        print("\n下载成功！共 {len(df)} 条记录")
        print("\n数据列: {list(df.columns)}")
        print("\n前5行数据:")
        print(df.head())


def example_download_index():
    """示例：下载指数数据"""
    print("\n" + "=" * 60)
    print("示例2：下载指数数据")
    print("=" * 60)

    # 下载上证指数数据
    df = download_data(code="sh000001", start_date="2024-01-01", end_date="2024-01-31")

    if df is not None and len(df) > 0:
        print(f"\n下载成功！共 {len(df)} 条记录")
        print(f"\n数据列: {list(df.columns)}")


@staticmethod
def example_batch_download():
    """示例：批量下载多只股票"""
    print("\n" + "=" * 60)
    print("示例3：批量下载多只股票")
    print("=" * 60)

    # 批量下载银行股
    stock_list = ["sh600000", "sh600036", "sz000001"]

    results = download_batch(
        codes=stock_list,
        start_date="2024-01-01",
        end_date="2024-01-31",
        include_daily_basic=True,
        include_moneyflow=True,
    )

    print(f"\n成功下载 {len(results)} 只股票")
    for code, df in results.items():
        print(f"  {code}: {len(df)} 条记录")


def example_load_and_query():
    """示例：加载已下载的数据并进行查询"""
    print("\n" + "=" * 60)
    print("示例4：加载和查询数据")
    print("=" * 60)

    # 先下载一份数据
    download_data(
        code="sh600000",
        start_date="2024-01-01",
        end_date="2024-01-31",
        include_daily_basic=True,
        include_moneyflow=True,
    )

    # 加载数据
    df = load_daily_data("sh600000")

    print("\n数据概览:")
    print(f"  代码: {df.iloc[0]['code']}")
    print(f"  类型: {df.iloc[0]['type']}")
    print(f"  名称: {df.iloc[0]['name']}")
    print(f"  行业: {df.iloc[0].get('industry_l1_name', 'N/A')}")
    print(f"  数据条数: {len(df)}")

    # 查询特定字段
    if "pe" in df.columns:
        print("\nPE指标:")
        print(f"  最小值: {df['pe'].min():.2f}")
        print(f"  最大值: {df['pe'].max():.2f}")
        print(f"  平均值: {df['pe'].mean():.2f}")

    if "net_mf_amount" in df.columns:
        print("\n资金流向(净流入额):")
        print(f"  最小值: {df['net_mf_amount'].min() / 1e8:.2f} 亿")
        print(f"  最大值: {df['net_mf_amount'].max() / 1e8:.2f} 亿")


def example_get_securities_info():
    """示例：获取已下载证券的基本信息"""
    print("\n" + "=" * 60)
    print("示例5：获取已下载证券信息")
    print("=" * 60)

    # 获取所有已下载证券的信息
    info_df = get_securities_info()

    if len(info_df) > 0:
        print(f"\n已下载 {len(info_df)} 只证券:")
        print(info_df.to_string())
    else:
        print("\n暂无已下载的证券数据")


def example_download_all_stocks(start_date="2024-01-01", end_date="2024-12-31"):
    """示例：一键下载所有A股股票数据（包括上市、退市、暂停上市）"""
    print("\n" + "=" * 60)
    print("示例6：一键下载所有A股股票数据")
    print("=" * 60)

    print("\n正在获取全部A股股票列表（包括上市、退市、暂停上市）...")
    stock_df = get_all_stock()  # 获取全部股票（上市+退市+暂停上市）
    print(f"共获取到 {len(stock_df)} 只股票")

    # 转换为项目代码格式
    codes = [
        f"{row['ts_code'].split('.')[1].lower()}{row['ts_code'].split('.')[0]}"
        for _, row in stock_df.iterrows()
    ]

    print("\n开始下载所有股票数据...")
    print(f"日期范围: {start_date} 至 {end_date}")
    print("预计需要较长时间，请耐心等待...")

    # 批量下载
    success_count = 0
    fail_count = 0
    total = len(codes)

    for i, code in enumerate(codes):
        while True:
            try:
                print(f"\n[{i + 1}/{total}] 正在下载 {code}...")
                df = download_data(
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    include_daily_basic=True,
                    include_moneyflow=True,
                )
                if df is not None and len(df) > 0:
                    success_count += 1
                else:
                    fail_count += 1
                break
            except Exception as e:
                print(f"  下载失败: {e}")
                fail_count += 1

    print("\n" + "=" * 60)
    print("下载完成！")
    print(f"  成功: {success_count}")
    print(f"  失败: {fail_count}")
    print("=" * 60)


def example_download_all_indices(start_date="2024-01-01", end_date="2024-12-31"):
    """示例：一键下载所有指数数据"""
    print("\n" + "=" * 60)
    print("示例7：一键下载所有指数数据")
    print("=" * 60)

    # 获取上证和深证指数
    all_indices = []

    print("\n正在获取上证指数列表...")
    sh_indices = get_index_basic(market="SSE")
    print(f"上证指数: {len(sh_indices)} 只")
    all_indices.append(sh_indices)

    print("正在获取深证指数列表...")
    sz_indices = get_index_basic(market="SZSE")
    print(f"深证指数: {len(sz_indices)} 只")
    all_indices.append(sz_indices)

    # 合并
    import pandas as pd

    indices_df = pd.concat(all_indices, ignore_index=True)
    print(f"共获取到 {len(indices_df)} 只指数")

    # 转换为项目代码格式
    codes = [
        f"{row['ts_code'].split('.')[1].lower()}{row['ts_code'].split('.')[0]}"
        for _, row in indices_df.iterrows()
    ]

    print("\n开始下载所有指数数据...")
    print(f"日期范围: {start_date} 至 {end_date}")

    # 批量下载
    success_count = 0
    fail_count = 0
    total = len(codes)

    for i, code in enumerate(codes):
        while True:
            try:
                print(f"\n[{i + 1}/{total}] 正在下载 {code}...")
                df = download_data(
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    include_daily_basic=False,  # 指数没有每日指标
                    include_moneyflow=False,  # 指数没有资金流向
                )
                if df is not None and len(df) > 0:
                    success_count += 1
                else:
                    fail_count += 1
                break
            except Exception as e:
                print(f"  下载失败: {e}")
                fail_count += 1

    print("\n" + "=" * 60)
    print("下载完成！")
    print(f"  成功: {success_count}")
    print(f"  失败: {fail_count}")
    print("=" * 60)


if __name__ == "__main__":
    # 运行示例
    # example_download_single_stock()
    # example_download_index()
    # example_batch_download()
    # example_load_and_query()
    # example_get_securities_info()
    example_download_all_stocks(
        start_date="2013-01-01", end_date="2026-01-01"
    )  # 一键下载所有股票（耗时较长）
    # example_download_all_indices(start_date="2013-01-01", end_date="2026-01-01")  # 一键下载所有指数（耗时较长）
