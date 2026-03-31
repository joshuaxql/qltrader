from gm.api import history, set_token, ADJUST_PREV, get_symbols
import pandas as pd
from rich.progress import track

set_token("your_token")


def download_symbols_data(symbols: list[str], start_time: str, end_time: str) -> dict:
    cal_df = history(
        symbol="SHSE.000001",
        frequency="1d",
        start_time=start_time,
        end_time=end_time,
        fields="eob",
        adjust=ADJUST_PREV,
        df=True,
    )
    if len(cal_df) == 0:
        cal_df = pd.DataFrame(columns=["eob"])
    cal_df = cal_df.rename(columns={"eob": "date"})
    cal_df["date"] = cal_df["date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    calendar = cal_df[["date"]].copy()
    data = {}
    for symbol in track(symbols):
        history_data: pd.DataFrame = history(
            symbol=symbol,
            frequency="1d",
            start_time=start_time,
            end_time=end_time,
            fields="open, close, low, high, volume, eob",
            adjust=ADJUST_PREV,
            df=True,
        )
        if len(history_data) == 0:
            continue
        history_data = history_data[["eob", "open", "high", "low", "close", "volume"]]
        history_data = history_data.rename(columns={"eob": "date"})
        history_data["volume"] = history_data["volume"] / 100
        history_data["date"] = history_data["date"].apply(
            lambda x: x.strftime("%Y-%m-%d")
        )
        symbol_calendar = calendar[calendar["date"] >= history_data.loc[0, "date"]]
        history_data = (
            history_data.set_index("date")
            .reindex(symbol_calendar["date"].to_list())
            .rename_axis("date")
            .reset_index()
        )
        history_data[["open", "high", "low", "close"]] = history_data[
            ["open", "high", "low", "close"]
        ].fillna(method="ffill")
        history_data["volume"] = history_data["volume"].fillna(0)
        data[symbol] = history_data
    return data


def data_to_csv(symbols: list[str], data: dict) -> None:
    print("开始存储至csv")
    for symbol in track(symbols):
        if symbol not in data.keys():
            continue
        symbol_data = data[symbol]
        if symbol[:4] == "SHSE":
            name = f"sh{symbol[-6:]}"
        else:
            name = f"sz{symbol[-6:]}"
        symbol_data.to_csv(f"D:/zipline_data/daily/{name}.csv", index=False)


if __name__ == "__main__":
    symbols = get_symbols(
        sec_type1=1010, sec_type2=101001, df=True, skip_suspended=False, skip_st=False
    )["symbol"].tolist()
    print(f"一共下载{len(symbols)}支股票")
    start_time = "2013-01-01"
    end_time = "2026-03-12"
    data = download_symbols_data(symbols, start_time, end_time)
    data_to_csv(symbols, data)
