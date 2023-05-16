import pandas as pd
import datetime as dt

from tsdb_client import TSDBClient
from utils.Config import EnvConfig


def load_orders(path: str):
    return pd.read_csv(path, dtype=str)


def process_orders(df: pd.DataFrame):
    col_mapping = {
        "交易員代碼": "trader_id",
        "委託書號": "order_id",
        "交易別": "security_type",
        "委託日期": "order_date",
        "委託時間": "order_time",
        "代碼": "code",
        "委託種類": "order_type",
        "方向": "action",
        "委託部位": "order_qty",
        "委託價格": "order_price",
        "狀態": "status",
        "策略別": "strategy",
    }
    df.rename(columns=col_mapping, inplace=True)


def load_trades(path: str):
    return pd.read_csv(path, dtype=str)


def process_trades(df: pd.DataFrame):
    col_mapping = {
        "交易員代碼": "trader_id",
        "委託書號": "order_id",
        "交易別": "security_type",
        "成交日期": "trade_date",
        "成交時間": "trade_time",
        "代碼": "code",
        "委託種類": "order_type",
        "方向": "action",
        "成交部位": "qty",
        "成交價格": "price",
        "狀態": "status",
        "策略別": "strategy",
        "成交序號 (seqno)": "seqno",
    }
    df.rename(columns=col_mapping, inplace=True)


def save2db(df: pd.DataFrame, table: str):
    cli = TSDBClient(
        host=EnvConfig.DB_HOST,
        port=EnvConfig.DB_PORT,
        user=EnvConfig.DB_USER,
        password=EnvConfig.DB_PASSWORD,
        db=EnvConfig.DB_DATABASE,
    )
    if table == "dealer.orders":
        date_ = "order_date"
    else:
        date_ = "trade_date"

    max_date: dt.datetime = cli.execute_query(f"select max({date_}) from {table}")[0][0]
    # only insert new data
    if max_date is not None:
        cond = pd.to_datetime(df[date_]) > pd.to_datetime(max_date)
        df = df.loc[cond]

    result = cli.execute_values_df(df, table)
    if result == 1:
        raise Exception("save2db error")


def pipline():
    # orders
    df_orders = load_orders("status/orders.csv")
    process_orders(df_orders)
    save2db(df_orders, table="dealer.orders")

    # trades
    df_trades = load_trades("status/trades.csv")
    process_trades(df_trades)
    save2db(df_trades, table="dealer.trades")


if __name__ == "__main__":
    pipline()
