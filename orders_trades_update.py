import pandas as pd
import datetime as dt

from tsdb_client import TSDBClient
from utils.Config import EnvConfig

cli = TSDBClient(
        host=EnvConfig.DB_HOST,
        port=EnvConfig.DB_PORT,
        user=EnvConfig.DB_USER,
        password=EnvConfig.DB_PASSWORD,
        db=EnvConfig.DB_DATABASE,
    )

strategy_df = cli.execute_query('''
        SELECT * FROM dealer.strategy
        ''', 
            out_type='df')

def time_transform(t):
    return dt.datetime.strptime(t, '%H%M%S').time()

def security_type_transform(security_type: str):
    if security_type == "現股":
        return 'S'
    elif security_type == "期貨":
        return 'F'

def action_transform(action: str):
    if action == 'Buy': return 'B'
    if action == 'Sell': return 'S'
        

def strategy_align(log_type: str, strategy_name: str, order: pd.DataFrame):

    log = pd.read_csv(f'print/{strategy_name}/{log_type}.log', header = None, names = ['serial', 'security_type', 'timestamp', 'code', 'order_type', 'action', 'order_qty', 'order_price'])

    for index, row in log.iterrows():
    
        cur_order = order[(order['security_type'] == row.security_type[0]) & (order['code'] == str(row.code)) & (order['order_type'] == row.order_type) & 
        (order['action'] == row.action) & (order['order_price'].astype('float') == row.order_price)]
        
        for index in cur_order.index:
            order.loc[index, 'strategy'] = int(strategy_df[strategy_df['name'] == strategy_name]['id'].values[0])
    return order


def load_orders(path: str):
    return pd.read_csv('status/Order.txt', header = None,
        names = ['trader_id','order_id', 'security_type', 'order_time', 'code', 'order_type', 'action', 'order_qty', 'order_price', 'status'],
        dtype='str')

def process_orders(order: pd.DataFrame):
    order['security_type'] = order['security_type'].apply(security_type_transform)
    order['order_time'] = order['order_time'].apply(time_transform)
    order['order_date'] = dt.datetime.now().date()
    order['action'] = order['action'].apply(action_transform)

    order['strategy'] = 0
    for strategy_name in strategy_df['name']:
        order = strategy_align("Buy", strategy_name, order)
        order = strategy_align("Sell", strategy_name, order)

    return order


def load_trades(path: str):
    return pd.read_csv(path, header = None,
        names = ['trader_id','order_id', 'security_type', 'trade_time', 'code', 'order_type', 'action', 'qty', 'price', 'status'], dtype='str')

def process_trades(deal: pd.DataFrame, order: pd.DataFrame):
    deal['security_type'] = deal['security_type'].apply(security_type_transform)
    deal['trade_time'] = deal['trade_time'].apply(time_transform)
    deal['trade_date'] = dt.datetime.now().date() 
    deal['action'] = deal['action'].apply(action_transform)

    deal['strategy'] = 0    
    for index, row in deal.iterrows():
        
        deal.loc[index, 'strategy'] = order[order['order_id'] == row.order_id]['strategy'].values[0]

    return deal


def save2db(df: pd.DataFrame, table: str):
    
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
    df_orders = load_orders("status/Order.txt")
    df_orders = process_orders(df_orders)
    save2db(df_orders[df_orders['status'].isna()], table="dealer.orders")

    # trades
    df_trades = load_trades("status/Deal.txt")
    df_trades = process_trades(df_trades, df_orders)
    save2db(df_trades, table="dealer.trades")


if __name__ == "__main__":
    pipline()
