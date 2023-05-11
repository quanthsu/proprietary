import psycopg2
import pandas as pd
from datetime import datetime, date

conn = psycopg2.connect(database="accountdb", user="chiubj", 
                        password="bunnygood", host="128.110.25.99", 
                        port="5432")
cur  = conn.cursor()

orders = pd.read_csv('status/orders.csv')
orders['委託日期'] = pd.to_datetime(orders['委託日期'])
trades = pd.read_csv('status/trades.csv')
trades['成交日期'] = pd.to_datetime(trades['成交日期'])


for order_index in range(len(orders)):
    if date(datetime.now().year, datetime.now().month, datetime.now().day) == date(orders.iloc[order_index]['委託日期'].year, orders.iloc[order_index]['委託日期'].month, orders.iloc[order_index]['委託日期'].day):
        record = [str(orders.iloc[order_index]['交易員代碼']), orders.iloc[order_index]['委託書號'], orders.iloc[order_index]['交易別'], orders.iloc[order_index]['委託日期'], orders.iloc[order_index]['委託時間'],
        str(orders.iloc[order_index]['代碼']), orders.iloc[order_index]['委託種類'], orders.iloc[order_index]['方向'], int(orders.iloc[order_index]['委託部位']), 
        float(orders.iloc[order_index]['委託價格']), orders.iloc[order_index]['狀態'], str(orders.iloc[order_index]['策略別'])]

        table_columns = '(trader_id, order_id, security_type, order_date, order_time, code, order_type, action, order_qty, order_price, status, strategy)'
        postgres_insert_query = f"""INSERT INTO accountdb.dealer.orders {table_columns} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cur.execute(postgres_insert_query, record)

        conn.commit()


for trade_index in range(len(trades)):
    if date(datetime.now().year, datetime.now().month, datetime.now().day) == date(trades.iloc[trade_index]['成交日期'].year, trades.iloc[trade_index]['成交日期'].month, trades.iloc[trade_index]['成交日期'].day):
        record = [str(trades.iloc[trade_index]['交易員代碼']), trades.iloc[trade_index]['委託書號'], trades.iloc[trade_index]['交易別'], trades.iloc[trade_index]['成交日期'], trades.iloc[trade_index]['成交時間'],
        str(trades.iloc[trade_index]['代碼']), trades.iloc[trade_index]['委託種類'], trades.iloc[trade_index]['方向'], int(trades.iloc[trade_index]['成交部位']), 
        float(trades.iloc[trade_index]['成交價格']), trades.iloc[trade_index]['狀態'], str(trades.iloc[trade_index]['策略別']), trades.iloc[trade_index]['成交序號 (seqno)']]

        table_columns = '(trader_id, order_id, security_type, trade_date, trade_time, code, order_type, action, qty, price, status, strategy, seqno)'
        postgres_insert_query = f"""INSERT INTO accountdb.dealer.trades {table_columns} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cur.execute(postgres_insert_query, record)

        conn.commit()


