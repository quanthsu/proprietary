import numpy as np 
import pandas as pd
import glob
import math
import datetime
import trading_calendars as tc
import pytz
xtai = tc.get_calendar("XTAI")

from utils import Config, check_folder_path_exist
from tsdb_client import TSDBClient

config = Config()

xq_stock_id_index = 1
xq_order_type_index = 2
xq_buy_or_sell_index = 3
xq_lots_index = 4
xq_order_price_index = 5

status_stock_id_index = 0
status_buy_or_sell_index = 1
status_lots_index = 2
status_in_date_index = 3
status_expected_out_date_index = 4


def xq_output_to_print():
    config = Config()
    
    counter = 1

    for xq_output_path in config.xq_output_paths:

        strategy_file_name = xq_output_path.split('\\')[-1]
        strategy_name = strategy_file_name[9:-6]
        slr = config.strategy_info[strategy_name]['Leverage_Ratio']
        holding_period = int(config.strategy_config[strategy_name]['holding_period'])
        start_date = datetime.datetime.now().strftime("%Y-%m-%d")
        end_date = xtai.sessions_window(pd.Timestamp(start_date), holding_period)[-1].strftime("%Y-%m-%d")
        
        strategy_print_path = f"{config.signal_print_path}{strategy_name}"
        check_folder_path_exist(strategy_print_path)

        f_buy = open(f"{strategy_print_path}\Buy.log", 'a+')
        f_sell = open(f"{strategy_print_path}\Sell.log", 'a+')
        
        status = open(f"{config.strategy_status_path}{strategy_name}.txt", 'a+')
        
        with open(xq_output_path, 'r') as f:
            for line in f:
                cur_line = line.split(' ')
                slr_lots = str(math.floor(float(cur_line[xq_lots_index]) * slr)) 
                status_list = [cur_line[xq_stock_id_index][:-3], cur_line[xq_buy_or_sell_index], slr_lots, start_date, end_date, '\n']
                output_list = [f"N{counter}", "Stock", str(datetime.datetime.now().timestamp()), cur_line[xq_stock_id_index][:-3], cur_line[xq_order_type_index], 
                cur_line[xq_buy_or_sell_index], slr_lots, cur_line[xq_order_price_index]]
                
                if cur_line[xq_buy_or_sell_index] == 'B':
                    f_buy.write(','.join(output_list) + '\n')
                elif cur_line[xq_buy_or_sell_index] == 'S':
                    f_sell.write(','.join(output_list) + '\n')
                else:
                    raise Exception('Invalid buy or sell signal')
                
                status.write(' '.join(status_list))
                counter += 1

        f_buy.close()
        f_sell.close()
        status.close()


def out_action():
    
    config = Config()
    cli = TSDBClient(
        host="128.110.25.99",
        port=5432,
        user="chiubj",
        password="bunnygood",
        db="accountdb"
        )
    cur_status = cli.execute_query(f'''
    SELECT * FROM dealer.status where out_date = '{datetime.datetime.now().strftime("%Y-%m-%d")}'
    ''', out_type='df')
    strategy_df = cli.execute_query('''
    SELECT * FROM dealer.strategy
    ''', 
        out_type='df')
    
    counter = 1

    for index, row in cur_status.iterrows():

        strategy_name = strategy_df[strategy_df['id'] == row.strategy]['name'].values[0]
        strategy_print_path = f"{config.signal_print_path}{strategy_name}"
        f_buy = open(f"{strategy_print_path}\Buy.log", 'a+')
        f_sell = open(f"{strategy_print_path}\Sell.log", 'a+')
        cur_contracts_df = cli.execute_query(f'''
            SELECT * FROM sino.contracts where code = '{row.code}'
            ''', out_type='df')
        
        if row.security_type == 'S': security_type = 'Stock' 
        if row.security_type == 'F': security_type = 'Futures'
        action = 'S' if row.qty > 0 else 'B'

        signal_list = [f'O{counter}', security_type, str(datetime.datetime.now().timestamp()), row.code, 'ROD', action, str(abs(row.qty)), '%.2f'%cur_contracts_df['limit_down'].values[0]]
        counter += 1

        if action == 'B':
            f_buy.write(','.join(signal_list) + '\n')
        elif action == 'S':
            f_sell.write(','.join(signal_list) + '\n')

        f_buy.close()
        f_sell.close()
    

if __name__ == "__main__":

    xq_output_to_print()
    out_action()
