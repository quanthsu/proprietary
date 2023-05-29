import numpy as np 
import pandas as pd
import glob
import math
import datetime
import trading_calendars as tc
import pytz
xtai = tc.get_calendar("XTAI")

from utils import Config, check_folder_path_exist
from utils.spread import _add_spread, _spread_cnt
from tsdb_client import TSDBClient
cli = TSDBClient(
        host="128.110.25.99",
        port=5432,
        user="chiubj",
        password="bunnygood",
        db="accountdb"
        )

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


def print_write(action, strategy_name, output_list):

    strategy_print_path = f"{config.signal_print_path}{strategy_name}"
    f_buy = open(f"{strategy_print_path}\Buy.log", 'a+')
    f_sell = open(f"{strategy_print_path}\Sell.log", 'a+')

    if action == 'B':
        f_buy.write(','.join(output_list) + '\n')
    elif action == 'S':
        f_sell.write(','.join(output_list) + '\n')
    else:
        raise Exception('Invalid buy or sell signal')

    f_buy.close()
    f_sell.close()

def output_write(cur_line, strategy_name, lots, counter):

    if lots ==0: return counter

    strategy_df = cli.execute_query('''
        SELECT * FROM dealer.strategy
        ''', 
            out_type='df')
    code = cur_line[xq_stock_id_index][:-3]
    action = cur_line[xq_buy_or_sell_index]
    order_low_ratio = strategy_df[strategy_df['name'] == strategy_name]['order_low_ratio'].values[0]

    reference_price = cli.execute_query(f'''
        SELECT * FROM sino.contracts where code = '{code}'
        ''', out_type='df')['reference'][0]

    lots_counter = 0
    while lots > 0:
        
        if lots_counter % 2 == 0:
            order_price = cur_line[xq_order_price_index]
        else:
            if action == 'B':
                _price = _add_spread(reference_price, -_spread_cnt((1 + order_low_ratio/100) * reference_price, reference_price))
            else:
                _price = _add_spread(reference_price, _spread_cnt(reference_price, (1 + order_low_ratio/100) * reference_price))
            order_price = '%.2f'%_price

        output_list = [f"N{counter}", "Stock", str(datetime.datetime.now().timestamp()), cur_line[xq_stock_id_index][:-3], cur_line[xq_order_type_index], 
        cur_line[xq_buy_or_sell_index], str(1), order_price]
        print_write(action, strategy_name, output_list)
        
        lots -= 1
        lots_counter += 1
        counter += 1

    return counter 

def xq_output_to_print():
    config = Config()
    
    counter = 1

    for xq_output_path in config.xq_output_paths:

        strategy_file_name = xq_output_path.split('\\')[-1]
        strategy_name = strategy_file_name[9:-4]
        slr = config.strategy_info[strategy_name]['Leverage_Ratio']
        holding_period = int(config.strategy_config[strategy_name]['holding_period'])
        start_date = datetime.datetime.now().strftime("%Y-%m-%d")
        end_date = xtai.sessions_window(pd.Timestamp(start_date), holding_period)[-1].strftime("%Y-%m-%d")
        
        strategy_print_path = f"{config.signal_print_path}{strategy_name}"
        check_folder_path_exist(strategy_print_path)

        status = open(f"{config.strategy_status_path}{strategy_name}.txt", 'a+')
        
        with open(xq_output_path, 'r') as f:
            for line in f:
                cur_line = line.split(' ')
                slr_lots = math.floor(float(cur_line[xq_lots_index]) * slr)
                status_list = [cur_line[xq_stock_id_index][:-3], cur_line[xq_buy_or_sell_index], str(slr_lots), start_date, end_date, '\n']
                
                counter = output_write(cur_line, strategy_name, slr_lots, counter)
                
                status.write(' '.join(status_list))
                counter += 1

        status.close()


def out_action():
    
    config = Config()

    cur_status = cli.execute_query("select * from dealer.ft_get_positions_fifo(CURRENT_DATE-1, 'B') union all select * from dealer.ft_get_positions_fifo(CURRENT_DATE, 'S')", out_type='df')

    strategy_df = cli.execute_query('''
    SELECT * FROM dealer.strategy
    ''', 
        out_type='df')
    
    counter = 1

    for index, row in cur_status.iterrows():

        strategy_name = strategy_df[strategy_df['id'] == row.strategy]['name'].values[0]
        holding_period = strategy_df[strategy_df['id'] == row.strategy]['holding_period'].values[0]

        expected_out_date = datetime.datetime.date(
            xtai.sessions_window(
                pd.Timestamp(row.first_entry_date.strftime("%Y-%m-%d")), holding_period
            )[-1]
        )

        if expected_out_date == datetime.datetime.now().date():
            
            strategy_print_path = f"{config.signal_print_path}{strategy_name}"
            f_buy = open(f"{strategy_print_path}\Buy.log", 'a+')
            f_sell = open(f"{strategy_print_path}\Sell.log", 'a+')
            cur_contracts_df = cli.execute_query(f'''
                SELECT * FROM sino.contracts where code = '{row.code}'
                ''', out_type='df') 

            if row.action == 'B':
                signal_list = [f'O{counter}', row.security_type, str(datetime.datetime.now().timestamp()), row.code, 'ROD', 'S', str(abs(row.qty)), '%.2f'%cur_contracts_df['limit_down'].values[0]]
                f_sell.write(','.join(signal_list) + '\n')
            elif row.action == 'S':
                signal_list = [f'O{counter}', row.security_type, str(datetime.datetime.now().timestamp()), row.code, 'ROD', 'B', str(abs(row.qty)), '%.2f'%cur_contracts_df['limit_up'].values[0]]
                f_buy.write(','.join(signal_list) + '\n')

            f_buy.close()
            f_sell.close()
            counter += 1
    

if __name__ == "__main__":

    xq_output_to_print()
    out_action()
