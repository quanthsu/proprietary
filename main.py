import numpy as np 
import pandas as pd
import glob
import math
import datetime
import trading_calendars as tc
import pytz
xtai = tc.get_calendar("XTAI")

from utils import Config, check_folder_path_exist

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

if __name__ == "__main__":

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
    
    