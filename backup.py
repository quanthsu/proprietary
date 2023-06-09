import numpy 
from utils import Config, check_folder_path_exist
import glob
import math
import datetime
import os
from tsdb_client import TSDBClient
cli = TSDBClient(
        host="128.110.25.99",
        port=5432,
        user="chiubj",
        password="bunnygood",
        db="accountdb"
        )

if __name__ == "__main__":

    config = Config()

    nowtime_str = datetime.datetime.now().strftime("%Y%m%d")
    nowtime_path = f"{config.signal_print_backup_path}{nowtime_str}"
    check_folder_path_exist(nowtime_path)

    def backup(strategy_print_path, backup_path, buy_or_sell_type):
            
        fw = open(f"{backup_path}\{buy_or_sell_type}.log", 'w')
        with open(f"{strategy_print_path}\{buy_or_sell_type}.log", 'r+') as fr:
            for line in fr:
                fw.write(line)
            fr.truncate(0)

        fr.close()
        fw.close()

    for strategy in cli.execute_query('''
    SELECT name FROM dealer.strategy where status
    '''):

        strategy_name = strategy[0]
        
        strategy_print_path = f"{config.signal_print_path}{strategy_name}"
        strategy_backup_path = f"{nowtime_path}\{strategy_name}"
        check_folder_path_exist(strategy_backup_path)
        
        backup(strategy_print_path, strategy_backup_path, 'Buy')
        backup(strategy_print_path, strategy_backup_path, 'Sell')

    for xq_output_path in glob.glob(f'{config.xq_output_path}*'):
        os.remove(xq_output_path)

    with open(config.deal_log, 'r+') as fr:
        fr.truncate(0)
    with open(config.order_log, 'r+') as fr:
        fr.truncate(0)
        