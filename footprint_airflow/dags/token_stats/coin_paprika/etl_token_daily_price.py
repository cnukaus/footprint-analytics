import configparser
import glob
import json
import os
from datetime import timedelta
import moment
import pandas as pd
import pydash
import requests
from basic.etl_basic import ETLBasic
from config import project_config

coin_paprika_host = 'https://api.coinpaprika.com'


def get_start_end_timestamp(start_str: str, end_str: str):
    start_date_timestamp = moment.unix(moment.date(start_str).date.timestamp(), True).date.timestamp()
    end_date_timestamp = moment.unix(moment.date(end_str).date.timestamp(), True).date.timestamp()
    return {'start': start_date_timestamp, 'end': end_date_timestamp}


def read_tokens():
    dags_folder = project_config.dags_folder
    cfg = configparser.ConfigParser()
    cfg_path = os.path.join(dags_folder, 'token_stats/coin_paprika/prices_lending.ini')
    cfg.read(cfg_path)
    return cfg


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            print('dir is exists')


class TokenDailyPrice(ETLBasic):
    task_name = 'token_daily_price'
    table_name = 'token_daily_price'
    columns = None
    task_airflow_execution_time = '30 0 * * *'

    def __init__(self, start_str=None, end_str=None):
        super().__init__()
        if start_str is None:
            now = moment.utcnow().datetime
            yesterday = now - timedelta(days=1)
            start_str = yesterday.strftime("%Y-%m-%d")
            end_str = now.strftime("%Y-%m-%d")
        self.start_str = start_str
        self.end_str = end_str

    def get_execution_date(self):
        now = moment.utcnow().datetime
        yesterday = now - timedelta(days=1)
        return yesterday

    def get_history_price_per_day_csv(self, id, start, end):
        paprika_res = requests.get(
            url=coin_paprika_host + '/v1/tickers/' + id + '/historical?start=' + str(int(start)) + '&end=' + str(
                int(end)) + '&limit=5000&interval=1d')
        if paprika_res.ok:
            return json.loads(paprika_res.text)
        return []

    def get_csv_file_name(self):
        folder_path = self.get_csv_folder()
        file_name = self.get_csv_name('all')
        return os.path.join(os.path.dirname(folder_path), f'{file_name}.csv')

    def get_csv_folder(self):
        dags_folder = project_config.dags_folder
        folder_path = os.path.join(dags_folder, '../data/token_price/price_' + self.start_str + '_' + self.end_str)
        return folder_path

    def get_csv_name(self, id: str):
        return 'price_' + id + '-' + self.start_str + '-' + self.end_str

    def get_sub_csv_path(self, id: str):
        folder_path = self.get_csv_folder()
        file_name = self.get_csv_name(id)
        file_path = os.path.join(folder_path, f'{file_name}.csv')
        ensure_dir(file_path)
        return file_path

    def get_one_token_price(self, item):
        id: str = item['id']
        start: str = item['start']
        end: str = item['end']
        csv_file = self.get_sub_csv_path(id)
        if os.path.isfile(csv_file):
            print("skip:" + id)
            return

        address = pydash.get(item, 'address', '')
        prices = self.get_history_price_per_day_csv(id, start, end)
        if len(prices) == 0:
            return
        csv_results = []
        for price in prices:
            day = moment.date(price['timestamp']).date.strftime("%Y-%m-%d")
            csv_results.append({
                'address': address.lower(),
                'price': price['price'],
                'day': day
            })

        df = pd.DataFrame(csv_results)
        df.to_csv(csv_file, index=False, header=True)

    def load_all_price_data(self, token_address=None):
        date_parse = get_start_end_timestamp(self.start_str, self.end_str)
        start = date_parse['start']
        end = date_parse['end']
        tokens_cfg = read_tokens()
        tokens = tokens_cfg.sections()
        items = []

        for name_index in range(len(tokens)):
            item = tokens_cfg[tokens[name_index]]
            address = pydash.get(item, 'address', '')
            if address == '':
                continue
            if token_address and address != token_address:
                continue
            obj = {
                'start': start,
                'end': end,
                'address': pydash.get(item, 'address', ''),
                'symbol': pydash.get(item, 'symbol', ''),
                'decimals': pydash.get(item, 'decimals', ''),
                'id': pydash.get(item, 'id', ''),
            }
            items.append(obj)
        for item in items:
            self.get_one_token_price(item)

    def merge_csv(self):
        one_file_path = self.get_csv_file_name()
        folder_path = self.get_csv_folder()

        all_filenames = [i for i in glob.glob(folder_path + '/*.csv')]
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        combined_csv.to_csv(one_file_path, index=False)

    def do_scrapy_data(self):
        self.load_all_price_data()
        self.merge_csv()

    def do_upload_csv_to_gsc(self):
        source_csv_file = self.get_csv_file_name()
        self.execution_date_str = self.start_str + '_' + self.end_str
        destination_file_path = '{name}/date={execution_date}/all_{execution_date}.csv'.format(
            name=self.task_name,
            execution_date=self.execution_date_str)
        self.upload_csv_to_gsc_with_cache(source_csv_file, destination_file_path)

