import configparser
import json
import os
import pydash
import requests
import tqdm
from joblib import Parallel, delayed

from config import project_config
from token_stats.coin_paprika.etl_token_daily_price import TokenDailyPrice, get_start_end_timestamp, coin_paprika_host
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery


def read_tokens():
    dags_folder = project_config.dags_folder
    cfg = configparser.ConfigParser()
    cfg_path = os.path.join(dags_folder, 'token_stats/coin_paprika/prices_full.ini')
    cfg.read(cfg_path)
    return cfg


class TokenDailyPriceFull(TokenDailyPrice):
    task_name = 'token_daily_price_full'
    table_name = 'token_daily_price_full'
    columns = None

    task_airflow_execution_time = '30 1 * * *'

    def load_all_price_data(self, token_address=None):
        date_parse = get_start_end_timestamp(self.start_str, self.end_str)
        start = date_parse['start']
        end = date_parse['end']
        tokens_cfg = read_tokens()
        tokens = tokens_cfg.sections()
        print("tokens len :", len(tokens))
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

        Parallel(n_jobs=5, backend='multiprocessing')(
            delayed(self.get_one_token_price)(item) for item in tqdm.tqdm(items))

    def get_history_price_per_day_csv(self, id, start, end, count=0):
        try:
            paprika_res = requests.get(
                url=f"{coin_paprika_host}/v1/tickers/{id}/historical?start={int(start)}&end={int(end)}&limit=5000&interval=1d")
            if paprika_res.ok:
                return json.loads(paprika_res.text)
        except Exception as e:
            if count < 3:
                return self.get_history_price_per_day_csv(id, start, end, count=count+1)
        return []

    def get_csv_folder(self):
        dags_folder = project_config.dags_folder
        folder_path = os.path.join(dags_folder, '../data/token_price_full/price_' + self.start_str + '_' + self.end_str)
        return folder_path

    def do_import_gsc_to_bigquery(self):
        import_gsc_to_bigquery('token_daily_price_full', custom_data_base='footprint_model_etl')
