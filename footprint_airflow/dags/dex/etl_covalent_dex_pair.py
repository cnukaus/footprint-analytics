from basic.etl_basic import ETLBasic
from config import project_config
import os
import json
import requests
import pydash
import math
from utils.date_util import DateUtil
from models import CovalentDexPairSource

url = 'https://api.covalenthq.com/v1/{chain_id}/networks/{covalent_name}/assets/?key='


class ETLCovalentDexPair(ETLBasic):
    columns = [
        'day',
        'protocol_id',
        'slug',
        'name',
        'chain',
        'token_0',
        'token_0_symbol',
        'token_1',
        'token_1_symbol',
        'fee_24h_quote',
        'total_liquidity',
        'annualized_fee',
        'quote_rate'
    ]

    task_name = 'covalent_dex_pair_daily_stats'

    table_name = 'covalent_dex_pair_daily_stats'

    task_airflow_execution_time = '30 23 * * *'

    def do_scrapy_data(self):
        self.handle_request_covalent_daily_source()
        self.handle_arrange_covalent_daily_source()

    def handle_request_covalent_daily_source(self):
        covalent_infos = self.get_covalent_dex_info()

        for covalent_info in covalent_infos:
            self.handle_covalent_info(covalent_info)

    def handle_arrange_covalent_daily_source(self):

        covalent_daily_result = []
        query = {
            'day': self.execution_date
        }
        total = CovalentDexPairSource.count(query)

        limit = 500
        round_times = math.ceil(total / limit)
        for round_time in range(0, round_times):
            print('handle_arrange_covalent_daily_source', round_time)
            covalent_daily_sources = list(CovalentDexPairSource.find(query).skip(round_time * limit).limit(limit))
            for covalent_daily_source in covalent_daily_sources:
                format_result = self.handle_covalent_daily_stats(covalent_daily_source)
                covalent_daily_result.append(format_result)

        self.do_write_csv(covalent_daily_result)

    def handle_covalent_daily_stats(self, covalent_daily_source):
        protocol_id = pydash.get(covalent_daily_source, 'protocol_id')
        slug = pydash.get(covalent_daily_source, 'protocol_slug')
        protocol_name = pydash.get(covalent_daily_source, 'protocol_name')
        chain = pydash.get(covalent_daily_source, 'chain')
        token_0 = pydash.get(covalent_daily_source, 'token_0.contract_address')
        token_0_symbol = pydash.get(covalent_daily_source, 'token_0.contract_ticker_symbol')
        token_1 = pydash.get(covalent_daily_source, 'token_1.contract_address')
        token_1_symbol = pydash.get(covalent_daily_source, 'token_1.contract_ticker_symbol')
        fee_24h_quote = pydash.get(covalent_daily_source, 'fee_24h_quote')
        total_liquidity = pydash.get(covalent_daily_source, 'total_liquidity_quote')
        annualized_fee = pydash.get(covalent_daily_source, 'annualized_fee')
        quote_rate = pydash.get(covalent_daily_source, 'quote_rate')
        return {
            'day': self.execution_date_str,
            'protocol_id': protocol_id,
            'slug': slug,
            'name': protocol_name,
            'chain': chain,
            'token_0': token_0,
            'token_0_symbol': token_0_symbol,
            'token_1': token_1,
            'token_1_symbol': token_1_symbol,
            'fee_24h_quote': fee_24h_quote,
            'total_liquidity': total_liquidity,
            'annualized_fee': annualized_fee,
            'quote_rate': quote_rate
        }

    def handle_covalent_info(self, covalent_info):
        page_number = 0
        page_size = 100
        mark = True
        while mark:
            covalent_info_items = self.handle_request_covalent(covalent_info, page_number, page_size)
            if covalent_info_items:
                self.handle_save_request_covalent_source(covalent_info, covalent_info_items)
                print("{slug} {chain} ==> page_number is {page_number}".format(slug=covalent_info['slug'], chain=covalent_info['chain'], page_number=page_number))
                page_number += 1
                if len(covalent_info_items) < page_size:
                    mark = False

    def handle_request_covalent(self, covalent_info, page_number, page_size):
        try:
            chain_id = covalent_info['chain_id']
            covalent_name = covalent_info['covalent_name']
            request_url = url.format(chain_id=chain_id, covalent_name=covalent_name)
            request_url = request_url + '&page-size={}'.format(page_size) + '&page-number={}'.format(page_number)
            request_result = requests.get(request_url)
            request_result = json.loads(request_result.text)
            items = pydash.get(request_result, 'data.items')
            return items
        except Exception as e:
            print('get covalent detail fail', e)
        return None

    def handle_save_request_covalent_source(self, covalent_info, covalent_info_items):
        coin_gecko_id = pydash.get(covalent_info, 'coin_gecko_id')
        chain = pydash.get(covalent_info, 'chain')
        chain_id = pydash.get(covalent_info, 'chain_id')
        protocol_id = pydash.get(covalent_info, 'protocol_id')
        name = pydash.get(covalent_info, 'name')
        slug = pydash.get(covalent_info, 'slug')
        covalent_name = pydash.get(covalent_info, 'covalent_name')

        for covalent_info_item in covalent_info_items:
            exchange = pydash.get(covalent_info_item, 'exchange')
            query = {
                'coin_gecko_id': coin_gecko_id,
                'chain': chain,
                'day': self.execution_date,
                'exchange': exchange
            }
            add_dict = {
                'created_at': DateUtil.utc_current(),
                'updated_at': DateUtil.utc_current(),
                'chain_id': chain_id,
                'covalent_name': covalent_name,
                'protocol_id': protocol_id,
                'protocol_name': name,
                'protocol_slug': slug
            }
            update = dict(add_dict, **covalent_info_item)
            CovalentDexPairSource.update_one(query=query, set_dict=update, upsert=True)

    def exec_local(self):
        self.do_start()

        self.do_scrapy_data()

        self.do_upload_csv_to_gsc()

        self.do_import_gsc_to_bigquery()

        self.do_common_valid()

        self.do_customize_valid()

        self.do_end()

    def get_csv_file_name(self):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, '../data/dex/{}_{}.csv'.format(self.task_name, self.execution_date_str))

    def get_covalent_dex_info(self):
        dags_folder = project_config.dags_folder
        path = os.path.join(dags_folder, 'dex/covalent_dex_info.json')
        with open(path, 'r') as f:
            res = json.loads(f.read())
            return res


if __name__ == '__main__':
    ETLCovalentDexPair().exec_local()
