from basic.etl_basic import ETLBasic
from config import project_config
import os
import json
import pydash
import requests
from utils.date_util import DateUtil
from models import CovalentLendingSource
import math

url = 'https://api.covalenthq.com/v1/{chain_id}/networks/{covalent_name}/assets/?key='


class CovalentLending(ETLBasic):
    columns = [
        'day',
        'protocol_id',
        'protocol_slug',
        'name',
        'chain',
        'version',
        'asset_address',
        'asset_symbol',
        'ctoken',
        'ctoken_name',
        'ctoken_symbol',
        'supply_apr',
        'borrow_apr',
        'supply_apy',
        'borrow_apy',
        'stable_borrow_apr',
        'variable_borrow_apr'
    ]

    task_name = 'covalent_lending_assets_daily_stats'

    table_name = 'covalent_lending_assets_daily_stats'

    task_airflow_execution_time = '30 23 * * *'

    def do_scrapy_data(self):
        self.handle_request_covalent_lending_daily_source()
        self.handle_arrange_covalent_lending_daily_source()

    def handle_request_covalent_lending_daily_source(self):
        covalent_lending_infos = self.get_covalent_lending_info()

        for covalent_lending_info in covalent_lending_infos:
            self.handle_covalent_info(covalent_lending_info)

    def handle_covalent_info(self, covalent_info):
        page_number = 0
        page_size = 100
        mark = True
        while mark:
            covalent_info_items = self.handle_request_covalent(covalent_info, page_number, page_size)
            if covalent_info_items:
                self.handle_save_covalent_lending_source(covalent_info, covalent_info_items)
                print("{slug} {chain} ==> page_number is {page_number}".format(slug=covalent_info['slug'], chain=covalent_info['chain'], page_number=page_number))
                page_number += 1
                if len(covalent_info_items) < page_size:
                    mark = False

    def handle_request_covalent(self, covalent_info, page_number, page_size):
        chain_id = covalent_info['chain_id']
        covalent_name = covalent_info['covalent_name']
        request_url = url.format(chain_id=chain_id, covalent_name=covalent_name)
        request_url = request_url + '&page-size={}'.format(page_size) + '&page-number={}'.format(page_number)
        request_result = requests.get(request_url)
        request_result = json.loads(request_result.text)
        print(len(request_result['data']['items']))
        items = pydash.get(request_result, 'data.items')
        return items

    def handle_save_covalent_lending_source(self, covalent_info, covalent_info_items):
        coin_gecko_id = pydash.get(covalent_info, 'coin_gecko_id')
        chain = pydash.get(covalent_info, 'chain')
        chain_id = pydash.get(covalent_info, 'chain_id')
        protocol_id = pydash.get(covalent_info, 'protocol_id')
        name = pydash.get(covalent_info, 'name')
        slug = pydash.get(covalent_info, 'slug')
        version = pydash.get(covalent_info, 'version')
        covalent_name = pydash.get(covalent_info, 'covalent_name')

        for covalent_info_item in covalent_info_items:
            underlying_contract_address = pydash.get(covalent_info_item, 'underlying.contract_address')
            query = {
                'coin_gecko_id': coin_gecko_id,
                'chain': chain,
                'day': self.execution_date,
                'underlying_contract_address': underlying_contract_address
            }
            add_dict = {
                'created_at': DateUtil.utc_current(),
                'updated_at': DateUtil.utc_current(),
                'chain_id': chain_id,
                'covalent_name': covalent_name,
                'protocol_id': protocol_id,
                'protocol_name': name,
                'protocol_slug': slug,
                'protocol_version': version
            }
            update = dict(add_dict, **covalent_info_item)
            CovalentLendingSource.update_one(query=query, set_dict=update, upsert=True)

    def exec_local(self):

        self.do_start()

        self.do_scrapy_data()

        self.do_upload_csv_to_gsc()

        self.do_import_gsc_to_bigquery()

        self.do_common_valid()

        self.do_customize_valid()

        self.do_end()

    def handle_arrange_covalent_lending_daily_source(self):
        covalent_daily_result = []
        query = {
            'day': self.execution_date
        }
        total = CovalentLendingSource.count(query)

        limit = 500
        round_times = math.ceil(total / limit)
        for round_time in range(0, round_times):
            print('handle_arrange_covalent_lending_daily_source', round_time)
            covalent_daily_sources = list(CovalentLendingSource.find(query).skip(round_time * limit).limit(limit))
            for covalent_daily_source in covalent_daily_sources:
                format_result = self.handle_covalent_lending_daily_stats(covalent_daily_source)
                covalent_daily_result.append(format_result)

        self.do_write_csv(covalent_daily_result)

    def handle_covalent_lending_daily_stats(self, covalent_daily_source):
        protocol_id = pydash.get(covalent_daily_source, 'protocol_id')
        slug = pydash.get(covalent_daily_source, 'protocol_slug')
        protocol_name = pydash.get(covalent_daily_source, 'protocol_name')
        chain = pydash.get(covalent_daily_source, 'chain')
        version = pydash.get(covalent_daily_source, 'protocol_version')
        asset_address = pydash.get(covalent_daily_source, 'underlying.contract_address')
        asset_symbol = pydash.get(covalent_daily_source, 'underlying.contract_ticker_symbol')
        ctoken = pydash.get(covalent_daily_source, 'atoken.contract_address') or pydash.get(covalent_daily_source, 'ctoken.contract_address')
        ctoken_name = pydash.get(covalent_daily_source, 'atoken.contract_name') or pydash.get(covalent_daily_source, 'ctoken.contract_name')
        ctoken_symbol = pydash.get(covalent_daily_source, 'atoken.contract_ticker_symbol') or pydash.get(covalent_daily_source, 'ctoken.contract_ticker_symbol')
        supply_apr = pydash.get(covalent_daily_source, 'supply_apr')
        borrow_apr = pydash.get(covalent_daily_source, 'borrow_apr')
        supply_apy = pydash.get(covalent_daily_source, 'supply_apy')
        borrow_apy = pydash.get(covalent_daily_source, 'borrow_apy')
        stable_borrow_apr = pydash.get(covalent_daily_source, 'stable_borrow_apr')
        variable_borrow_apr = pydash.get(covalent_daily_source, 'variable_borrow_apr')
        return {
            'day': self.execution_date_str,
            'protocol_id': protocol_id,
            'protocol_slug': slug,
            'name': protocol_name,
            'chain': chain,
            'version': version,
            'asset_address': asset_address,
            'asset_symbol': asset_symbol,
            'ctoken': ctoken,
            'ctoken_name': ctoken_name,
            'ctoken_symbol': ctoken_symbol,
            'supply_apr': supply_apr,
            'borrow_apr': borrow_apr,
            'supply_apy': supply_apy,
            'borrow_apy': borrow_apy,
            'stable_borrow_apr': stable_borrow_apr,
            'variable_borrow_apr': variable_borrow_apr
        }

    def get_csv_file_name(self):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, '../data/lending/{}_{}.csv'.format(self.task_name, self.execution_date_str))

    def get_covalent_lending_info(self):
        dags_folder = project_config.dags_folder
        path = os.path.join(dags_folder, 'lending/lending_asset/covalent_lending_info.json')
        with open(path, 'r') as f:
            res = json.loads(f.read())
            return res


if __name__ == '__main__':
    CovalentLending().exec_local()



