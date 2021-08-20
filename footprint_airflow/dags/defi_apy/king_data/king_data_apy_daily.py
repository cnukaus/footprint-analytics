import moment
import requests
import json
from config import project_config
import os
from utils.query_bigquery import query_bigquery
from basic.etl_basic import ETLBasic
from datetime import datetime
from utils.date_util import DateUtil
from utils.upload_csv_to_gsc import upload_csv_to_gsc

host = 'https://kingdata.com'
borrow_url = host + '/api/apy/dataset/?chain=all&category=lend_single&lang=cn'
deposit_url = host + '/api/apy/dataset/?chain=all&category=loan_single&lang=cn'


class ApyDaily(ETLBasic):
    task_name = 'apy_daily'
    task_airflow_execution_time = '20 0 * * *'
    task_name = 'apy_daily'

    columns = [
        'protocol_id',
        'chain',
        'token_address',
        'day',
        'type',
        'apy'
    ]

    valid_null_fields = [
        'apy'
    ]

    valid_less_than_zero_fields = []

    def __init__(self):
        do_task_date = moment.utc(datetime.now()).add('days', -1).date
        super().__init__(do_task_date)

    def do_scrapy_data(self):
        king_data_result = []
        data_sources = [borrow_url, deposit_url]
        for index in range(len(data_sources)):
            url = data_sources[index]
            apy_type = 'borrow'
            if index != 0:
                apy_type = 'deposit'
            loan_res = requests.get(url=url)
            result = json.loads(loan_res.text)
            print(result)
            if result['code'] != 0:
                raise ValueError(f'{result["code"]} variable not is 0')

            loan_datas = result['data']
            loan_data_format_result = self.handle_loan_data(loan_datas, apy_type)
            king_data_result = king_data_result + loan_data_format_result

        self.do_write_csv(king_data_result)

    def handle_loan_data(self, loan_datas, apy_type):
        format_result = []
        for loan_data in loan_datas:
            project_name = loan_data['project_name']
            details = loan_data['data']
            for token_name in details.keys():
                token_result = self.handle_token_data(project_name, apy_type, details, token_name)
                print('handle_loan_data', token_result)
                if token_result is not None:
                    format_result.append(token_result)

        return format_result

    def do_upload_csv_to_gsc(self):
        print('upload csv to gsc ...')
        source_csv_file = self.get_csv_file_name()
        destination_file_path = '{name}/date={execution_date}/all_{execution_date}.csv'.format(name=self.task_name, execution_date=self.execution_date_str)
        upload_csv_to_gsc(source_csv_file, destination_file_path)

    def handle_token_data(self, project_name, apy_type, details, token_name):
        detail = details[token_name]
        chain = detail['chain']
        token_name = token_name
        print(token_name, chain)
        if (chain != 'eth' and chain != 'bsc') or token_name == 'MDX':
            return None
        apy = detail['apy']
        address = self.get_token_address(chain, token_name)
        protocol_id = self.get_protocol_id(self.format_chain(chain), project_name)
        if not protocol_id:
            return None
        return {
            'protocol_id': protocol_id,
            'chain': self.format_chain(chain),
            'token_address': address,
            'day': self.execution_date_str,
            'type': apy_type,
            'apy': apy,
        }

    def get_token_address(self, chain, token_name):
        data = {
            'bsc': {
                'ETH': '0x2170ed0880ac9a755fd29b2688956bd959f933f8',
                'BTC': '0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c',
                'USDT': '0x55d398326f99059ff775485246999027b3197955',
                'DAI': '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3',
                'USDC': '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'
            },
            'eth': {
                'ETH': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                'BTC': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                'USDT': '0xdac17f958d2ee523a2206206994597c13d831ec7',
                'DAI': '0x6b175474e89094c44da98b954eedeac495271d0f',
                'USDC': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                'WBTC': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                'AAVE': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
            }
        }
        if chain in data:
            if token_name in data[chain]:
                return data[chain][token_name]
        raise ValueError('not find token address chain:' + chain + ' token_name:' + token_name)

    def get_csv_file_name(self):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, '../data/apy_daily/{}_{}.csv'.format(self.task_name, self.execution_date_str))

    def format_chain(self, chain):
        if chain == 'eth':
            return 'Ethereum'
        if chain == 'bsc':
            return 'Binance'
        raise ValueError(f'format {chain} fail')

    def get_protocol_id(self, chain, name):
        sql = self.get_protocol_id_sql(chain, name)
        df = query_bigquery(sql)
        print(df)
        if 'protocol_id' in df:
            id = df['protocol_id'].values
            id_list = list(id)
            if len(id_list) > 0:
                return int(id_list[0])
        return None

    def get_protocol_id_sql(self, chain, name):
        query_string = 'SELECT name,chain,protocol_id FROM `footprint_etl.defi_protocol_info` where upper(name) = "{{name}}" and chain = "{{chain}}"'
        query_string = query_string.replace('{{name}}', name.upper())
        query_string = query_string.replace('{{chain}}', chain)
        return query_string

    def continuity_sql(self):
        date7ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 7, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
        SELECT day,
             protocol_id,
             type,
             chain,
             address,
             question_type
    FROM 
        (SELECT *
        FROM 
            (SELECT day,
             protocol_id,
             chain,
             type,
             address,
             'missing_data' AS question_type
            FROM 
                (SELECT DISTINCT day,
             protocol_id,
             chain,
             type,
             address,
             LEAD(day,
             1)
                    OVER (PARTITION BY chain, protocol_id, type, address
                ORDER BY  day DESC) AS lastday
                FROM `xed-project-237404.{data_base}.apy_daily`
                WHERE day > {date7ago}
                ORDER BY  day ASC )
                WHERE DATE_DIFF(day, lastday, day) >1) m
                UNION
                ALL 
                    (SELECT day,
             protocol_id,
             chain,
             type,
             address,
             'duplication_data' AS question_type,
                    FROM `xed-project-237404.{data_base}.apy_daily`
                    WHERE day > {date7ago}
                    GROUP BY  day, type, chain, protocol_id, address
                    HAVING COUNT(concat(day, '-', type, '-', chain, '-', protocol_id, '-', address)) >1
                    ORDER BY  day DESC)
                    ORDER BY  day)
        """.format(date7ago=date7ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def data_is_null_sql(self):
        execution_date_str = "'" + self.execution_date_str + "'"
        return """
                SELECT day, apy, 'null_value' AS question_type
                    FROM `xed-project-237404.{data_base}.apy_daily`
                    WHERE day = {execution_date}
                        AND apy IS NULL
                    UNION ALL
                    (SELECT day, apy, 'less_than_zero' AS question_type
                    FROM `xed-project-237404.{data_base}.apy_daily`
                    WHERE day = {execution_date}
                        AND apy < 0)
            """.format(execution_date=execution_date_str, data_base=project_config.bigquery_etl_database)

    def anomaly_sql(self):
        return ''
