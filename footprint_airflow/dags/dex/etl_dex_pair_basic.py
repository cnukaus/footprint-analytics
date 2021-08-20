from basic.etl_basic import ETLBasic
from config import project_config
import os.path
import pandas as pd
import moment
from models import BigQueryCheckPoint
from datetime import timedelta, datetime
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.date_util import DateUtil
from models import MonitorDashBoard

dt = None


def slug(s: str):
    return s.lower().replace(' ', '-')


def load_dex_info():
    global dt
    if dt is not None:
        return dt
    dags_folder = project_config.dags_folder
    df = pd.read_csv(os.path.join(dags_folder, './dex/dex-protocol-info.csv'))
    dt = {}
    for info in df.to_dict(orient='records'):
        dt[info['name'] + info['protocol']] = {
            'name': info['new name'],
            'protocol_id': info['protocol_id']
        }
    return dt


class ETLDexPairBasic(ETLBasic):
    network = ''
    chain = ''
    dexs: dict = load_dex_info()
    skip_cache = False
    task_airflow_execution_time = '30 1 * * *'
    task_name = 'dex_pair_daily_stats'
    table_name = 'dex_pair_daily_stats'

    valid_null_fields = [
        'token_0_price',
        'token_1_price',
        'trade_count'
    ]

    valid_less_than_zero_fields = [
        'token_0_price',
        'token_1_price',
        'trade_count'
    ]

    def __init__(self, execution_date=None):
        super().__init__(execution_date)
        transport = AIOHTTPTransport(url="https://graphql.bitquery.io")
        self.gql_client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=240)

    def get_execution_date(self):
        now = moment.utcnow().datetime
        yesterday = now - timedelta(days=1)  # yesterday time
        return yesterday

    def do_scrapy_data(self):
        df = self.load_data()
        self.save_pairs(df)

    def get_pairs_from_bitquery(self):
        print('start fetch bitquery data ')
        fliter_ = """{
              ethereum(network: %s) {
                dexTrades(
                  options: {limit: 100000, asc: "timeInterval.day"}
                  date: {is: "%s"}
                ) {
                  timeInterval {
                    day(count: 1)
                  }
                  protocol
                  exchange{
                    name
                  }
                  baseCurrency {
                    symbol
                    address
                  }
                  quoteCurrency {
                    symbol
                    address
                  }
                  baseAmount
                  quoteAmount
                  tradeAmount(in: USD)
                  trades: count
                  side
                }
              }
            }
            """ % (self.network, self.execution_date_str)
        # Execute the query on the transport
        print('query from bitquery ', fliter_)
        result = self.gql_client.execute(gql(fliter_))
        pairs = result['ethereum']['dexTrades']
        print('get pairs from bitquery nums ', len(pairs))
        # time.sleep(20)
        return pairs

    def flatten_column(self, df: pd.DataFrame, column_names: list):
        sub_dfs = []
        for column in column_names:
            print('flatten column', column)
            sub_df = pd.json_normalize(df[column].tolist()).add_prefix(column + '_')
            sub_dfs.append(sub_df)
        df = df.drop(column_names, axis=1)
        df = df.join(sub_dfs)
        return df

    def get_csv_file_name(self):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, '../data/dex/{}_pairs-{}.csv'.format(
            self.network.lower(),
            self.execution_date_str))

    def get_pair_price_csv_filename(self):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder,
                            '../data/dex/{}_pairs_price_{}.csv'.format(
                                self.network.lower(),
                                self.execution_date_str))

    def load_data(self):
        if os.path.isfile(self.get_csv_file_name()):
            return
        pairs = self.get_pairs_from_bitquery()
        df = pd.DataFrame(pairs)
        df = self.flatten_column(df, ['baseCurrency', 'quoteCurrency', 'timeInterval', 'exchange'])
        df.to_csv(self.get_csv_file_name(), index=False)
        return df

    def fix_name_and_set_pair_usd_price(self, obj):
        key = obj['exchange_name'] + obj['protocol']
        if key in self.dexs:
            obj['exchange_name'] = self.dexs[key]['name']
            obj['protocol_id'] = self.dexs[key]['protocol_id']
        else:
            obj['protocol_id'] = 0

        obj['slug'] = slug(obj['exchange_name'])
        obj['token_0_price'] = 0 if obj['baseAmount'] == 0 else obj['tradeAmount'] / obj['baseAmount']
        obj['token_1_price'] = 0 if obj['quoteAmount'] == 0 else obj['tradeAmount'] / obj['quoteAmount']
        return obj

    def save_pairs(self, df):
        if os.path.isfile(self.get_pair_price_csv_filename()):
            if not self.skip_cache:
                return

        df['protocol_id'] = 0
        df['slug'] = 'none'
        df['chain'] = self.chain

        df = df[df['exchange_name'].notna()]

        df = df.apply(self.fix_name_and_set_pair_usd_price, axis=1)
        df = df.rename(columns={
            'tradeAmount': 'volume',
            'exchange_name': 'name',
            'baseCurrency_address': 'token_0',
            'quoteCurrency_address': 'token_1',
            'baseCurrency_symbol': 'token_0_symbol',
            'quoteCurrency_symbol': 'token_1_symbol',
            'timeInterval_day': 'day',
            'trades': 'trade_count',
        })
        df = df[['day', 'protocol_id', 'slug', 'protocol', 'name', 'chain', 'volume',
                 'token_0', 'token_0_symbol', 'token_0_price',
                 'token_1', 'token_1_symbol', 'token_1_price',
                 'trade_count']]
        df.to_csv(self.get_pair_price_csv_filename(), index=False)

    def do_upload_csv_to_gsc(self):
        source_csv_file = self.get_pair_price_csv_filename()
        task_name = self.network.lower() + '_' + self.task_name
        destination_file_path = '{folder}/{execution_date}_{name}.csv'.format(
            folder=self.task_name,
            name=task_name,
            execution_date=self.execution_date_str)
        self.upload_csv_to_gsc_with_cache(source_csv_file, destination_file_path)

    def airflow_dag_params(self):
        dag_params = {
            "dag_id": "footprint_{}_dag".format(self.task_name + '_' + self.network),
            "catchup": False,
            "schedule_interval": self.task_airflow_execution_time,
            "description": "{}_dag".format(self.task_name),
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 7, 1)
            },
            "dagrun_timeout": timedelta(days=30)
        }
        print('dag_params', dag_params)
        return dag_params

    def get_monthly_file_name(self, month_str):
        dags_folder = project_config.dags_folder
        one_file_path = os.path.join(dags_folder, '../data/dex_monthly/{}_pairs_price_{}.csv'.format(
            self.network.lower(), month_str))
        return one_file_path

    def merge_csv(self, days, month_str):
        dags_folder = project_config.dags_folder
        print(month_str)
        one_file_path = self.get_monthly_file_name(month_str)

        if os.path.isfile(one_file_path):
            return

        all_filenames = []
        for d in days:
            p = os.path.join(dags_folder, '../data/dex/{}_pairs_price_{}.csv'.format(
                self.network.lower(), d))
            if not os.path.isfile(p):
                print(p)
                raise Exception('file not exist ' + d)
            all_filenames.append(p)

        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        combined_csv.to_csv(one_file_path, index=False)
        print(f'merge {len(all_filenames)} files to one {one_file_path}')

    def upload_monthly_csv_to_gsc(self, month_str):
        source_csv_file = self.get_monthly_file_name(month_str)
        task_name = self.network.lower() + '_' + self.task_name
        destination_file_path = '{folder}/{execution_date}_{name}.csv'.format(
            folder=self.task_name,
            name=task_name,
            execution_date=month_str)
        print('destination_file_path', destination_file_path)
        self.upload_csv_to_gsc_with_cache(source_csv_file, destination_file_path)

    def merge_csv_by_month(self, start, end):
        start_date = moment.utc(start).datetime
        end_date = moment.utc(end).datetime
        month = start_date.month
        days = []
        for n in range(1, int((end_date - start_date).days) + 1):
            it = (start_date + timedelta(n))
            pass_day = (start_date + timedelta(n - 1))
            if it.month != month:
                print(days)
                month_str = '{}-{}'.format(pass_day.year, pass_day.month)
                self.merge_csv(days, month_str)
                self.upload_monthly_csv_to_gsc(month_str)
                month = it.month
                days = []
            days.append(it.strftime("%Y-%m-%d"))

    def upload_csv_to_gsc_with_cache(self, source_csv_file, destination_file_path):
        if BigQueryCheckPoint.has_check_point(destination_file_path, self.execution_date_str):
            return False
        upload_csv_to_gsc(source_csv_file, destination_file_path)
        BigQueryCheckPoint.set_check_point(destination_file_path, self.execution_date_str)

    def save_monitor(self, rule_name: str, item_value: int, result_code: int, desc: str, desc_cn: str, sql: str, field: str = None):
        query = {
            'task_name': self.task_name + '_' + self.network,
            'rule_name': rule_name,
            'field': field or '',
            'stats_date': DateUtil.utc_start_of_date(self.execution_date)
        }

        update = {
            'database_name': project_config.bigquery_etl_database,
            'table_name': self.table_name,
            'desc': desc,
            'item_value': item_value,
            'result_code': result_code,
            'sql': sql,
            'desc_cn': desc_cn
        }
        MonitorDashBoard.update_one(query=query, set_dict=update, upsert=True)
