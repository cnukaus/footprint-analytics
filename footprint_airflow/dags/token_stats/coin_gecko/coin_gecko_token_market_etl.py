from basic.etl_basic import ETLBasic
from pycoingecko import CoinGeckoAPI
from token_stats.coin_gecko.token_daily_stats_format import token_daily_stats_format
from utils.date_util import DateUtil
from datetime import datetime
import time
import pydash
from models import Token, TokenDailyStats, CoinGeckoTokenMarketDailySource
from config import project_config
import os
import math


class CoinGeckoTokenMarketETL(ETLBasic):
    cg = CoinGeckoAPI()

    columns = [
        'symbol',
        'address',
        'coin_gecko_id',
        'day',
        'trading_vol_24h',
        'high_price_24h',
        'low_price_24h',
        'circulating_supply',
        'facebook_likes',
        'fdv_to_tvl_ratio',
        'fully_diluted_valuation',
        'market_cap',
        'market_cap_rank',
        'max_supply',
        'mcap_to_tvl_ratio',
        'price',
        'reddit_accounts_active_48h',
        'reddit_average_comments_48h',
        'reddit_average_posts_48h',
        'reddit_subscribers',
        'telegram_channel_user_count',
        'total_supply',
        'total_value_locked',
        'twitter_followers',
        'created_at',
        'updated_at'
    ]

    valid_null_fields = [
        'price',
        'trading_vol_24h',
        'low_price_24h',
        'high_price_24h',
        'market_cap',
        'circulating_supply'
    ]

    valid_less_than_zero_fields = [
        'price'
    ]

    table_name = 'token_daily_stats'

    task_name = 'token_daily_stats'

    task_airflow_execution_time = '0 23 * * *'

    remark = ''

    def __init__(self):
        super().__init__()

    def get_execution_date(self):
        do_task_date = DateUtil.utc_start_of_date(DateUtil.utc_current())
        return do_task_date

    def do_scrapy_data(self):
        self.handle_request_token_market_daily_source()
        self.handle_arrange_token_market_daily_source()
        self.fix_market_cap_rank()
        self.save_format_data()

    def airflow_steps(self):
        return [
            self.do_start,
            self.do_scrapy_data,
            self.do_upload_csv_to_gsc,
            self.do_import_gsc_to_bigquery,
            self.do_common_valid,
            self.do_customize_valid,
            self.do_end
        ]

    def handle_arrange_token_market_daily_source(self):
        query = {
            'day': self.execution_date
        }
        total = CoinGeckoTokenMarketDailySource.count(query)

        limit = 500
        round_times = math.ceil(total / limit)
        for round_time in range(0, round_times):
            print('handle_arrange_token_market_daily_source', round_time)
            market_daily_sources = list(CoinGeckoTokenMarketDailySource.find(query).skip(round_time * limit).limit(limit))
            for market_daily_source in market_daily_sources:
                self.handle_token_daily_stats(market_daily_source)

    def handle_request_token_market_daily_source(self):
        execution_date = DateUtil.utc_start_of_date(self.execution_date)
        page = 1
        per_page = 250
        mark = True
        while mark:
            market_details = self.get_coin_gecko_market_detail(per_page, page)
            if market_details:
                self.handle_save_token_market_daily_source(market_details, execution_date)
                print(f"page is {page}".format(page=page))
                page += 1
                if len(market_details) < per_page:
                    mark = False

    def handle_save_token_market_daily_source(self, market_details, execution_date):
        for market_detail in market_details:
            coin_gecko_id = pydash.get(market_detail, 'id')
            query = {
                "coin_gecko_id": coin_gecko_id,
                "day": execution_date
            }
            time_dict = {
                "created_at": DateUtil.utc_current(),
                "updated_at": DateUtil.utc_current()
            }

            update = dict(time_dict, **market_detail)
            CoinGeckoTokenMarketDailySource.update_one(query=query, set_dict=update, upsert=True)

    def get_coin_gecko_market_detail(self, per_page, page):
        time.sleep(2)
        try:
            market_details = self.cg.get_coins_markets(vs_currency='usd', per_page=per_page, page=page, order='market_cap_desc')
            return market_details
        except Exception as e:
            print('get coin market detail fail', e)
        return None

    def handle_token_daily_stats(self, token_daily_source):
        symbol = pydash.get(token_daily_source, 'symbol')
        coin_gecko_id = pydash.get(token_daily_source, 'id')
        if symbol is None:
            return None

        query = {
            'coin_gecko_id': coin_gecko_id
        }
        token_result = Token.find_one(query=query)

        if token_result is None:
            return None

        coin_gecko_id = pydash.get(token_result, 'coin_gecko_id')
        address = pydash.get(token_result, 'token')
        price = pydash.get(token_daily_source, 'current_price')
        market_cap = pydash.get(token_daily_source, 'market_cap')
        fully_diluted_valuation = pydash.get(token_daily_source, 'fully_diluted_valuation')
        total_supply = pydash.get(token_daily_source, 'total_supply')
        max_supply = pydash.get(token_daily_source, 'max_supply')
        circulating_supply = pydash.get(token_daily_source, 'circulating_supply')
        market_cap_rank = pydash.get(token_daily_source, 'market_cap_rank')
        _24h_low_price = pydash.get(token_daily_source, 'low_24h')
        _24h_high_price = pydash.get(token_daily_source, 'high_24h')
        _24_hour_trading_vol = pydash.get(token_daily_source, 'total_volume')

        query = {
            'coin_gecko_id': coin_gecko_id,
            'day': DateUtil.utc_start_of_date(dt=self.execution_date)
        }
        update = {
            'symbol': symbol,
            'address': address,
            'price': price,
            'market_cap': market_cap,
            'fully_diluted_valuation': fully_diluted_valuation,
            'total_supply': total_supply,
            'max_supply': max_supply,
            'circulating_supply': circulating_supply,
            'market_cap_rank': market_cap_rank,
            'low_price_24h': _24h_low_price,
            'high_price_24h': _24h_high_price,
            'trading_vol_24h': _24_hour_trading_vol,
            'created_at': DateUtil.utc_current(),
            'updated_at': DateUtil.utc_current()
        }
        TokenDailyStats.update_one(query=query, set_dict=update, upsert=True)

    def save_format_data(self):
        query = {
            'day': self.execution_date
        }
        total = TokenDailyStats.count(query)
        limit = 500
        round_times = math.ceil(total / limit)
        format_data = []
        for round_time in range(0, round_times):
            token_daily_stats = list(TokenDailyStats.find(query).skip(round_time * limit).limit(limit))
            for token_daily in token_daily_stats:
                format_result = token_daily_stats_format(token_daily)
                format_data.append(format_result)

        self.do_write_csv(format_data)

    def get_csv_file_name(self):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, '../data/token_stats/{}_{}.csv'.format(self.task_name, self.execution_date_str))

    def data_is_null_sql(self):
        execution_date_str = "'" + datetime.strftime(self.execution_date, '%Y-%m-%d') + "'"
        query_str = """
                SELECT day, coin_gecko_id, price, trading_vol_24h, low_price_24h, high_price_24h, market_cap, circulating_supply, 'null_value' AS question_type
                    FROM `xed-project-237404.{data_base}.token_daily_stats`
                    WHERE day = {execution_date}
                        AND market_cap > 0
                        AND market_cap < 1000
                        AND (price IS NULL or trading_vol_24h IS NULL or low_price_24h IS NULL or high_price_24h IS NULL or market_cap IS NULL or circulating_supply IS NULL)
                    UNION ALL
                    (SELECT day, coin_gecko_id, price, trading_vol_24h, low_price_24h, high_price_24h, market_cap, circulating_supply, 'less_than_zero' AS question_type
                    FROM `xed-project-237404.{data_base}.token_daily_stats`
                    WHERE day = {execution_date}
                        AND market_cap > 0
                        AND market_cap < 1000
                        AND (price < 0 or trading_vol_24h < 0 or low_price_24h < 0 or high_price_24h < 0 or market_cap < 0 or circulating_supply < 0))
            """.format(execution_date=execution_date_str, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_core_field_exists_null_check(self, core_field_null_result):
        print('token daily check the data. Null data appears in the core field data of the day')

    def handle_core_field_exists_less_than_zero_check(self, core_field_less_zero_result):
        print('token daily check the data. The core field data of the current day is not allowed to be less than 0')

    def anomaly_sql(self):
        date_str = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 1, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
                WITH daily AS 
                    (SELECT day,
                         coin_gecko_id,
                         SUM(price) AS price,

                    FROM `xed-project-237404.{data_base}.token_daily_stats`
                    WHERE day > {date_str}
                    GROUP BY  day, coin_gecko_id ),
                avgforxd AS 
                    (SELECT day,
                         coin_gecko_id,
                         price,

                        (SELECT AVG(price)
                        FROM daily AS daily2
                        WHERE daily2.day < daily1.day
                                AND daily2.day > DATE_SUB(daily1.day, INTERVAL 3 DAY)
                                AND daily1.coin_gecko_id = daily2.coin_gecko_id ) AS price_past_xd
                        FROM daily daily1 )
                    SELECT *
                FROM avgforxd d
                WHERE d.price > d.price_past_xd * 4
                        OR d.price < d.price_past_xd * 0.25
                ORDER BY  d.day 
            """.format(date_str=date_str, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_exists_anomaly_check(self, anomaly_result):
        print('token daily check the data, and the core field data of the day has a field with large fluctuation')

    def continuity_sql(self):
        date7ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 7, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
            with result AS 
                (SELECT market_cap_rank,
                    coin_gecko_id,
                     symbol,
                    day
                FROM `xed-project-237404.{data_base}.token_daily_stats`
                WHERE market_cap_rank < 1000
                        AND market_cap_rank > 0
                        AND day > {date7ago}
                GROUP BY  day ,coin_gecko_id,symbol,market_cap_rank
                ORDER BY  day DESC ) , miss_data AS 
                (SELECT *
                FROM 
                    (SELECT *,
                     'missing_data' AS question_type
                    FROM 
                        (SELECT DISTINCT day,
                    market_cap_rank,
                     coin_gecko_id,
                     symbol,
                     lead(day,
                    1)
                            OVER ( partition by coin_gecko_id
                        ORDER BY  day desc) AS lastday
                        FROM `xed-project-237404.{data_base}.token_daily_stats`
                        ORDER BY  day ASC )
                        WHERE date_diff(day, lastday, day) >1 )m
                        UNION
                        all 
                            (SELECT day,
                    market_cap_rank,
                     coin_gecko_id,
                     symbol,
                     day AS day_time,
                     'duplication_data' AS question_type,
                            FROM `xed-project-237404.{data_base}.token_daily_stats`
                            WHERE day > {date7ago}
                            GROUP BY  day,coin_gecko_id,symbol,market_cap_rank
                            HAVING count(coin_gecko_id) > 1
                            ORDER BY  day DESC ) )
                        SELECT t.*
                    FROM 
                    (SELECT *
                    FROM miss_data )t
                INNER JOIN 
                (SELECT *
                FROM result) r
                ON t.day = r.day
                    AND t.coin_gecko_id = r.coin_gecko_id
                    AND t.symbol =r.symbol
            ORDER BY  t.day
        """.format(date7ago=date7ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_exists_missing_data_check(self, missing_data_result):
        print('token daily check the data, and there is missing data in recent 7 days')

    def handle_exists_duplicate_check(self, duplicate_data_result):
        print('token daily check the data, and there are duplicate data in the last 7 days')

    def fix_execution_date_stats(self):
        pass

    def fix_market_cap_rank(self):
        all_token_daily_stats = list(TokenDailyStats.find({'day': self.execution_date, 'row_status': {'$ne': 'disable'}}).sort('market_cap', -1))
        for index in range(0, len(all_token_daily_stats)):
            coin_gecko_id = all_token_daily_stats[index]['coin_gecko_id']
            day = all_token_daily_stats[index]['day']
            query = {
                'coin_gecko_id': coin_gecko_id,
                'day': day,
                'row_status': {'$ne': 'disable'}
            }
            update = {
                'market_cap_rank': index + 1
            }
            TokenDailyStats.update_one(query=query, set_dict=update)
