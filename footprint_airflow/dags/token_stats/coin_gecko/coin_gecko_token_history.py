# get coin_gecko_history price api
import os
from pycoingecko import CoinGeckoAPI
import pydash
from datetime import datetime
import pandas
from models import Token, TokenDailyStats
from utils.date_util import DateUtil
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
import json
import time
from config import project_config

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

day_times = [
]

name = 'token_daily_stats'


def get_coins():
    coin_list = cg.get_coins_list()
    return coin_list


def get_history_market_from_coin_gecko():
    coins = get_history_data_token()
    for day in day_times:
        day_result = []
        for coin in coins:
            try:
                value = handle_history_data(coin['id'], day)
                if value:
                    day_result.append(value)

            except Exception as e:
                print('get coin detail fail', e)

            print(day, coin['id'])
        print(day_result)
        df = pandas.DataFrame(day_result, columns=columns)
        csv_file = get_token_daily_stats_csv_name(day)
        df.to_csv(csv_file, index=False, header=True)
        handle_upload_csv_to_gsc(day)

    handle_import_gsc_csv_to_bigquery()


def handle_history_data(coin_id, day_time):
    time.sleep(2)
    to_date = datetime.strptime(day_time, '%Y-%m-%d')
    date_format_str = datetime.strftime(to_date, '%d-%m-%Y')
    history_detail = cg.get_coin_history_by_id(id=coin_id, date=date_format_str)
    value = format_data(day_time, history_detail)
    return value


def format_data(day_time, history_detail):
    total_volume = pydash.get(history_detail, 'market_data.total_volume.usd')
    price = pydash.get(history_detail, 'market_data.current_price.usd')
    market_cap = pydash.get(history_detail, 'market_data.market_cap.usd')
    symbol = pydash.get(history_detail, 'symbol')
    coin_gecko_id = pydash.get(history_detail, 'id')

    if not price and not market_cap and not total_volume:
        print('format_data =>', coin_gecko_id, price, market_cap, total_volume)
        return None

    token = Token.find_one(query={'coin_gecko_id': coin_gecko_id, 'symbol': symbol})
    address = pydash.get(token, 'token')
    value = {
        'coin_gecko_id': coin_gecko_id,
        'symbol': symbol,
        'day': day_time + ' 00:00:00',
        'address': address,
        'price': price,
        'market_cap': market_cap,
        'trading_vol_24h': total_volume,
        'created_at': DateUtil.utc_current(),
        'updated_at': DateUtil.utc_current()
    }
    query = {
        'coin_gecko_id': coin_gecko_id,
        'day': DateUtil.utc_start_of_date(datetime.strptime('{day_time} 00:00:00'.format(day_time=day_time), '%Y-%m-%d %H:%M:%S'))
    }
    update = {
        'symbol': symbol,
        'address': address,
        'price': price,
        'market_cap': market_cap,
        'trading_vol_24h': total_volume,
        'created_at': DateUtil.utc_current(),
        'updated_at': DateUtil.utc_current()
    }
    TokenDailyStats.update_one(query=query, set_dict=update, upsert=True)
    return value

def handle_upload_csv_to_gsc(execution_date):
    source_csv_file = get_token_daily_stats_csv_name(execution_date)
    destination_file_path = '{name}/date={execution_date}/{name}_{execution_date}.csv'.format(name=name,
                                                                                              execution_date=execution_date)

    upload_csv_to_gsc(source_csv_file, destination_file_path)
    print(source_csv_file)
    print(destination_file_path)


def handle_import_gsc_csv_to_bigquery():
    custom_data_base = project_config.bigquery_etl_database
    import_gsc_to_bigquery(name=name, custom_data_base=custom_data_base)


def get_token_daily_stats_csv_name(day_time):
    dags_folder = project_config.dags_folder
    return os.path.join(dags_folder, 'token_stats/csv/{}_{}.csv'.format(name, day_time))


def get_history_data_token():
    dags_folder = project_config.dags_folder
    path = os.path.join(dags_folder, 'token_stats/coin_gecko/history_data_token.json')
    with open(path, 'r') as f:
        res = json.loads(f.read())
        return res
