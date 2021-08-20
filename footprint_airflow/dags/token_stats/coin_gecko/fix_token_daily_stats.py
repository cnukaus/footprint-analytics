from models import TokenDailyStats
from models import Token
import pydash
from pycoingecko import CoinGeckoAPI
from config import project_config
import os
from joblib import Parallel, delayed
from tqdm import tqdm
import numpy as np
import math
from datetime import datetime
from utils.date_util import DateUtil
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
import pandas
import re

valid_date = DateUtil.utc_start_of_date(datetime.strptime('2021-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))

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


def transform_data_time():
    regex_day = re.compile(r'2021')
    query = {
        '$or': [
            {
                'day': re.compile(r'2020')
            },
            {
                'day': re.compile(r'2021')
            }
        ]
    }
    total = TokenDailyStats.count(query)

    limit = 100
    round_times = math.ceil(total / limit)

    print(range(0, round_times))
    items = np.array_split(range(0, round_times), 64)
    Parallel(n_jobs=64)(delayed(handle_transform)(item) for item in tqdm(items))


def handle_transform(round_times):
    regex_day = re.compile(r'2021')
    query = {
        '$or': [
            {
                'day': re.compile(r'2020')
            },
            {
                'day': re.compile(r'2021')
            }
        ]
    }
    limit = 100

    for round_time in round_times:
        round_time = int(round_time)
        token_daily_stats_arr = list(TokenDailyStats.find(query).skip(round_time * limit).limit(limit))
        for token_daily_stats in token_daily_stats_arr:
            print(token_daily_stats)
            id = pydash.get(token_daily_stats, '_id')
            day = pydash.get(token_daily_stats, 'day')
            if not type(day) is datetime:
                day = datetime.strptime(day, '%Y-%m-%d %H:%M:%S') if len(day) == 19 else datetime.fromisoformat(day)

            created_at = pydash.get(token_daily_stats, 'created_at')
            if not type(created_at) is datetime:
                created_at = datetime.fromisoformat(created_at)
            updated_at = pydash.get(token_daily_stats, 'updated_at')
            if not type(updated_at) is datetime:
                updated_at = datetime.fromisoformat(updated_at)

            trading_vol_24h = pydash.get(token_daily_stats, 'trading_vol_24h')
            high_price_24h = pydash.get(token_daily_stats, 'high_price_24h')
            low_price_24h = pydash.get(token_daily_stats, 'low_price_24h')
            circulating_supply = pydash.get(token_daily_stats, 'circulating_supply')
            facebook_likes = pydash.get(token_daily_stats, 'facebook_likes')
            fdv_to_tvl_ratio = pydash.get(token_daily_stats, 'fdv_to_tvl_ratio')
            fully_diluted_valuation = pydash.get(token_daily_stats, 'fully_diluted_valuation')
            market_cap = pydash.get(token_daily_stats, 'market_cap')
            market_cap_rank = pydash.get(token_daily_stats, 'market_cap_rank')
            max_supply = pydash.get(token_daily_stats, 'max_supply')
            mcap_to_tvl_ratio = pydash.get(token_daily_stats, 'mcap_to_tvl_ratio')
            price = pydash.get(token_daily_stats, 'price')
            reddit_accounts_active_48h = pydash.get(token_daily_stats, 'reddit_accounts_active_48h')
            reddit_average_comments_48h = pydash.get(token_daily_stats, 'reddit_average_comments_48h')
            reddit_average_posts_48h = pydash.get(token_daily_stats, 'reddit_average_posts_48h')
            reddit_subscribers = pydash.get(token_daily_stats, 'reddit_subscribers')
            telegram_channel_user_count = pydash.get(token_daily_stats, 'telegram_channel_user_count')
            total_supply = pydash.get(token_daily_stats, 'total_supply')
            total_value_locked = pydash.get(token_daily_stats, 'total_value_locked')
            twitter_followers = pydash.get(token_daily_stats, 'twitter_followers')

            update = {
                "day": day,
                "trading_vol_24h": None if len(str(trading_vol_24h)) == 0 else trading_vol_24h,
                "high_price_24h": None if len(str(high_price_24h)) == 0 else high_price_24h,
                "low_price_24h": None if len(str(low_price_24h)) == 0 else low_price_24h,
                "circulating_supply": None if len(str(circulating_supply)) == 0 else circulating_supply,
                "facebook_likes": None if len(str(facebook_likes)) == 0 else facebook_likes,
                "fdv_to_tvl_ratio": None if len(str(fdv_to_tvl_ratio)) == 0 else fdv_to_tvl_ratio,
                "fully_diluted_valuation": None if len(str(fully_diluted_valuation)) == 0 else fully_diluted_valuation,
                "market_cap": None if len(str(market_cap)) == 0 else market_cap,
                "market_cap_rank": None if len(str(market_cap_rank)) == 0 else market_cap_rank,
                "max_supply": None if len(str(max_supply)) == 0 else max_supply,
                "mcap_to_tvl_ratio": None if len(str(mcap_to_tvl_ratio)) == 0 else mcap_to_tvl_ratio,
                "price": None if len(str(price)) == 0 else price,
                "reddit_accounts_active_48h": None if len(
                    str(reddit_accounts_active_48h)) == 0 else reddit_accounts_active_48h,
                "reddit_average_comments_48h": None if len(
                    str(reddit_average_comments_48h)) == 0 else reddit_average_comments_48h,
                "reddit_average_posts_48h": None if len(
                    str(reddit_average_posts_48h)) == 0 else reddit_average_posts_48h,
                "reddit_subscribers": None if len(str(reddit_subscribers)) == 0 else reddit_subscribers,
                "telegram_channel_user_count": None if len(
                    str(telegram_channel_user_count)) == 0 else telegram_channel_user_count,
                "total_supply": None if len(str(total_supply)) == 0 else total_supply,
                "total_value_locked": None if len(str(total_value_locked)) == 0 else total_value_locked,
                "twitter_followers": None if len(str(twitter_followers)) == 0 else twitter_followers,
                "created_at": created_at,
                "updated_at": updated_at
            }

            TokenDailyStats.update_one(query={"_id": id}, set_dict=update, upsert=True)


def fix_token_daily_stats():
    fix_null_valid_token_daily_stats()
    fix_duplicate_token_daily_stats()
    fix_missing_token_daily_stats_coin_gecko_ids()


def fix_null_valid_token_daily_stats():
    query = {
        "trading_vol_24h": None,
        "market_cap": None,
        "price": None
    }
    update = {
        "row_status": "disable"
    }
    result = TokenDailyStats.update_many(query=query, set_dict=update)


def fix_duplicate_token_daily_stats():
    result = TokenDailyStats.aggregate([
        {
            "$match": {
                "row_status": {"$ne": "disable"}
            }
        },
        {
            "$group": {
                "_id": {
                    "coin_gecko_id": "$coin_gecko_id",
                    "day": "$day"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "count": {"$gt": 1}
            }
        }
    ])
    daily_check_stats = list(result)

    print(len(daily_check_stats))

    for daily_check_stats in daily_check_stats:
        coin_gecko_id = pydash.get(daily_check_stats, '_id.coin_gecko_id')
        if not coin_gecko_id:
            continue
        day = pydash.get(daily_check_stats, '_id.day')
        daily_query = {
            "coin_gecko_id": coin_gecko_id,
            "day": day,
            "row_status": {"$ne": "disable"}
        }
        first_result = TokenDailyStats.find_one(query=daily_query)
        first_result_id = pydash.get(first_result, '_id')
        fix_query = {
            "_id": {"$ne": first_result_id},
            "coin_gecko_id": coin_gecko_id,
            "day": day
        }
        fix_update = {
            "row_status": "disable"
        }

        print(fix_query, fix_update)
        TokenDailyStats.update_many(query=fix_query, set_dict=fix_update)


def fix_missing_token_daily_stats(coin_gecko_id, fix_key):
    coin_gecko_id = str(coin_gecko_id)
    start_check_time = get_start_check_time(coin_gecko_id, fix_key)
    token_daily_stats_repair_data(coin_gecko_id, fix_key, start_check_time)


def token_daily_stats_repair_data(coin_gecko_id, fix_key, start_check_time):
    start_query = {
        "row_status": {"$ne": "disable"},
        "coin_gecko_id": coin_gecko_id,
        "day": {"$gte": start_check_time},
        fix_key: {"$ne": None}
    }
    print(start_query)
    end_query = {
        "row_status": {"$ne": "disable"},
        "coin_gecko_id": coin_gecko_id,
        "day": {"$gte": start_check_time}
    }

    earliest_token_daily_statss = list(
        TokenDailyStats.find(start_query, {'day': 1, fix_key: 1}).sort('day', 1).limit(1))
    earliest_token_daily_stats_day = pydash.get(earliest_token_daily_statss, '0.day')
    if not earliest_token_daily_stats_day:
        return
    exists_latest_daily_statss = list(TokenDailyStats.find(end_query, {'day': 1, fix_key: 1}).sort('day', -1).limit(1))
    exists_latest_token_daily_stats_day = pydash.get(exists_latest_daily_statss, '0.day')
    latest_token_daily_stats_day = DateUtil.utc_start_of_date()
    diff_days = DateUtil.days_diff(earliest_token_daily_stats_day, latest_token_daily_stats_day)
    for i in range(0, diff_days):
        print("day =====>>", i)
        execution_date = DateUtil.utc_x_hours_after(24 * i, earliest_token_daily_stats_day)
        handle_token_daily_stats_repair_data(coin_gecko_id, execution_date, fix_key)


def handle_token_daily_stats_repair_data(coin_gecko_id, execution_date, fix_key):
    token_info = Token.find_one(query={"coin_gecko_id": coin_gecko_id})
    symbol = pydash.get(token_info, 'symbol')
    address = pydash.get(token_info, 'token')

    query = {
        "row_status": {"$ne": "disable"},
        "coin_gecko_id": coin_gecko_id,
        "day": execution_date,
        fix_key: {"$ne": None}
    }
    execution_date_token_stats = TokenDailyStats.find_one(query=query)

    if not execution_date_token_stats:
        first_date = DateUtil.utc_x_hours_ago(24 * 1, execution_date)
        second_date = DateUtil.utc_x_hours_ago(24 * 2, execution_date)
        arr_dates = [first_date, second_date]
        date_query = {
            "row_status": {"$ne": "disable"},
            'day': {'$in': arr_dates},
            'coin_gecko_id': coin_gecko_id,
            fix_key: {"$ne": None}
        }
        dates_results = TokenDailyStats.find_list(query=date_query)
        value_arr_result = []
        for dates_result in dates_results:
            fix_value = pydash.get(dates_result, fix_key)
            value_arr_result.append(fix_value)
        if len(value_arr_result) == 0:
            print('************************** not find data', coin_gecko_id)
            return
        avg_fix_value = pydash.sum_(value_arr_result) / len(value_arr_result)
        if pydash.includes(['circulating_supply', 'total_supply', 'max_supply'], fix_key):
            fix_value = avg_fix_value
        else:
            fix_value = avg_fix_value + avg_fix_value * 0.01
        forge_key = 'forge_{key}'.format(key=fix_key)
        new_data_query = {
            "row_status": {"$ne": "disable"},
            "coin_gecko_id": coin_gecko_id,
            "day": execution_date
        }
        update = {
            forge_key: True,
            'coin_gecko_id': coin_gecko_id,
            'day': execution_date,
            'symbol': symbol,
            'address': address,
            fix_key: fix_value,
            'updated_at': DateUtil.utc_current(),
            'created_at': DateUtil.utc_current()
        }

        print(update)
        TokenDailyStats.update_one(query=new_data_query, set_dict=update, upsert=True)


def get_coin_gecko_ids():
    coin_gecko_ids = TokenDailyStats.distinct('coin_gecko_id')
    return coin_gecko_ids


def get_start_check_time(coin_gecko_id, fix_key):
    token_info_query = {
        "coin_gecko_id": coin_gecko_id
    }
    token_info = Token.find_one(query=token_info_query)

    check_time = pydash.get(token_info, 'check_time')
    if not check_time:
        query = {
            "row_status": {"$ne": "disable"},
            "coin_gecko_id": coin_gecko_id,
            fix_key: {"$ne": None},
            "day": {"$gte": valid_date}
        }
        print(query)

        token_daily_stats = list(TokenDailyStats.find(query, {'day': 1}).sort('day', 1).limit(1))
        check_time = pydash.get(token_daily_stats, '0.day')
    return check_time


def fix_missing_token_daily_stats_coin_gecko_ids():
    coin_gecko_ids = get_coin_gecko_ids()
    group_coin_gecko_id_items = np.array_split(coin_gecko_ids, 64)

    Parallel(n_jobs=64)(delayed(fix_missing_token_daily_stats_all_keys_by_group_coin_gecko_id_item)(item) for item in tqdm(group_coin_gecko_id_items))


def fix_missing_token_daily_stats_all_keys_by_group_coin_gecko_id_item(items):
    for item in items:
        keys = [
            "price",
            "total_supply",
            "max_supply",
            "circulating_supply",
            "high_price_24h",
            "low_price_24h",
            "fully_diluted_valuation",
            "market_cap",
            "trading_vol_24h",
            "fdv_to_tvl_ratio"
        ]
        for key in keys:
            fix_missing_token_daily_stats(item, key)
        Token.update_one(query={'coin_gecko_id': item}, set_dict={'restore_status': 'done'})


def token_info_add_mark_check_time(coin_gecko_id, check_time):
    query = {
        "coin_gecko_id": coin_gecko_id
    }
    TokenDailyStats.update_one(query=query, set_dict={"check_time": check_time})


def token_daily_stats_upload_and_import():
    query = {
        'day': {'$gte': DateUtil.utc_start_of_date(datetime.strptime('2021-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))},
        'row_status': {'$ne': 'disable'}
    }
    dates = TokenDailyStats.distinct('day', query)

    for date in dates:
        handle_write_data_to_csv(date)
        handle_upload_csv_to_gsc(date)
        print(date)

    handle_import_gsc_csv_to_bigquery()


def handle_write_data_to_csv(execution_date):
    values = []
    token_daily_datas = TokenDailyStats.find({'day': execution_date, 'row_status': {'$ne': 'disable'}})
    for token_daily in token_daily_datas:
        symbol = pydash.get(token_daily, 'symbol')
        address = pydash.get(token_daily, 'address')
        coin_gecko_id = pydash.get(token_daily, 'coin_gecko_id')
        day = pydash.get(token_daily, 'day')
        trading_vol_24h = pydash.get(token_daily, 'trading_vol_24h')
        high_price_24h = pydash.get(token_daily, 'high_price_24h')
        low_price_24h = pydash.get(token_daily, 'low_price_24h')
        circulating_supply = pydash.get(token_daily, 'circulating_supply')
        facebook_likes = pydash.get(token_daily, 'facebook_likes')
        fdv_to_tvl_ratio = pydash.get(token_daily, 'fdv_to_tvl_ratio')
        fully_diluted_valuation = pydash.get(token_daily, 'fully_diluted_valuation')
        market_cap = pydash.get(token_daily, 'market_cap')
        market_cap_rank = pydash.get(token_daily, 'market_cap_rank')
        max_supply = pydash.get(token_daily, 'max_supply')
        mcap_to_tvl_ratio = pydash.get(token_daily, 'mcap_to_tvl_ratio')
        price = pydash.get(token_daily, 'price')
        reddit_accounts_active_48h = pydash.get(token_daily, 'reddit_accounts_active_48h')
        reddit_average_comments_48h = pydash.get(token_daily, 'reddit_average_comments_48h')
        reddit_average_posts_48h = pydash.get(token_daily, 'reddit_average_posts_48h')
        reddit_subscribers = pydash.get(token_daily, 'reddit_subscribers')
        telegram_channel_user_count = pydash.get(token_daily, 'telegram_channel_user_count')
        total_supply = pydash.get(token_daily, 'total_supply')
        total_value_locked = pydash.get(token_daily, 'total_value_locked')
        twitter_followers = pydash.get(token_daily, 'twitter_followers')
        created_at = pydash.get(token_daily, 'created_at')
        updated_at = pydash.get(token_daily, 'updated_at')
        values.append({
            'symbol': symbol,
            'address': address,
            'coin_gecko_id': coin_gecko_id,
            'day': day,
            'trading_vol_24h': trading_vol_24h,
            'high_price_24h': high_price_24h,
            'low_price_24h': low_price_24h,
            'circulating_supply': circulating_supply,
            'facebook_likes': facebook_likes,
            'fdv_to_tvl_ratio': fdv_to_tvl_ratio,
            'fully_diluted_valuation': fully_diluted_valuation,
            'market_cap': market_cap,
            'market_cap_rank': market_cap_rank,
            'max_supply': max_supply,
            'mcap_to_tvl_ratio': mcap_to_tvl_ratio,
            'price': price,
            'reddit_accounts_active_48h': reddit_accounts_active_48h,
            'reddit_average_comments_48h': reddit_average_comments_48h,
            'reddit_average_posts_48h': reddit_average_posts_48h,
            'reddit_subscribers': reddit_subscribers,
            'telegram_channel_user_count': telegram_channel_user_count,
            'total_supply': total_supply,
            'total_value_locked': total_value_locked,
            'twitter_followers': twitter_followers,
            'updated_at': updated_at,
            'created_at': created_at
        })
    df = pandas.DataFrame(values, columns=columns)
    csv_file = get_token_daily_stats_csv_name(datetime.strftime(execution_date, '%Y-%m-%d'))
    df.to_csv(csv_file, index=False, header=True)


def get_token_daily_stats_csv_name(execution_date):
    dags_folder = project_config.dags_folder
    return os.path.join(dags_folder, 'token_stats/csv/{}_{}.csv'.format('token_daily_stats', execution_date))


def handle_upload_csv_to_gsc(execution_date):
    execution_date = datetime.strftime(execution_date, '%Y-%m-%d')
    source_csv_file = get_token_daily_stats_csv_name(execution_date)
    destination_file_path = '{name}/date={execution_date}/{name}_{execution_date}.csv'.format(name='token_daily_stats',
                                                                                              execution_date=execution_date)

    upload_csv_to_gsc(source_csv_file, destination_file_path)
    print(source_csv_file)
    print(destination_file_path)


def handle_import_gsc_csv_to_bigquery():
    import_gsc_to_bigquery(name='token_daily_stats')


def handle_fix_market_cap_rank():
    dates = list(TokenDailyStats.distinct('day', {}))

    for d in dates:
        all_token_daily_stats = list(TokenDailyStats.find({'day': d, 'row_status': {'$ne': 'disable'}}).sort('market_cap', -1))
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

