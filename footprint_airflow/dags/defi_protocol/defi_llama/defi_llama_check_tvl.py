from models import DefiProtocolInfo, DefiDailyStats
import pydash
from utils.date_util import DateUtil


def defi_llama_check_tvl(slug, chain):
    start_check_time = get_start_check_time(slug, chain)
    if start_check_time is None:
        return
    defi_repair_data(slug, chain, start_check_time)


def defi_repair_data(slug, chain, start_check_time):
    query = {
        'slug': slug,
        'chain': chain,
        'day': {'$gte': start_check_time}
    }
    earliest_defi_daily_statss = list(DefiDailyStats.find(query, {'day': 1, 'tvl': 1}).sort('day', 1).limit(1))
    earliest_defi_daily_stats_day = pydash.get(earliest_defi_daily_statss, '0.day')
    if not earliest_defi_daily_stats_day:
        return
    exists_latest_daily_statss = list(DefiDailyStats.find(query, {'day': 1, 'tvl': 1}).sort('day', -1).limit(1))
    exists_latest_defi_daily_stats_day = pydash.get(exists_latest_daily_statss, '0.day')
    latest_defi_daily_stats_day = DateUtil.utc_start_of_date()
    diff_days = DateUtil.days_diff(earliest_defi_daily_stats_day, latest_defi_daily_stats_day)
    for i in range(0, diff_days + 1):
        execution_date = DateUtil.utc_x_hours_after(24 * i, earliest_defi_daily_stats_day)
        handle_defi_repair_data(slug, chain, execution_date)

    defi_llama_add_mark_check_time(slug, chain, exists_latest_defi_daily_stats_day)


def handle_defi_repair_data(slug, chain, execution_date):
    defi_protocol_info = DefiProtocolInfo.find_one(query={'slug': slug, 'chain': chain})
    query = {
        'slug': slug,
        'chain': chain,
        'day': execution_date
    }
    defi_protocol_info_name = pydash.get(defi_protocol_info, 'name')
    defi_protocol_id = pydash.get(defi_protocol_info, 'protocol_id')
    execution_date_defi_stats = DefiDailyStats.find_one(query=query)
    if not execution_date_defi_stats:
        first_date = DateUtil.utc_x_hours_ago(24 * 1, execution_date)
        second_date = DateUtil.utc_x_hours_ago(24 * 2, execution_date)
        arr_dates = [first_date, second_date]
        date_query = {
            'day': {'$in': arr_dates},
            'slug': slug,
            'chain': chain
        }
        dates_results = DefiDailyStats.find_list(query=date_query)
        tvl_arr_result = []
        for dates_result in dates_results:
            tvl = pydash.get(dates_result, 'tvl')
            tvl_arr_result.append(tvl)
        if len(tvl_arr_result) == 0:
            print('************************** not find data', slug, chain)
            return
        avg_tvl = pydash.sum_(tvl_arr_result) / len(tvl_arr_result)
        tvl = avg_tvl + avg_tvl * 0.01
        update = {
            'forge': True,
            'chain': chain,
            'day': execution_date,
            'name': defi_protocol_info_name,
            'protocol_id': defi_protocol_id,
            'slug': slug,
            'tvl': tvl,
            'updated_at': DateUtil.utc_current(),
            'created_at': DateUtil.utc_current()
        }
        DefiDailyStats.update_one(query=query, set_dict=update, upsert=True)


def defi_end_check_time():
    return DateUtil.utc_x_hours_ago(24)


def get_start_check_time(slug, chain):
    query = {
        'slug': slug,
        'chain': chain
    }
    defi_protocol_info = DefiProtocolInfo.find_one(query=query)
    check_time = pydash.get(defi_protocol_info, 'check_time')
    if check_time is None:
        defi_daily_stats = list(DefiDailyStats.find(query, {'day': 1}).sort('day', 1).limit(1))
        check_time = pydash.get(defi_daily_stats, '0.day')

    return check_time


def defi_llama_add_mark_check_time(slug, chain, check_time):
    query = {
        'slug': slug,
        'chain': chain
    }
    DefiProtocolInfo.update_one(query=query, set_dict={'check_time': check_time})
