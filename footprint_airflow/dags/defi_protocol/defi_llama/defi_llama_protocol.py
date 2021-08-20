import pydash
import os
import requests, json, math
from models import DefiProtocolInfo, DefiDailyStats, RequestLog
from datetime import datetime
import pandas
from utils.date_util import DateUtil
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
from config import project_config
from defi_protocol.defi_llama.defi_llama_check_tvl import defi_llama_check_tvl

project_name = 'defi_daily_stats'

defi_llama_host = 'https://api.llama.fi'

columns = [
    'chain',
    'day',
    'name',
    'protocol_id',
    'slug',
    'tvl',
    'updated_at',
    'created_at'
]


def handle_all_protocol_tvl():
    defi_protocol_info_count = DefiProtocolInfo.count(query={})
    per_limit = 100
    round_times = math.ceil(defi_protocol_info_count / per_limit)
    new_handle_dates = []
    for i in range(0, round_times):
        print('i ====', i)
        defi_protocol_infos = DefiProtocolInfo.find_list(query={}, projection={'protocol_id': 1, 'chain': 1, 'name': 1, 'slug': 1}, limit=per_limit, skip=(i * per_limit))
        for defi_protocol_info in defi_protocol_infos:
            dates = handle_protocol_tvl(defi_protocol_info)
            new_handle_dates = new_handle_dates + dates

    handle_write_data_to_csv(new_handle_dates)


def handle_write_data_to_csv(dates):
    dates = pydash.uniq(dates)

    for date in dates:
        values = []
        defi_info_daily_datas = DefiDailyStats.find({'day': date})
        for defi_info_daily in defi_info_daily_datas:
            chain = pydash.get(defi_info_daily, 'chain')
            day = pydash.get(defi_info_daily, 'day')
            name = pydash.get(defi_info_daily, 'name')
            protocol_id = pydash.get(defi_info_daily, 'protocol_id')
            slug = pydash.get(defi_info_daily, 'slug')
            tvl = pydash.get(defi_info_daily, 'tvl')
            created_at = pydash.get(defi_info_daily, 'created_at')
            updated_at = pydash.get(defi_info_daily, 'updated_at')
            values.append({
                'chain': chain,
                'day': DateUtil.utc_start_of_date(day),
                'name': name,
                'protocol_id': protocol_id,
                'slug': slug,
                'tvl': tvl,
                'updated_at': updated_at,
                'created_at': created_at
            })
        df = pandas.DataFrame(values, columns=columns)
        csv_file = get_defi_daily_stats_csv_name(datetime.strftime(date, '%Y-%m-%d'))
        df.to_csv(csv_file, index=False, header=True)
        handle_upload_csv_to_gsc(date)


def handle_protocol_tvl(defi_protocol_info):
    protocol_id = pydash.get(defi_protocol_info, 'protocol_id')
    chain = pydash.get(defi_protocol_info, 'chain')
    slug = pydash.get(defi_protocol_info, 'slug')
    protocol_result = get_protocol_tvl(slug, protocol_id)

    dates = []

    if protocol_result is None:
        return dates

    if protocol_result['chainTvls'] and chain in protocol_result['chainTvls'] and len(protocol_result['chainTvls'][chain]):
        ethereum_tvl_arrays = protocol_result['chainTvls'][chain]['tvl']
        for tvl in ethereum_tvl_arrays:
            day = save_tvl_by_date(tvl, defi_protocol_info)
            if not pydash.includes(dates, day):
                dates.append(day)
    elif len(protocol_result['chains']) == 1 and pydash.get(protocol_result, 'chains.0') == chain and protocol_result['tvl']:
        tvls = protocol_result['tvl']
        for tvl in tvls:
            day = save_tvl_by_date(tvl, defi_protocol_info)
            if not pydash.includes(dates, day):
                dates.append(day)

    defi_llama_check_tvl(slug, chain)
    return dates


def save_tvl_by_date(tvl, defi_protocol_info):
    protocol_id = pydash.get(defi_protocol_info, 'protocol_id')
    chain = pydash.get(defi_protocol_info, 'chain')
    name = pydash.get(defi_protocol_info, 'name')
    slug = pydash.get(defi_protocol_info, 'slug')
    day = DateUtil.utc_start_of_date(datetime.utcfromtimestamp(tvl['date']))
    today_tvl = tvl['totalLiquidityUSD']
    update = {
        'forge': False,
        'chain': chain,
        'day': day,
        'name': name,
        'protocol_id': protocol_id,
        'slug': slug,
        'tvl': today_tvl,
        'updated_at': DateUtil.utc_current(),
        'created_at': DateUtil.utc_current()
    }
    query = {"slug": slug, "chain": chain, "day": day}
    DefiDailyStats.update_one(query=query, set_dict=update, upsert=True)
    return day


def get_protocol_tvl(slug, protocol_id):
    url = defi_llama_host + "/protocol/" + slug
    result = requests.get(url=url)
    json_result = json.loads(result.text)
    save_request_log(url, json_result, protocol_id)
    if 'id' in json_result and len(json_result['id']):
        return json_result
    else:
        return None


def save_request_log(url, log, protocol_id):
    data = {
        'status': 'finish',
        'service': 'defi_llama',
        'requestTime': DateUtil.utc_current(),
        'method': 'get',
        'url': url,
        'body': {
            'protocol_id': protocol_id
        },
        'isForm': False,
        'response': log,
        'responseTime': DateUtil.utc_current(),
        'useTime': 1000
    }
    RequestLog.insert_one(data=data)


def get_defi_daily_stats_csv_name(execution_date):
    dags_folder = project_config.dags_folder
    return os.path.join(dags_folder, 'defi_protocol/csv/{}_{}.csv'.format(project_name, execution_date))


def handle_upload_csv_to_gsc(execution_date):
    execution_date = datetime.strftime(execution_date, '%Y-%m-%d')
    source_csv_file = get_defi_daily_stats_csv_name(execution_date)
    destination_file_path = '{name}/date={execution_date}/{name}_{execution_date}.csv'.format(name=project_name,
                                                                                              execution_date=execution_date)

    upload_csv_to_gsc(source_csv_file, destination_file_path)
    print(source_csv_file)
    print(destination_file_path)


def handle_import_gsc_csv_to_bigquery():
    import_gsc_to_bigquery(name=project_name)
