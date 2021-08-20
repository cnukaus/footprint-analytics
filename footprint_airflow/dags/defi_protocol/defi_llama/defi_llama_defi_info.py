import pydash
import os
import requests, json
from models import DefiProtocolInfo
import pandas
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
from config import project_config
from utils.slug_transform_util import tranform_slug_from_out

defi_llama_host = 'https://api.llama.fi'

project_name = 'defi_protocol_info'


def handle_batch_update_defi_infos():
    protocols = requests.get(url=defi_llama_host + '/protocols')
    protocol_results = json.loads(protocols.text)
    update_flag = False
    for protocol_result in protocol_results:
        slug = pydash.get(protocol_result, 'slug')
        slug = tranform_slug_from_out(slug)
        chains = pydash.get(protocol_result, 'chains')
        for chain in chains:
            query = {
                'slug': slug,
                'chain': chain
            }
            defi_info = DefiProtocolInfo.find_one(query=query)
            if defi_info is None:
                update_flag = True
                insert_defi_info_to_db(protocol_result, chain)

    if update_flag:
        update_defi_info_data()


def insert_defi_info_to_db(protocol_info, chain):
    name = pydash.get(protocol_info, 'name')
    slug = pydash.get(protocol_info, 'slug')
    slug = tranform_slug_from_out(slug)
    category = pydash.get(protocol_info, 'category')
    cmc_id = pydash.get(protocol_info, 'cmcId')
    coin_gecko_id = pydash.get(protocol_info, 'gecko_id')
    description = pydash.get(protocol_info, 'description')
    logo = pydash.get(protocol_info, 'logo')
    symbol = pydash.get(protocol_info, 'symbol')
    token = pydash.last(pydash.split(pydash.get(protocol_info, 'address'), ':'))
    twitter = pydash.get(protocol_info, 'twitter')
    url = pydash.get(protocol_info, 'url')
    discord = pydash.get(protocol_info, 'discord')
    github = pydash.get(protocol_info, 'github')
    telegram = pydash.get(protocol_info, 'telegram')
    sort_defi_infos = list(DefiProtocolInfo.find().sort('protocol_id', -1).limit(1))
    defi_info = pydash.get(sort_defi_infos, '0.protocol_id')
    protocol_id = defi_info + 1
    query = {
        'slug': slug,
        'chain': chain
    }
    update = {
        'name': name,
        'category': category,
        'cmc_id': cmc_id,
        'coin_gecko_id': coin_gecko_id,
        'description': description,
        'logo': logo,
        'symbol': symbol,
        'token': token,
        'twitter': twitter,
        'url': url,
        'discord': discord,
        'github': github,
        'telegram': telegram,
        'protocol_id': protocol_id
    }
    DefiProtocolInfo.update_one(query=query, set_dict=update, upsert=True)


def update_defi_info_data():
    defi_info_columns = [
        'category',
        'chain',
        'cmc_id',
        'coin_gecko_id',
        'description',
        'discord',
        'github',
        'launched_time',
        'logo',
        'name',
        'protocol_id',
        'slug',
        'symbol',
        'telegram',
        'token',
        'twitter',
        'url'
    ]
    defi_infos = DefiProtocolInfo.find_list(query={})

    csv_results = []
    for defi_info in defi_infos:
        _id = str(pydash.get(defi_info, '_id'))
        category = pydash.get(defi_info, 'category')
        chain = pydash.get(defi_info, 'chain', '')
        cmc_id = pydash.get(defi_info, 'cmc_id', '')
        coin_gecko_id = pydash.get(defi_info, 'coin_gecko_id', '')
        description = pydash.get(defi_info, 'description', '').rstrip() if pydash.get(defi_info, 'description', '') is not None else ''
        discord = pydash.get(defi_info, 'discord', '')
        launched_time = pydash.get(defi_info, 'launched_time', '')
        logo = pydash.get(defi_info, 'logo', '')
        name = pydash.get(defi_info, 'name', '')
        protocol_id = pydash.get(defi_info, 'protocol_id', '')
        slug = pydash.get(defi_info, 'slug', '')
        symbol = pydash.get(defi_info, 'symbol', '')
        telegram = pydash.get(defi_info, 'telegram', '')
        token = pydash.get(defi_info, 'token', '')
        twitter = pydash.get(defi_info, 'twitter', '')
        url = pydash.get(defi_info, 'url', '')

        value = {
            'category': category,
            'chain': chain,
            'cmc_id': cmc_id,
            'coin_gecko_id': coin_gecko_id,
            'description': description,
            'discord': discord,
            'launched_time': launched_time,
            'logo': logo,
            'name': name,
            'protocol_id': protocol_id,
            'slug': slug,
            'symbol': symbol,
            'telegram': telegram,
            'token': token,
            'twitter': twitter,
            'url': url
        }
        csv_results.append(value)

    df = pandas.DataFrame(csv_results, columns=defi_info_columns)
    csv_file = get_defi_info_csv_name()
    df.to_csv(csv_file, index=False, header=True)
    handle_upload_defi_info_csv_to_gsc()
    handle_import_gsc_csv_to_bigquery()


def get_defi_info_csv_name():
    dags_folder = project_config.dags_folder
    return os.path.join(dags_folder, 'defi_protocol/csv/defi_protocol_info.csv')


def handle_upload_defi_info_csv_to_gsc():
    source_csv_file = get_defi_info_csv_name()
    destination_file_path = 'defi_protocol_info/defi_protocol_info.csv'
    upload_csv_to_gsc(source_csv_file, destination_file_path)
    print(source_csv_file)
    print(destination_file_path)


def handle_import_gsc_csv_to_bigquery():
    # custom_data_base = project_config.bigquery_database
    import_gsc_to_bigquery(name=project_name)
