import configparser
import os

import pandas as pd
import pydash

from config import project_config

coin_paprika_host = 'https://api.coinpaprika.com'

price_columns = [
    'address',
    'price',
    'minute',
]


def read_tokens():
    cfg = configparser.ConfigParser()
    cfg.read('./prices.ini')
    return cfg


def read_lending_tokens():
    df = pd.read_csv('./lending_tokens.csv')
    tokens = df['address'].tolist()
    return tokens


def write_tokens():
    dags_folder = project_config.dags_folder
    cfg = configparser.ConfigParser()
    cfg_path = os.path.join(dags_folder, 'token_stats/coin_paprika/prices.ini')
    cfg.read(cfg_path)
    return cfg


def gen():
    lending_tokens = read_lending_tokens()
    tokens_cfg = read_tokens()
    tokens = tokens_cfg.sections()
    lending_config = configparser.ConfigParser()
    for name_index in range(len(tokens)):
        item = tokens_cfg[tokens[name_index]]
        address = pydash.get(item, 'address', '')
        if address.lower() in lending_tokens:
            print(address.lower())
            lending_config[tokens[name_index]] = item

    with open('prices_lending.ini', 'w') as configfile:
        lending_config.write(configfile)


if __name__ == '__main__':
    gen()
