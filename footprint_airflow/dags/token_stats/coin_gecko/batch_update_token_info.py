import pydash
from utils.date_util import DateUtil
from models import Token
import time
from pycoingecko import CoinGeckoAPI

cg = CoinGeckoAPI()


def batch_update_token_info():
    coins = get_coins()
    for coin in coins:
        check_coin_token(coin)


def get_coins():
    data = cg.get_coins_list()
    return data


def check_coin_token(coin):
    symbol = pydash.get(coin, 'symbol')
    coin_id = pydash.get(coin, 'id')
    query = {
        'symbol': symbol,
        'coin_gecko_id': coin_id
    }
    token_result = Token.find_one(query=query)
    if token_result is None:
        coin_detail = get_coin_detail(coin_id)
        if coin_detail is not None:
            token_result = handle_update_token_info(coin_detail)
    return token_result


def handle_update_token_info(coin_detail):
    coin_id = pydash.get(coin_detail, 'id')
    token = pydash.get(coin_detail, 'contract_address', '')
    symbol = pydash.get(coin_detail, 'symbol')
    total_supply = pydash.get(coin_detail, 'market_data.total_supply')
    query = {
        'coin_gecko_id': coin_id,
        'symbol': symbol
    }
    update = {
        'token': token,
        'total_supply': total_supply,
        'created_at': DateUtil.utc_current(),
        'updated_at': DateUtil.utc_current()
    }
    token_result = Token.update_one(query=query, set_dict=update, upsert=True)
    return token_result


def get_coin_detail(coin_id):
    time.sleep(1)
    try:
        detail = cg.get_coin_by_id(coin_id)
        return detail
    except Exception as e:
        print(coin_id + 'get coin detail fail', e)
    return None
