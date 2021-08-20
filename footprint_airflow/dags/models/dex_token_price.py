from models.model_helper import ModelHelper


class DexTokenPriceModel(ModelHelper):
    collection_name = 'dex_token_price'

    def save_prices(self, prices: dict, date_time: str):
        self.remove({'date_time': date_time}, multi=True)
        dict_list = []
        for key, value in prices.items():
            dict_list.append({
                'token': key,
                'price': value,
                'date_time': date_time,
            })
        self.insert_many(dict_list)
        return True

    def load_prices(self, date_time: str):
        prices = {}
        tokens = self.find({'date_time': date_time})
        for t in tokens:
            prices[t['token']] = t['price']
        return prices
