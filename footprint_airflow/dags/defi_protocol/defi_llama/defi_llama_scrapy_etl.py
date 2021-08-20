import pydash
import requests, json, math
from models import DefiProtocolInfo
from defi_protocol.defi_llama.defi_llama_protocol_tvl_etl import DefiLlamaProtocolTvlETL
from defi_protocol.defi_llama.defi_llama_protocol_lending_etl import DefiLlamaProtocolLendingETL
from utils.slug_transform_util import transform_slug_to_out

defi_llama_host = 'https://api.llama.fi'


class DefiLlamaScrapy(object):

    defi_llama_protocol_lending_etl = DefiLlamaProtocolLendingETL()
    defi_llama_protocol_tvl_etl = DefiLlamaProtocolTvlETL()

    def exec(self):
        query = {
            'row_status': {'$ne': 'disable'}
        }
        defi_protocol_info_count = DefiProtocolInfo.count(query=query)
        per_limit = 100
        round_times = math.ceil(defi_protocol_info_count / per_limit)
        for i in range(0, round_times):
            print('i ====', i)
            defi_protocol_infos = DefiProtocolInfo.find_list(query=query, projection={'protocol_id': 1, 'chain': 1, 'name': 1, 'slug': 1, 'category': 1}, limit=per_limit, skip=(i * per_limit))
            for defi_protocol_info in defi_protocol_infos:
                print(defi_protocol_info['name'])
                self.handle_defi_protocol_info(defi_protocol_info)

        self.defi_llama_protocol_lending_etl.defi_llama_lending_upload_and_import()
        self.defi_llama_protocol_tvl_etl.defi_llama_tvl_upload_and_import()
        self.etl_valid()
        self.etl_end()

    def handle_defi_protocol_info(self, defi_protocol_info):
        protocol_result = self.get_defi_protocol_info(defi_protocol_info)
        self.handle_defi_llama_protocol_tvl(defi_protocol_info, protocol_result)
        self.handle_defi_llama_protocol_lending(defi_protocol_info, protocol_result)

    def get_defi_protocol_info(self, defi_protocol_info):
        protocol_id = pydash.get(defi_protocol_info, 'protocol_id')
        slug = pydash.get(defi_protocol_info, 'slug')
        protocol_result = self.get_protocol_tvl(slug, protocol_id)
        return protocol_result

    def handle_defi_llama_protocol_lending(self, defi_protocol_info, protocol_result):
        category = pydash.get(defi_protocol_info, 'category')
        if category == 'Lending':
            self.defi_llama_protocol_lending_etl.defi_llama_protocol_lending(defi_protocol_info, protocol_result)

    def handle_defi_llama_protocol_tvl(self, defi_protocol_info, protocol_result):
        self.defi_llama_protocol_tvl_etl.defi_llama_protocol_tvl(defi_protocol_info, protocol_result)

    def get_protocol_tvl(self, slug, protocol_id):
        slug = transform_slug_to_out(slug=slug)
        url = defi_llama_host + "/protocol/" + slug
        result = requests.get(url=url)
        if not result:
            return None
        json_result = json.loads(result.text)
        if 'id' in json_result and len(json_result['id']):
            return json_result
        else:
            return None

    def etl_valid(self):
        self.defi_llama_protocol_tvl_etl.do_common_valid()
        self.defi_llama_protocol_lending_etl.do_common_valid()

    def etl_end(self):
        self.defi_llama_protocol_tvl_etl.do_end()
        self.defi_llama_protocol_lending_etl.do_end()
