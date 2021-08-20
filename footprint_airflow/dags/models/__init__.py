from models.bigquery_check_point import BigQueryCheckPointModel
from models.dex_token_price import DexTokenPriceModel
from models.token_daily_stats import TokenDailyStatsModel
from models.token import TokenModel
from models.defi_protocol_info import DefiProtocolInfoModel
from models.defi_daily_stats import DefiDailyStatsModel
from models.request_log import RequestLogModel
from models.defi_llama_lending_daily_stats import DefiLamaLendingDailyStatsModel
from models.coin_gecko_token_market_daily_source import CoinGeckoTokenMarketDailySourceModel
from models.monitor_dashboard import MonitorDashBoardModel
from models.covalent_dex_pair_source import CovalentDexPairSourceModel
from models.covalent_lending_source import CovalentLendingSourceModel

BigQueryCheckPoint = BigQueryCheckPointModel()
DexTokenPriceModel = DexTokenPriceModel()
TokenDailyStats = TokenDailyStatsModel()
Token = TokenModel()
DefiProtocolInfo = DefiProtocolInfoModel()
DefiDailyStats = DefiDailyStatsModel()
RequestLog = RequestLogModel()
DefiLlamaLendingDailyStats = DefiLamaLendingDailyStatsModel()
CoinGeckoTokenMarketDailySource = CoinGeckoTokenMarketDailySourceModel()
MonitorDashBoard = MonitorDashBoardModel()
CovalentDexPairSource = CovalentDexPairSourceModel()
CovalentLendingSource = CovalentLendingSourceModel()
