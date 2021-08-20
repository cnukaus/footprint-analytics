from dex.etl_dex_pair_bsc import DexPairBSC
from datetime import datetime
from utils.date_util import DateUtil
from config import project_config


class DexPairEthereum(DexPairBSC):
    network = 'ethereum'
    chain = 'Ethereum'

    def __init__(self, execution_date=None):
        super().__init__(execution_date)

    def data_is_null_sql(self):
        execution_date_str = "'" + self.execution_date_str + "'"
        return """
            SELECT day, token_0_price, token_1_price, trade_count, 'null_value' AS question_type
                    FROM `xed-project-237404.{data_base}.dex_pair_daily_stats`
                    WHERE day = {execution_date}
                        AND chain = 'Ethereum'
                        AND (token_0_price IS NULL or token_1_price IS NULL or trade_count IS NULL)
                    UNION ALL
                    (SELECT day, token_0_price, token_1_price, trade_count, 'less_than_zero' AS question_type
                    FROM `xed-project-237404.{data_base}.dex_pair_daily_stats`
                    WHERE day = {execution_date}
                        AND chain = 'Ethereum'
                        AND (token_0_price < 0 or token_1_price < 0 or trade_count < 0))
        """.format(execution_date=execution_date_str, data_base=project_config.bigquery_etl_database)

    def continuity_sql(self):
        date7ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 7, self.execution_date), '%Y-%m-%d') + "'"
        return """
        SELECT day,
             slug,
             chain,
             question_type
        FROM 
            (SELECT *
            FROM 
                (SELECT day,
                 slug,
                 chain,
                 'missing_data' AS question_type
                FROM 
                    (SELECT DISTINCT day,
                 slug,
                 chain,
                 LEAD(day,
                 1)
                        OVER (PARTITION BY chain, slug
                    ORDER BY  day DESC) AS lastday
                    FROM `xed-project-237404.{data_base}.dex_pair_daily_stats`
                    WHERE day > {date7ago}
                            AND chain = 'Ethereum'
                    ORDER BY  day ASC )
                    WHERE DATE_DIFF(day, lastday, day) >1) m
                    UNION
                    ALL 
                        (SELECT day,
                 slug,
                 chain,
                 'duplication_data' AS question_type,
                        FROM `xed-project-237404.{data_base}.dex_pair_daily_stats`
                        WHERE day > {date7ago}
                                AND chain = 'Ethereum'
                        GROUP BY  day, chain, slug
                        HAVING COUNT(concat(day, '-', slug, '-', chain)) >1
                        ORDER BY  day DESC)
                        ORDER BY  day)
        """.format(date7ago=date7ago, data_base=project_config.bigquery_etl_database)

    def anomaly_sql(self):
        return ''
