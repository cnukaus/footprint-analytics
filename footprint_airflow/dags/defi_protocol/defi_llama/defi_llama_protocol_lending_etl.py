from basic.etl_basic import ETLBasic
from utils.date_util import DateUtil
import pydash
from datetime import datetime
from models import DefiLlamaLendingDailyStats
import pandas
import os
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from config import project_config


class DefiLlamaProtocolLendingETL(ETLBasic):
    columns = [
        'chain',
        'day',
        'name',
        'protocol_id',
        'slug',
        'asset',
        'total_supply',
        'created_at',
        'updated_at'
    ]
    valid_null_fields = [
        'total_supply'
    ]
    valid_less_than_zero_fields = [
        'total_supply'
    ]
    table_name = 'defi_llama_lending_daily_stats'

    task_name = 'defi_llama_lending_daily_stats'

    def __init__(self):
        super().__init__(DateUtil.utc_start_of_date(DateUtil.utc_current()))

    def defi_llama_protocol_lending(self, defi_protocol_info, protocol_result):
        self.do_start()

        chain = defi_protocol_info['chain']
        slug = defi_protocol_info['slug']

        if protocol_result is None:
            return

        seven_days_ago_timestamp = DateUtil.utc_start_of_date(DateUtil.utc_x_hours_ago(7 * 24)).timestamp()
        if protocol_result and protocol_result['chainTvls'] and chain in protocol_result['chainTvls'] and len(protocol_result['chainTvls'][chain]):
            chain_details = protocol_result['chainTvls'][chain]['tokensInUsd']
            chain_details = pydash.filter_(chain_details, lambda _chain_detail: _chain_detail['date'] >= seven_days_ago_timestamp)
            for chain_detail in chain_details:
                self.save_chain_detail_by_date(chain_detail, defi_protocol_info)


    def save_chain_detail_by_date(self, chain_detail, defi_protocol_info):
        protocol_id = pydash.get(defi_protocol_info, 'protocol_id')
        chain = pydash.get(defi_protocol_info, 'chain')
        name = pydash.get(defi_protocol_info, 'name')
        slug = pydash.get(defi_protocol_info, 'slug')
        execution_date = pydash.get(chain_detail, 'date')

        day = DateUtil.utc_start_of_date(datetime.utcfromtimestamp(execution_date))

        tokens = pydash.get(chain_detail, 'tokens')

        if tokens is None:
            return

        for token in tokens:
            total_supply = tokens[token]
            query = {
                'chain': chain,
                'slug': slug,
                'day': day,
                'asset': token
            }
            update = {
                'forge': False,
                'slug': slug,
                'name': name,
                'total_supply': total_supply,
                'protocol_id': protocol_id,
                'updated_at': DateUtil.utc_current(),
                'created_at': DateUtil.utc_current()
            }
            DefiLlamaLendingDailyStats.update_one(query=query, set_dict=update, upsert=True)

    def defi_llama_lending_upload_and_import(self):
        seven_days_ago = DateUtil.utc_start_of_date(DateUtil.utc_x_hours_ago(7 * 24))

        query = {
            'row_status': {'$ne': 'disable'},
            'day': {'$gte': seven_days_ago}
        }
        dates = DefiLlamaLendingDailyStats.distinct('day', query)
        for date in dates:
            self.handle_write_data_to_csv(date)
            self.handle_upload_csv_to_gsc(date)

        self.handle_import_gsc_csv_to_bigquery()

    def handle_write_data_to_csv(self, execution_date):
        values = []
        defi_llama_lending_daily_stats_datas = DefiLlamaLendingDailyStats.find({'day': execution_date})
        for defi_llama_lending_daily_stats_data in defi_llama_lending_daily_stats_datas:
            chain = pydash.get(defi_llama_lending_daily_stats_data, 'chain')
            day = pydash.get(defi_llama_lending_daily_stats_data, 'day')
            name = pydash.get(defi_llama_lending_daily_stats_data, 'name')
            protocol_id = pydash.get(defi_llama_lending_daily_stats_data, 'protocol_id')
            slug = pydash.get(defi_llama_lending_daily_stats_data, 'slug')
            total_supply = pydash.get(defi_llama_lending_daily_stats_data, 'total_supply')
            asset = pydash.get(defi_llama_lending_daily_stats_data, 'asset')
            created_at = pydash.get(defi_llama_lending_daily_stats_data, 'created_at')
            updated_at = pydash.get(defi_llama_lending_daily_stats_data, 'updated_at')
            values.append({
                'chain': chain,
                'day': DateUtil.utc_start_of_date(day),
                'name': name,
                'protocol_id': protocol_id,
                'slug': slug,
                'total_supply': total_supply,
                'asset': asset,
                'updated_at': updated_at,
                'created_at': created_at
            })
        df = pandas.DataFrame(values, columns=self.columns)
        csv_file = self.get_defi_daily_stats_csv_name(datetime.strftime(execution_date, '%Y-%m-%d'))
        df.to_csv(csv_file, index=False, header=True)

    def data_is_null_sql(self):
        date7ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 7, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
                SELECT day, chain, slug, asset, total_supply, 'null_value' AS question_type
                    FROM `xed-project-237404.{data_base}.defi_llama_lending_daily_stats`
                    WHERE day >= {date7ago}
                        AND total_supply IS NULL
                    UNION ALL
                    (SELECT day, chain, slug, asset, total_supply, 'less_than_zero' AS question_type
                    FROM `xed-project-237404.{data_base}.defi_llama_lending_daily_stats`
                    WHERE day >= {date7ago}
                        AND total_supply < 0)
            """.format(date7ago=date7ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_core_field_exists_null_check(self, core_field_null_result):
        print('lending daily stats check data , there is null data in the check result ')

    def handle_core_field_exists_less_than_zero_check(self, core_field_less_zero_result):
        print('lending daily stats check data, check the data. The core field data in recent 7 days is not allowed to be less than 0')

    def anomaly_sql(self):
        date14ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 14, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
                WITH daily AS 
                    (SELECT day,
                         chain,
                         slug,
                         asset,
                         SUM(total_supply) AS total_supply,

                    FROM `xed-project-237404.{data_base}.defi_llama_lending_daily_stats`
                    WHERE day > {date14ago}
                    GROUP BY day, chain, slug, asset),
                avgforxd AS 
                    (SELECT day,
                         chain,
                         slug,
                         asset,
                         total_supply,

                        (SELECT AVG(total_supply)
                        FROM daily AS daily2
                        WHERE daily2.day < daily1.day
                                AND daily2.day > DATE_SUB(daily1.day, INTERVAL 3 DAY)
                                AND daily1.chain = daily2.chain
                                AND daily1.slug = daily2.slug
                                AND daily1.asset = daily2.asset) AS total_supply_past_xd
                        FROM daily daily1)
                    SELECT concat(total_supply, '') as total_supply , day, chain, slug, asset, concat(total_supply_past_xd, '') as total_supply_past_xd
                FROM avgforxd d
                WHERE d.total_supply > d.total_supply_past_xd * 2
                        OR d.total_supply < d.total_supply_past_xd * 0.5
                ORDER BY d.day 
            """.format(date14ago=date14ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_exists_anomaly_check(self, anomaly_result):
        print('lending daily stats, check the data, and the core field data of the day has a field with large fluctuation')

    def continuity_sql(self):
        date7ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 7, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
            SELECT *
                FROM 
                    (SELECT *,
                        'missing_data' AS question_type
                    FROM 
                        (SELECT DISTINCT day,
                        chain,
                        slug,
                        asset,
                        lead(day,
                        1)
                            OVER ( partition by chain, slug, asset
                        ORDER BY day desc) AS lastday
                        FROM `xed-project-237404.{data_base}.defi_llama_lending_daily_stats`
                        WHERE day > {date7ago}
                        ORDER BY  day ASC )
                        WHERE date_diff(day, lastday, day) >1 )m
                    UNION
                all 
                    (SELECT day,
                        chain,
                        slug,
                        asset,
                        day AS day_time,
                        'duplication_data' AS question_type,
                    FROM `xed-project-237404.{data_base}.defi_llama_lending_daily_stats`
                    WHERE day > {date7ago}
                    GROUP BY day, chain, slug, asset
                    HAVING count(CONCAT(chain, "_",slug, "_",asset)) >1
                    ORDER BY  day DESC )
                ORDER BY  day
        """.format(date7ago=date7ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_exists_missing_data_check(self, missing_data_result):
        print('lending daily stats, check the data, and there is missing data in recent 7 days')

    def handle_exists_duplicate_check(self, duplicate_data_result):
        print('lending daily stats, check the data, and there are duplicate data in the last 7 days')

    def get_defi_daily_stats_csv_name(self, execution_date):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, 'defi_protocol/csv/{}_{}.csv'.format(self.task_name, execution_date))

    def handle_upload_csv_to_gsc(self, execution_date):
        execution_date = datetime.strftime(execution_date, '%Y-%m-%d')
        source_csv_file = self.get_defi_daily_stats_csv_name(execution_date)
        destination_file_path = '{name}/date={execution_date}/{name}_{execution_date}.csv'.format(name=self.task_name,
                                                                                                  execution_date=execution_date)

        upload_csv_to_gsc(source_csv_file, destination_file_path)
        print(source_csv_file)
        print(destination_file_path)

    def handle_import_gsc_csv_to_bigquery(self):
        import_gsc_to_bigquery(name=self.task_name)
