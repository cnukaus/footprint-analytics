from basic.etl_basic import ETLBasic
from utils.date_util import DateUtil
import pydash
import pandas
from datetime import datetime
from models import DefiDailyStats
from config import project_config
from defi_protocol.defi_llama.defi_llama_check_tvl import defi_llama_check_tvl
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
import os


class DefiLlamaProtocolTvlETL(ETLBasic):

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

    valid_null_fields = [
        'tvl'
    ]
    valid_less_than_zero_fields = [
        'tvl'
    ]

    table_name = 'defi_daily_stats'

    task_name = 'defi_daily_stats'

    def __init__(self):
        super().__init__(DateUtil.utc_start_of_date(DateUtil.utc_current()))

    def defi_llama_protocol_tvl(self, defi_protocol_info, protocol_result):
        self.do_start()

        chain = defi_protocol_info['chain']
        slug = defi_protocol_info['slug']

        if protocol_result is None:
            return

        seven_days_ago_timestamp = DateUtil.utc_start_of_date(DateUtil.utc_x_hours_ago(7 * 24)).timestamp()
        if protocol_result['chainTvls'] and chain in protocol_result['chainTvls'] and len(protocol_result['chainTvls'][chain]):
            tvl_arrays = protocol_result['chainTvls'][chain]['tvl']
            tvl_arrays = pydash.filter_(tvl_arrays, lambda _tvl: _tvl['date'] >= seven_days_ago_timestamp)
            for tvl in tvl_arrays:
                self.save_tvl_by_date(tvl, defi_protocol_info)
        elif len(protocol_result['chains']) == 1 and pydash.get(protocol_result, 'chains.0') == chain and protocol_result['tvl']:
            tvls = protocol_result['tvl']
            tvls = pydash.filter_(tvls, lambda _tvl: _tvl['date'] >= seven_days_ago_timestamp)
            for tvl in tvls:
                self.save_tvl_by_date(tvl, defi_protocol_info)
        defi_llama_check_tvl(slug, chain)

    def save_tvl_by_date(self, tvl, defi_protocol_info):
        protocol_id = pydash.get(defi_protocol_info, 'protocol_id')
        chain = pydash.get(defi_protocol_info, 'chain')
        name = pydash.get(defi_protocol_info, 'name')
        slug = pydash.get(defi_protocol_info, 'slug')
        day = DateUtil.utc_start_of_date(datetime.utcfromtimestamp(tvl['date']))
        today_tvl = tvl['totalLiquidityUSD']
        if today_tvl == 0:
            return
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

    def defi_llama_tvl_upload_and_import(self):
        seven_days_ago = DateUtil.utc_start_of_date(DateUtil.utc_x_hours_ago(7 * 24))
        query = {
            'row_status': {'$ne': 'disable'},
            'day': {'$gte': seven_days_ago}
        }
        dates = DefiDailyStats.distinct('day', query)

        for date in dates:
            self.handle_write_data_to_csv(date)
            self.handle_upload_csv_to_gsc(date)

        self.handle_import_gsc_csv_to_bigquery()

    def handle_write_data_to_csv(self, execution_date):
        values = []
        defi_info_daily_datas = DefiDailyStats.find({'day': execution_date})
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
        df = pandas.DataFrame(values, columns=self.columns)
        csv_file = self.get_defi_daily_stats_csv_name(datetime.strftime(execution_date, '%Y-%m-%d'))
        df.to_csv(csv_file, index=False, header=True)

    def data_is_null_sql(self):
        date7ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 7, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
                SELECT day, chain, slug, tvl, 'null_value' AS question_type
                    FROM `xed-project-237404.{data_base}.defi_daily_stats`
                    WHERE day >= {date7ago}
                        AND tvl IS NULL
                    UNION ALL
                    (SELECT day, chain, slug, tvl, 'less_than_zero' AS question_type
                    FROM `xed-project-237404.{data_base}.defi_daily_stats`
                    WHERE day >= {date7ago}
                        AND tvl < 0)
            """.format(date7ago=date7ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_core_field_exists_null_check(self, core_field_null_result):
        print('defi_daily_stats, check the data. Null data appears in the core field data in recent 7 days')

    def handle_core_field_exists_less_than_zero_check(self, core_field_less_zero_result):
        print('defi_daily_stats, check the data. The core field data in recent 7 days is not allowed to be less than 0')

    def anomaly_sql(self):
        date14ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 14, self.execution_date), '%Y-%m-%d') + "'"
        query_str = """
                WITH daily AS 
                    (SELECT day,
                         chain,
                         slug,
                         SUM(tvl) AS tvl,
                    FROM `xed-project-237404.{data_base}.defi_daily_stats`
                    WHERE day > {date14ago}
                    GROUP BY day, chain, slug),
                avgforxd AS 
                    (SELECT day,
                         chain,
                         slug,
                         tvl,

                        (SELECT AVG(tvl)
                        FROM daily AS daily2
                        WHERE daily2.day < daily1.day
                                AND daily2.day > DATE_SUB(daily1.day, INTERVAL 3 DAY)
                                AND daily1.chain = daily2.chain
                                AND daily1.slug = daily2.slug) AS tvl_past_xd
                        FROM daily daily1)
                    SELECT day, chain, slug, concat(tvl, '') as tvl, concat(tvl_past_xd, '') as tvl_past_xd
                FROM avgforxd d
                WHERE d.tvl > d.tvl_past_xd * 2
                        OR d.tvl < d.tvl_past_xd * 0.5
                ORDER BY d.day 
            """.format(date14ago=date14ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_exists_anomaly_check(self, anomaly_result):
        print('defi_daily_stats check the data, and the core field data of the day has a field with large fluctuation')

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
                        lead(day,
                        1)
                            OVER ( partition by chain, slug
                        ORDER BY day desc) AS lastday
                        FROM `xed-project-237404.{data_base}.defi_daily_stats`
                        WHERE day > {date7ago}
                        ORDER BY  day ASC )
                        WHERE date_diff(day, lastday, day) >1 )m
                    UNION
                all 
                    (SELECT day,
                        chain,
                        slug,
                        day AS day_time,
                        'duplication_data' AS question_type,
                    FROM `xed-project-237404.{data_base}.defi_daily_stats`
                    WHERE day > {date7ago}
                    GROUP BY day, chain, slug
                    HAVING count(CONCAT(chain, "_",slug)) >1
                    ORDER BY  day DESC )
                ORDER BY  day
        """.format(date7ago=date7ago, data_base=project_config.bigquery_etl_database)
        return query_str

    def handle_exists_missing_data_check(self, missing_data_result):
        print('defi_daily_stats 检查数据，近7天数据出现缺失数据')

    def handle_exists_duplicate_check(self, duplicate_data_result):
        print('defi_daily_stats 检查数据，近7天数据出现重复数据')

    # 输出的csv 文件
    def get_defi_daily_stats_csv_name(self, execution_date):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, 'defi_protocol/csv/{}_{}.csv'.format(self.task_name, execution_date))

    # 上传数据到gsc
    def handle_upload_csv_to_gsc(self, execution_date):
        execution_date = datetime.strftime(execution_date, '%Y-%m-%d')
        source_csv_file = self.get_defi_daily_stats_csv_name(execution_date)
        destination_file_path = '{name}/date={execution_date}/{name}_{execution_date}.csv'.format(name=self.task_name,
                                                                                                  execution_date=execution_date)

        upload_csv_to_gsc(source_csv_file, destination_file_path)
        print(source_csv_file)
        print(destination_file_path)

    # gsc 数据导入bigquery
    def handle_import_gsc_csv_to_bigquery(self):
        import_gsc_to_bigquery(name=self.task_name)
