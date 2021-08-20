import os
from lending.lending_asset.loan_data_lending_basic import LoanDataLendingBasic
from config import project_config
from utils.common import read_file
from datetime import datetime
from utils.date_util import DateUtil


class LendingBorrow(LoanDataLendingBasic):

    task_airflow_execution_time = '0 2 * * *'
    table_name = 'lending_borrow'
    task_name = 'lending_borrow'
    columns = None

    def build_query_sql(self):
        dags_folder = project_config.dags_folder
        sql_path = os.path.join(dags_folder, 'lending/lending_asset/lending_borrow.sql')
        sql = read_file(sql_path)
        sql = sql.replace('{{date_filter}}', self.date_filter)
        print('lending borrow build_query_sql query_string', sql)
        return sql

    def anomaly_sql(self):
        date15ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 15, self.execution_date), '%Y-%m-%d') + "'"
        return """
        WITH daily AS 
            (SELECT day,
                 project,
                 version,
                 CONCAT(project,
                 "_",
                version) AS id,
                 SUM(token_amount) AS token_amount
            FROM `xed-project-237404.{data_base}.lending_borrow`
            WHERE day > {date15ago}
            GROUP BY  day, project, version ), avgfor3d AS 
            (SELECT day,
                id,
                token_amount,
                 
                (SELECT (sum(token_amount) / 3)
                FROM daily d2
                WHERE d2.day < d1.day
                        AND d2.day >= DATE_SUB(d1.day, INTERVAL 3 DAY)
                        AND d2.id = d1.id ) AS token_amount_past_3d
                FROM daily d1 )
            SELECT *
        FROM avgfor3d d
        WHERE (d.token_amount > d.token_amount_past_3d * 50
                OR d.token_amount < d.token_amount_past_3d * (1/50))
        ORDER BY  d.day
        """.format(date15ago=date15ago, data_base=project_config.bigquery_etl_database)
