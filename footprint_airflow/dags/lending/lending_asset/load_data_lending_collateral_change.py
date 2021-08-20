import os
from config import project_config
from utils.common import read_file
from lending.lending_asset.loan_data_lending_basic import LoanDataLendingBasic


class LendingCollateralChange(LoanDataLendingBasic):
    columns = None
    task_name = 'lending_collateral_change'
    table_name = 'lending_collateral_change'

    task_airflow_execution_time = '0 2 * * *'

    def build_query_sql(self):
        dags_folder = project_config.dags_folder
        sql_path = os.path.join(dags_folder, 'lending/lending_asset/lending_collateral_change.sql')
        sql = read_file(sql_path)
        sql = sql.replace('{{date_filter}}', self.date_filter)
        print('lending collateral change query_string', sql)
        return sql

    def anomaly_sql(self):
        return ''

