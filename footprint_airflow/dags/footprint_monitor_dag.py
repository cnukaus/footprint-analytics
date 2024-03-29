from models import MonitorDashBoard
from utils.date_util import DateUtil
from utils import Constant
from datetime import datetime, timedelta
from utils.monitor import send_to_slack
import pydash
from utils.build_dag_util import BuildDAG


def python_callable():
    get_task_execution_result()


def get_execution_date():
    return DateUtil.utc_start_of_date(DateUtil.utc_x_hours_ago(24 * 1))


def get_task_execution_result():
    execution_date = get_execution_date()
    regular_query = {
        'stats_date': execution_date,
        'rule_name': Constant.DASH_BOARD_RULE_NAME['TASK_EXECUTION'],
        'result_code': Constant.DASH_BOARD_RESULT_CODE['REGULAR']
    }
    regular_result = MonitorDashBoard.distinct('task_name', regular_query)
    regular_count = len(regular_result)

    exception_query = {
        'stats_date': execution_date,
        'rule_name': Constant.DASH_BOARD_RULE_NAME['TASK_EXECUTION'],
        'result_code': Constant.DASH_BOARD_RESULT_CODE['EXCEPTION']
    }
    exception_result = MonitorDashBoard.distinct('task_name', exception_query)
    exception_count = len(exception_result)

    execution_date_str = datetime.strftime(execution_date, '%Y-%m-%d')

    exception_detail = pydash.join(exception_result, '、')
    print(exception_detail)
    text = f'UTC Time: {execution_date_str} The task execution monitoring result is: the number of tasks executed normally: {regular_count}, Number of abnormal task execution: {exception_count}'

    if exception_count > 0:
        text = text + 'Details of abnormal tasks are as follows\n {exception_detail}'.format(exception_detail=exception_detail)

    send_to_slack(text)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 7, 1)
}

dag_params = {
    "dag_id": "footprint_monitor",
    "catchup": False,
    "schedule_interval": '0 3 * * *',
    "description": "footprint_monitor dag",
    "default_args": default_dag_args,
    "dagrun_timeout": timedelta(hours=1)
}

dag_task_params = [
    {
        "task_id": "send_monitor_dash_board_result",
        "python_callable": python_callable,
        "execution_timeout": timedelta(hours=1)
    }
]


DAG = BuildDAG().build_dag(dag_params=dag_params, dag_task_params=dag_task_params)
