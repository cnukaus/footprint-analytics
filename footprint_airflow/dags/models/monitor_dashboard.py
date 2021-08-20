from models.model_helper import ModelHelper
from config import project_config


class MonitorDashBoardModel(ModelHelper):
    """monitor_dashboard"""
    collection_name = 'monitor_dashboard'

    db_name = 'footprint_monitor'

    db_uri = project_config.mongodb_footprint_monitor_uri

