import os

from config.base import BaseConfig
from config.develop import DevelopConfig
from config.test import TestConfig
from config.production import ProductionConfig
from config.restore import RestoreConfig

config_dict = {
    'develop': DevelopConfig,
    'production': ProductionConfig,
    'test': TestConfig,
    'default': BaseConfig,
    'restore': RestoreConfig
}


def get_environment_config(environment: str = None):
    environment = environment or os.environ.get('ENVIRONMENT', 'default')
    return config_dict[environment]


project_config = get_environment_config()
