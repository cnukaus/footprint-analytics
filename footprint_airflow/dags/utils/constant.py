

PROXY_LOG_STATUS = {
    "PENDING": 'pending',
    "ERROR": "err",
    "FINISH": "finish"
}

DASH_BOARD_RULE_NAME = {
    'TASK_EXECUTION': 'task_execution',
    'FIELD_LEGAL_NULL': 'field_legal_null',
    'FIELD_LEGAL_LESS_THAN_ZERO': 'field_legal_less_than_zero',
    'FIELD_ANOMALY': 'field_anomaly',
    'FIELD_CONTINUITY': 'data_continuity'
}
DASH_BOARD_RULE_NAME_DESC_CN = {
    'TASK_EXECUTION': 'Task effectiveness',
    'FIELD_LEGAL_NULL': 'Data null validation',
    'FIELD_LEGAL_LESS_THAN_ZERO': 'Data less than 0 validation',
    'FIELD_ANOMALY': 'Data volatility',
    'FIELD_CONTINUITY': 'Data continuity'
}

DASH_BOARD_RESULT_CODE = {
    'EXCEPTION': 1,  # data success
    'REGULAR': 0 # data exception
}