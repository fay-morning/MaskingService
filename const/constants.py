FILE_TASK = 1
DB_TASK = 2

SAMPLING_RULE_FLAG = 1
MASKING_RULE_FLAG = 2
AGGREGATION_RULE_FLAG = 3
CATEGORIZATION_RULE_FLAG = 4
GENERALIZATION_RULE_FLAG = 5
SUPPRESSION_RULE_FLAG = 6
MASKING_TASK_FLAG = 7
KA_TASK_FLAG = 8
TPL_FLAG = 9
KTPL_FLAG = 10
KTPL_TEMP_FLAG = 11

AVAILABLE_FILE_TYPE = ['txt', 'csv', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx']  # 全小写
AVAILABLE_DB_TYPE = ['mysql']  # 全小写
DEFAULT_AGGREGATION_RULE = '3_aggregation_default'
USER_UPLOAD_SERVER = 'useruploadserver'

THRESHOLD = 0.05
DOWN_PATH = "./data"
COL_RULE = {'1': str, '2': str, '3': float, '4': float, '5': str, '6': str}
DOWNLOAD_PATH = "/home/hit/MaskingFiles"