import os

PROJECT_ID = os.getenv('_PROJECT_ID')
DATASET_MUDATA_RAW = os.getenv('_DATASET_MUDATA_RAW')
DATASET_MUDATA_CURATED = os.getenv('_DATASET_MUDATA_CURATED')
DATASET_MUDATA_AGGREGATED = os.getenv('_DATASET_MUDATA_AGGREGATED')

class TRIGGER_RULES:
    ALL_SUCCESS= 'all_success'
    ALL_FAILED= 'all_failed'
    ALL_DONE= 'all_done'
    ALL_SKIPPED= 'all_skipped'
    ONE_FAILED= 'one_failed'
    ONE_SUCCESS= 'one_success'
    NONE_FAILED= 'none_failed'
    NONE_FAILED_MIN_ONE_SUCCESS= 'none_failed_min_one_success'
    NONE_SKIPPED= 'none_skipped'
    ALWAYS= 'always'

class BUSINESS_ROLES:
    TCC= 'TCC'
    TCP= 'TCP'
    BO= 'BO'
    AP= 'AP'
    AC= 'AC'
    AL= 'AL'