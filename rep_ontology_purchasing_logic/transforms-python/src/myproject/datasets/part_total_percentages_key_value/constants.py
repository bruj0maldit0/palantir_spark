## CONFIGURATION VALUES

#### BUMP THIS TO REFRESH ALL INCREMENTALS. NOTE THAT YOU WILL LOSE OLD HISTORY, SO BACKFILL MAY BE NECESSARY.
SEMANTIC_VERSION = 34

PRICE_ORIG_RAW_MIN = 0
PRICE_ORIG_RAW_MAX = 10000

## CONFIGURATION VALUES END

# Mandatory fields from input data
PURCHASE_REFERENCE_KEY = 'purchase_reference_key'
TYPE = 'type'
MANDATORY_COLUMNS_PART_PROPERTIES = [PURCHASE_REFERENCE_KEY, TYPE]

# We prefix every column that comes from the excel upload with this, to be able
# to easily identify it and prevent clashes with other sources.
PART_PROP_COLUMN_PREFIX = "part_prop_"

# Constants for identifying the run
FILE_MODIFIED_DATE = 'file_modified_date'
FILE = 'file'
PART_PROP_FILE_MODIFIED_DATE = PART_PROP_COLUMN_PREFIX + FILE_MODIFIED_DATE
PART_PROP_FILE_MODIFIED_DATE_STR = PART_PROP_FILE_MODIFIED_DATE + "_str"
PART_PROP_FILE = PART_PROP_COLUMN_PREFIX + FILE
PRIMARY_KEY = 'primary_key'
RUN_TIMESTAMP = 'run_timestamp'

# Fields from transactions (PUR02)
YEAR_MONTH = "year_month"
YEAR = "year"
MONTH = "month"
SUM_NET_RECEIVED_QTY_BUY_UNIT = 'sum_net_received_qty_buy_unit'
CORRECTED_SEGMENT_TEXT = 'segment_ppma'
DIVISION_TEXT = 'division_text'
BAU = 'bau'
GENERIC_SUPPLIER_MDM_TEXT = 'generic_supplier_mdm_text'
INCOTERM_KEY = 'incoterm_key'
IS_MANDATED_YN_KEY = 'is_mandated_yn_key'
DATE = 'date'
TRANSACTION_DATE = 'transaction_date'
IS_SPARE_PART = 'is_spare_part'
NET_WEIGHT_G = PART_PROP_COLUMN_PREFIX + 'net_weight_g'
PO_BUYER_TEXT = 'po_buyer_key'
PO_STATUS = 'po_status'
PRICE_ORIG_RAW = "last_month_price_eur_buy_unit"
LAST_MONTH_PRICE_BUY_UNIT = "last_month_price_buy_unit"
LAST_MONTH_PRICE_CURRENCY_KEY = "last_month_price_currency_key"
MONTH_SPENDS_EUR = 'month_spends_eur'
PERIOD_SPENDS_EUR = 'period_spends_eur'
FIRST_MONTH_OF_RECEPTION = "first_month_of_reception"
LAST_MONTH_OF_RECEPTION = "last_month_of_reception_key"
LAST_MONTH_PRICE_PO_CURRBUY_UNIT = "last_month_price_po_currbuy_unit"
STD_PRICE_CURINV_UNIT_2 = "std_price_std_price_curinv_unit_2"
PURCHASE_ORDER_NUMBER_KEY = 'purchase_order_number_key'
PURCHASE_ORDER_NUMBER_KEY_EKKO_FORMAT = 'purchase_order_number_key_ekko_format'
PRODUCTION_COUNTRY_REGION_KEY = 'production_country_region_key'
SEGMENT_NAME = 'segment_name'
STEERING_UNIT_KEY = 'steering_unit_key'
STEERING_UNIT_TEXT = 'steering_unit_text'
SU_COUNTRY_TEXT = 'su_country_text'
VOLUME_TRANSACTION_RAW = "net_received_qty_buy_unit"
TOTAL_PERIOD_VOLUME_TRANSACTION = "total_period_volume_transaction"
STD_PRICE_CURRENCY_KEY = "std_price_currency_key"
STD_PRICE_EUR_BUY_UNIT = "std_price_eur_buy_unit"
STD_PRICE_EUR = "std_price_eur"
STD_ERP_PRICE_IN_PLANT_CURRENCY = "std_erp_price_in_plant_currency"
PAYMENT_TERMS = "payment_terms"
SEGMENT_CODE = "segment"
BG = "bg"
PURCHASING_ORGANISATION = "purchasing_organisation"
PLANT_SPECIFIC_MATERIAL_STATUS = "plant_specific_material_status"
SOURCE_SYSTEM = "source_system"
FUNCTION_KEY = "function_key"
IS_INTERNAL_YN_KEY = "is_internal"

VOLUME_AGG_COLS = [PURCHASE_REFERENCE_KEY, STEERING_UNIT_KEY, INCOTERM_KEY, PRICE_ORIG_RAW, PO_BUYER_TEXT]
SPEND_AND_PRICE_AGG_COLS = [PURCHASE_REFERENCE_KEY, STEERING_UNIT_KEY, INCOTERM_KEY, PO_BUYER_TEXT]

TRANSACTION_ID_COLS = [PURCHASE_REFERENCE_KEY, PURCHASE_ORDER_NUMBER_KEY, STEERING_UNIT_KEY, INCOTERM_KEY, PRICE_ORIG_RAW, TOTAL_PERIOD_VOLUME_TRANSACTION, PO_BUYER_TEXT, DATE]

# Other fields and constants
COMMODITY = 'commodity'
SUBCOMMODITY = 'subcommodity'
AVERAGE = 'Average'
BEST_PRICE = 'Best price'
COMPONENT_TYPE = 'component_type'
DELTA = 'delta'
DELTA_TO_MEAN = 'delta_to_mean'
DIVISION = 'division'
ERP_SUPPLIER_NEW = 'erp_supplier_new'
ERP_SUPPLIER_TEXT = 'erp_supplier_text'
ERP_SUPPLIER_KEY = 'erp_supplier_key'
EXPENSIVE = 'Expensive'
FEATURE = 'feature'
GENERIC_SUPPLIER = 'generic_supplier'
GENERIC_SUPPLIER_MDM_KEY = 'generic_supplier_mdm_key'
SUPPLIER_SITE_MDM_KEY = 'supplier_site_mdm_key'
ULTIMATE_SUPPLIER_MDM_KEY = 'ultimate_supplier_mdm_key'
ULTIMATE_SUPPLIER_MDM_TEXT = 'ultimate_supplier_mdm_text'
HAS_COMPOSITION = 'has_composition'
INCOTERM_GROUP = 'incoterm_group'
INCOTERM_KEY = 'incoterm_key'
INCOTERMS_1 = "incoterms_1"
INCOTERMS_2 = "incoterms_2"
INCOTERMS_3 = "incoterms_3"
INCOTERMS_1_CODES = ['DAP', 'DDP', 'DDU']
INCOTERMS_2_CODES = ['EXW', 'FCA']
MISSING_VALUES = 'missing_values'
PART = 'part'
PART_ICT = 'part_ict'
PART_7_DIGITS_ID = 'part_7_digits_id'
PART_7_DIGITS_ID_ICT = 'part_7_digits_id_ict'
PART_SUBSTANCE = 'part_substance'
PART_TYPE_GROUP = 'part_type_group'
PLANT_ID = 'plant_id'
POTENTIAL_SAVINGS = 'potential_savings'
POTENTIAL_SAVINGS_TRANSACTION = 'potential_savings_transaction'
PRICE_ASSESSMENT = 'price_assessment'
PRICE_EUR = 'price_eur'
PRICE_EUR_MAX = 'price_eur_max'
PRICE_EUR_MEAN = 'price_eur_mean'
PRICE_EUR_MIN = 'price_eur_min'
PRICE_ORIG = "price_orig"
PRICE_ORIG_MEAN = "price_orig_mean"
PRICE_PRED = "price_pred"
REL_DELTA = 'rel_delta'
SEGMENT = 'segment'
SPEND_TRANSACTION = 'spend_transaction'
TRANSACTION_ID = 'transaction_id'
TRANSACTION_FILE_ID = 'transaction_file_id'
TYPE_GROUP = 'type_group'
VOLUME = 'volume'
WEIGHT = 'weight'
PRODUCTION_COUNTRY = 'production_country_text'
SAP_VENDOR = "sap_vendor"

# Numerical constants
# Accept this % of NaNs in any given column of training dataset
ACCEPT_X_NAN_FRAC = 0.50
# Sensitivity test delta for numerical variables
SENSITIVITY_TEST_DELTA = 0.1
# Size of train and test datasets
TRAIN_SIZE_FRAC = 0.7
TEST_SIZE_FRAC = 0.3

##### model_sensitivity_test_result returned columns (not exhaustive)
SENSITIVITY_FALSE = "sensitivity_false"
SENSITIVITY_TRUE = "sensitivity_true"
SENSITIVITY_YES = "sensitivity_yes"
SENSITIVITY_NO = "sensitivity_no"
DELTA_NUM = "delta_num"
DELTA_RAND = "delta_rand"
DELTA_RANK1 = "delta_rank1"
DELTA_RANK2 = "delta_rank2"
DELTA_YES = "delta_yes"
DELTA_NO = "delta_no"
DELTA_MIN = "delta_min"
DELTA_MAX = "delta_max"
IS_BINARY = 'is_binary'

##### prediction_mean_importances returned columns (not exhaustive)
IMPORTANCE = "importance"
CHECK_SUM = "check_sum"

#### part_predictions returned columns (not exhaustive)
VOLUME_TRANSACTION = "volume_transaction"
PRICE_ORIG_HIGH = "price_orig_high"
PRICE_ORIG_AVERAGE = "price_orig_average"
PRICE_ORIG_LOW = "price_orig_low"
PRICE_PRED_HIGH = "price_pred_high"
PRICE_PRED_AVERAGE = "price_pred_average"
PRICE_PRED_LOW = "price_pred_low"
OPPORTUNITIES = "opportunities"
RISKS = "risks"

#### aggregated predictions tables
TOTAL_MONTH_SPENDS_EUR = 'total_month_spends_eur'
TOTAL_SPENDS = 'total_spends'
TOTAL_VOLUME = 'total_volume'
MIN_PRICE_ORIG = 'min_price_orig'
MEAN_PRICE_ORIG = 'mean_price_orig'
MAX_PRICE_ORIG = 'max_price_orig'
MIN_PRICE_PRED = 'min_price_pred'
MEAN_PRICE_PRED = 'mean_price_pred'
MAX_PRICE_PRED = 'max_price_pred'
TRANSACTION_DATES = 'dates'

#### errors_historical columns (not exhaustive)
ERROR_MESSAGE = "error_message"
ERROR_SEVERITY = "error_severity"
COLUMN = "column"
ROW = "row"

# Model Performance Quality
GOOD = "Good"
POOR = "Poor"
FAIR = "Fair"
MODEL_PERFORMANCE = "model_performance"
TRAIN_R2_SCORE = "train_r2_score"
TEST_R2_SCORE = "test_r2_score"
TRAIN_MSE_SCORE = "train_mse_score"
TEST_MSE_SCORE = "test_mse_score"
