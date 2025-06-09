from modules.validators.dq_null_values_check import dq_null_values_check
from modules.validators.dq_mobile_length_check import dq_mobile_length_check
from modules.validators.dq_dob_age_check import dq_dob_age_check
from modules.validators.dq_customer_age_check import dq_customer_age_check
from modules.validators.dq_pattern_check import dq_pattern_check
from modules.validators.dq_invalid_date_check import dq_invalid_date_check
from modules.validators.dq_pincode_check import dq_pincode_check

predefined_rules = [
    {"rule_name": "dq_null_values_check", "description": "Check for null values", "class": dq_null_values_check},
    {"rule_name": "dq_mobile_length_check", "description": "Check values for mobile length", "class": dq_mobile_length_check},
    {"rule_name": "dq_dob_age_check", "description": "Check age value is less than 16","class": dq_dob_age_check},
    {"rule_name": "dq_customer_age_check", "description": "Check age value expected a two-digit number", "class": dq_customer_age_check},
    {"rule_name": "dq_pattern_check","description": "Check validation of pattern given by user", "class": dq_pattern_check},
    {"rule_name": "dq_invalid_date_check","description": "Check validation of date format", "class": dq_invalid_date_check},
    {"rule_name": "dq_pincode_check","description": "Check validation of date format", "class": dq_pincode_check},

]
