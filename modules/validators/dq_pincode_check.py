import pandas as pd
from datetime import datetime
import pytz
import numpy as np
import logging
import sys
import re

class dq_pincode_check:
    def __init__(self, column, meta, add_info):
        self.column_name = column
        self.meta_info = meta
        self.add_info = add_info

        # Logger setup
        self.logger = logging.getLogger("dq_pincode_check")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            log_path = "logs"
            log_filename = f"{log_path}/dq_rules_logs_{datetime.now().strftime('%Y-%m-%d')}.log"

            file_handler = logging.FileHandler(log_filename)
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s"))

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def test(self, df):
        try:
            self.logger.info(f"Applying DQ rule [dq_pincode_check] on column '{self.column_name}'.")

            if self.column_name not in df.columns:
                raise ValueError(f"Column '{self.column_name}' does not exist in the DataFrame.")

            # Convert to string and clean whitespaces
            df[self.column_name] = df[self.column_name].apply(
                lambda x: str(x).strip() if pd.notnull(x) else x
            )

            # Regex: Indian pincodes are exactly 6 digits and start from 1-9
            pincode_regex = re.compile(r"^[1-9][0-9]{5}$")

            def is_invalid_pincode(pincode):
                if pd.isnull(pincode) or pincode.lower() in ["", "null", "none"]:
                    return False  # null check handled separately
                return not pincode_regex.fullmatch(str(pincode))

            df_result = df[df[self.column_name].apply(is_invalid_pincode)].copy()

            # Add DQ metadata
            df_result["ERROR_DESCRIPTION"] = f"{self.column_name} is not a valid 6-digit Indian pincode."
            df_result["PRIMARY_KEY_VALUE"] = df_result[self.meta_info["primary_key"]].astype(str)
            df_result["DQ_NAME"] = "dq_pincode_check"
            df_result["INVALID_VALUE"] = df_result[self.column_name].astype(str)
            df_result["DQ_COLUMN_NAME"] = self.column_name
            df_result["SOURCE_SYSTEM"] = self.meta_info["source_db"]
            df_result["TABLE_NAME"] = self.meta_info["table_name"]
            df_result["PRIMARY_KEY"] = self.meta_info["primary_key"]
            df_result["DQ_GENERATION_DATE"] = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%dT%H:%M:%SZ")

            self.logger.info(f"Found {len(df_result)} rows with invalid pincodes in column '{self.column_name}'.")

            return df_result

        except Exception as e:
            self.logger.exception("An error occurred in dq_pincode_check.")
            sys.exit(1)

