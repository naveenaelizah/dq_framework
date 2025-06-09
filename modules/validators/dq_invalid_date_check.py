from datetime import datetime
import pandas as pd
import pytz
import numpy as np
import logging
import sys

class dq_invalid_date_check:
    def __init__(self, column, meta, add_info, expected_format="%Y-%m-%d"):
        self.column_name = column
        self.meta_info = meta
        self.add_info = add_info
        self.expected_format = expected_format  # Enforce strict format

        self.logger = logging.getLogger("dq_invalid_date_check")
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
            self.logger.info(f"Applying DQ rule [dq_invalid_date_check] on '{self.column_name}' with format '{self.expected_format}'.")

            if self.column_name not in df.columns:
                raise ValueError(f"Column '{self.column_name}' does not exist in the DataFrame.")

            # Normalize string values but preserve nulls
            df[self.column_name] = df[self.column_name].apply(
                lambda x: str(x).strip() if pd.notnull(x) else x
            )

            def is_invalid_date(val):
                if pd.isnull(val) or str(val).strip().lower() in ["", "null", "none"]:
                    return False  # Skip nulls (handled by null check rule)
                try:
                    datetime.strptime(val, self.expected_format)
                    return False
                except Exception:
                    return True

            # Filter invalid dates
            df_result = df[df[self.column_name].apply(is_invalid_date)].copy()

            # Add DQ metadata
            df_result["ERROR_DESCRIPTION"] = f"{self.column_name} has invalid date format. Expected format: {self.expected_format}"
            df_result["PRIMARY_KEY_VALUE"] = df_result[self.meta_info["primary_key"]].astype(str)
            df_result["DQ_NAME"] = "dq_invalid_date_check"
            df_result["INVALID_VALUE"] = df_result[self.column_name].astype(str)
            df_result["DQ_COLUMN_NAME"] = self.column_name
            df_result["SOURCE_SYSTEM"] = self.meta_info["source_db"]
            df_result["TABLE_NAME"] = self.meta_info["table_name"]
            df_result["PRIMARY_KEY"] = self.meta_info["primary_key"]
            df_result["DQ_GENERATION_DATE"] = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%dT%H:%M:%SZ")

            self.logger.info(f"Found {len(df_result)} rows with invalid date formats in column '{self.column_name}'.")

            return df_result

        except Exception as e:
            self.logger.exception("An error occurred in dq_invalid_date_check.")
            sys.exit(1)

