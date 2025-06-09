import pandas as pd
from datetime import datetime
import pytz
import numpy as np
import logging
import re

class dq_pattern_check:

    def __init__(self, column, meta, add_info):
        self.column_name = column
        self.meta_info = meta
        self.add_info = add_info

        # Extract pattern from add_info dictionary
        self.pattern_str = self.add_info.get('pattern')
        if not self.pattern_str:
            raise ValueError("Missing 'pattern' in add_info for dq_pattern_check.")

        self.pan_pattern = re.compile(self.pattern_str)

        self.logger = logging.getLogger("dq_pattern_check")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            current_date = datetime.now().strftime("%Y-%m-%d")
            log_path = "logs"
            log_filename = f"{log_path}/dq_rules_logs_{current_date}.log"
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)

            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                "[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S"
            )
            console_handler.setFormatter(console_formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def test(self, df):
        try:
            self.logger.info(f"Applying DQ rule [dq_pattern_check] on '{self.column_name}' column.")

            if self.column_name not in df.columns:
                error_message = f"Column '{self.column_name}' does not exist in the DataFrame."
                self.logger.error(error_message)
                raise ValueError(error_message)

            required_keys = ['primary_key', 'source_db', 'table_name']
            if not all(key in self.meta_info for key in required_keys):
                error_message = f"Required keys {required_keys} missing in meta_info."
                self.logger.error(error_message)
                raise ValueError(error_message)

            df.replace(["null", ""], np.nan, inplace=True)

            detailed_report_df = df.copy()

            detailed_report_df.loc[:, "ERROR_DESCRIPTION"] = detailed_report_df[self.column_name].apply(
                lambda pan: (
                    f"{self.column_name} format is invalid: expected pattern '{self.pattern_str}', got '{pan}'"
                    if pd.isnull(pan) or not bool(self.pan_pattern.match(str(pan).strip().upper()))
                    else None
                )
            )

            detailed_report_df.loc[:, "PRIMARY_KEY_VALUE"] = detailed_report_df.apply(
                lambda row: str(row[self.meta_info['primary_key']])
                if pd.isnull(row[self.column_name]) or not bool(self.pan_pattern.match(str(row[self.column_name]).strip().upper()))
                else None,
                axis=1
            )
            detailed_report_df.loc[:, "DQ_NAME"] = "dq_pattern_check"
            detailed_report_df.loc[:, "INVALID_VALUE"] = detailed_report_df[self.column_name].astype(str)
            detailed_report_df.loc[:, "DQ_COLUMN_NAME"] = self.column_name
            detailed_report_df.loc[:, "SOURCE_SYSTEM"] = self.meta_info['source_db']
            detailed_report_df.loc[:, "TABLE_NAME"] = self.meta_info['table_name']
            detailed_report_df.loc[:, "PRIMARY_KEY"] = self.meta_info['primary_key']
            detailed_report_df.loc[:, "DQ_GENERATION_DATE"] = datetime.now(
                pytz.timezone("Asia/Kolkata")
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            df_result = detailed_report_df[detailed_report_df["ERROR_DESCRIPTION"].notnull()]

            if not df_result.empty:
                total_errors = len(df_result)
                self.logger.info(
                    f"Input validation completed: Column '{self.column_name}' has {total_errors} rows with invalid PAN formats."
                )
                print("\n")
            else:
                self.logger.info(
                    f"Input validation completed: Column '{self.column_name}' has no invalid PAN formats."
                )
                print("\n")

            return df_result

        except Exception as e:
            self.logger.exception("An error occurred during the dq_pattern_check function.")
            raise

