import pandas as pd
from datetime import datetime
import pytz
import numpy as np
import logging
import sys  # Import sys to use sys.exit()

class dq_null_values_check:
    def __init__(self, column, meta, add_info):
        self.column_name = column
        self.meta_info = meta
        self.add_info = add_info

        # Configure logging to log to both console and file
        self.logger = logging.getLogger("dq_null_values_check")
        self.logger.setLevel(logging.INFO)

        # Check if the logger already has handlers to avoid duplicates
        if not self.logger.handlers:
            # File handler
            current_date = datetime.now().strftime("%Y-%m-%d")
            log_path = "logs"
            log_filename = f"{log_path}/dq_rules_logs_{current_date}.log"
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)

            # Console handler (Only log INFO messages to the console, not errors)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # Console formatter
            console_formatter = logging.Formatter(
                "[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S"
            )
            console_handler.setFormatter(console_formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def test(self, df):
        try:
            self.logger.info(f"Applying DQ rule [dq_null_values_check] on '{self.column_name}' column.")

            # Step 1: Validate inputs
            self.logger.info(f"Validating inputs.")
            if self.column_name not in df.columns:
                error_message = f"Column '{self.column_name}' does not exist in the DataFrame."
                self.logger.error(error_message)
                raise ValueError(error_message)

            required_keys = ['primary_key', 'source_db', 'table_name']
            if not all(key in self.meta_info for key in required_keys):
                error_message = f"Required keys {required_keys} missing in meta_info."
                self.logger.error(error_message)
                raise ValueError(error_message)

            # Step 2: Replace "null" and blank strings with NaN
            df.replace(["null", ""], np.nan, inplace=True)

            # Step 3: Create detailed report DataFrame
            detailed_report_df = df.copy()

            # Generate error description and related fields
            detailed_report_df["ERROR_DESCRIPTION"] = detailed_report_df[self.column_name].apply(
                lambda x: f"{self.column_name} has null or blank value." if pd.isnull(x) else None
            )
            detailed_report_df["PRIMARY_KEY_VALUE"] = detailed_report_df.apply(
                lambda row: str(row[self.meta_info['primary_key']])
                if pd.isnull(row[self.column_name]) or str(row[self.column_name]).strip() == "" else None,
                axis=1
            )
            detailed_report_df["DQ_NAME"] = "dq_null_values_check"
            detailed_report_df["INVALID_VALUE"] = detailed_report_df[self.column_name].astype(str)
            detailed_report_df["DQ_COLUMN_NAME"] = self.column_name
            detailed_report_df["SOURCE_SYSTEM"] = self.meta_info['source_db']
            detailed_report_df["TABLE_NAME"] = self.meta_info['table_name']
            detailed_report_df["PRIMARY_KEY"] = self.meta_info['primary_key']
            detailed_report_df["DQ_GENERATION_DATE"] = datetime.now(pytz.timezone("Asia/Kolkata")).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

            # Step 4: Filter results
            df_result = detailed_report_df[detailed_report_df["ERROR_DESCRIPTION"].notnull()]

            # Log summary for the column
            if not df_result.empty:
                total_errors = len(df_result)
                self.logger.info(
                    f"Input validation completed successfully: Column '{self.column_name}' has {total_errors} rows with null or blank values."
                )
            else:
                self.logger.info(
                    f"Input validation completed successfully: Column '{self.column_name}' has no null or blank values."
                )

            # # Export clean records (those without errors) to a CSV
            # clean_records_df = detailed_report_df[detailed_report_df["ERROR_DESCRIPTION"].isnull()]
            # if not clean_records_df.empty:
            #     current_date = datetime.now().strftime("%Y-%m-%d")
            #     clean_records_filename = f"data/processed/lean_records_{self.meta_info['table_name']}_{current_date}.csv"
            #     clean_records_df.to_csv(clean_records_filename, index=False)
            #     self.logger.info(f"Clean records exported to {clean_records_filename}")

            return df_result

        except Exception as e:
            # Log the error but suppress it in the console
            self.logger.exception(f"An error occurred during the dq_null_values_check function.")
            sys.exit(1)  # Exit the program immediately after logging the error

