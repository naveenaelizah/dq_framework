import pandas as pd
from datetime import datetime
import pytz
import numpy as np
import logging


class dq_kyc_length_check:
    def __init__(self, column_name, meta, add_info):
        self.column_name = column_name
        self.meta_info = meta
        self.add_info = add_info

        # Configure logging to log to both console and file
        self.logger = logging.getLogger("DQKYCNumberLengthCheck")
        self.logger.setLevel(logging.INFO)

        # Check if the logger already has handlers to avoid duplicates
        if not self.logger.handlers:
            # File handler
            current_date = datetime.now().strftime("%Y-%m-%d")
            log_path = "logs"
            log_filename = f"{log_path}/dq_rules_logs_{current_date}.log"
            #print(log_filename)
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                "[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S"
            )
            console_handler.setFormatter(console_formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

        #self.logger.info(f"Initialized dq_kyc_length_check class.")

    def test(self, df):
        try:

            self.logger.info(f"Applying DQ rule [dq_kyc_length_check] on '{self.column_name}' column.")

            # Step 1: Validate inputs
            self.logger.info("Validating inputs.")
            if self.column_name not in df.columns:
                error_message = f"Column '{self.column_name}' does not exist in the DataFrame."
                self.logger.error(error_message)
                raise ValueError(error_message)

            required_keys = ['primary_key', 'source_db', 'table_name']
            if not all(key in self.meta_info for key in required_keys):
                error_message = f"Required keys {required_keys} missing in meta_info."
                self.logger.error(error_message)
                raise ValueError(error_message)

            #self.logger.info(f"Input validation for column '{self.column_name}' completed successfully.")

            # Step 2: Replace "null" and blank strings with NaN
            df.replace(["null", ""], np.nan, inplace=True)

            # Step 3: Create detailed report DataFrame
            detailed_report_df = df.copy()

            # Step 4: Check KYC number length based on prefix
            detailed_report_df.loc[:, "ERROR_DESCRIPTION"] = detailed_report_df[self.column_name].apply(
                lambda kyc_number: (
                    f"{self.column_name} is invalid: for prefix '{kyc_number[:4]}', expected length "
                    f"{12 if kyc_number[:4] == 'IDP6' else 10}, got '{len(str(kyc_number))}'"
                    if pd.notnull(kyc_number) and (
                        (kyc_number[:4] == 'IDP6' and len(str(kyc_number)) != 12) or
                        ((kyc_number[:4] == 'IDP8' or kyc_number[:4] == 'IDP14') and len(str(kyc_number)) != 10)
                    )
                    else None
                )
            )

            # Step 5: Add primary key values for invalid rows
            primary_key = self.meta_info['primary_key']
            if isinstance(primary_key, list):
                detailed_report_df.loc[:, "PRIMARY_KEY_VALUE"] = detailed_report_df.apply(
                    lambda row: " | ".join(str(row[key]) for key in primary_key),
                    axis=1
                )
            else:
                detailed_report_df.loc[:, "PRIMARY_KEY_VALUE"] = detailed_report_df[primary_key].astype(str)

            # Step 6: Add other metadata columns
            detailed_report_df.loc[:, "DQ_NAME"] = "dq_kyc_number_length_check"
            detailed_report_df.loc[:, "INVALID_VALUE"] = detailed_report_df[self.column_name].astype(str)
            detailed_report_df.loc[:, "DQ_COLUMN_NAME"] = self.column_name
            detailed_report_df.loc[:, "SOURCE_SYSTEM"] = self.meta_info['source_db']
            detailed_report_df.loc[:, "TABLE_NAME"] = self.meta_info['table_name']
            detailed_report_df.loc[:, "PRIMARY_KEY"] = self.meta_info['primary_key']
            detailed_report_df.loc[:, "DQ_GENERATION_DATE"] = datetime.now(
                pytz.timezone("Asia/Kolkata")
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Step 7: Filter for invalid rows
            df_result = detailed_report_df[detailed_report_df["ERROR_DESCRIPTION"].notnull()]

            # Log summary for the column
            if not df_result.empty:
                total_errors = len(df_result)
                self.logger.info(
                    f"Input validation completed successfully: Column '{self.column_name}' has {total_errors} rows with invalid KYC numbers."
                )
                print("\n")  # Adding 4 blank lines after completion message
            else:
                self.logger.info(
                    f"Input validation completed successfully: Column '{self.column_name}' has no invalid KYC numbers."
                )
                print("\n")  # Adding 4 blank lines after completion message

            return df_result

        except Exception as e:
            self.logger.exception("An error occurred during the dq_kyc_number_length_check function.")
            raise
