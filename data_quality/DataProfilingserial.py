import pandas as pd
from sqlalchemy.orm.persistence import delete_obj
from data_quality.DataProfiling import DataProfiling
from reader.JSONFileReader import JSONFileReader
from utils.utils import create_df_from_bad_dq_results, log, warning, error
from modules.rules.predefined_rule import predefined_rules
from data_loader.db_loader import DatabaseManager
from modules.validators import dq_null_values_check, dq_mobile_length_check
import time
from rich.progress import Progress
from data_loader.csv_loader import load_data
from config.setting import settings
from rich.console import Console
from datetime import datetime
import logging
import psutil
import os

logger = logging.getLogger("dq_null_values_check psic")
console = Console()

class DataQuality:
    def __init__(self, config_path, db_config, dataprofile, output_directory="data/output/"):
        self.config_path = config_path
        self.db_config = db_config
        self.data_profiling_check = dataprofile
        self.data_profiling = DataProfiling(output_directory)
        self.output_directory = output_directory
        self.resource_usage_file = f"{output_directory}resource_usage.csv"
        # Initialize resource usage CSV with headers if it doesn't exist
        if not os.path.exists(self.resource_usage_file):
            pd.DataFrame(columns=['Timestamp', 'CPU_Usage_Percent', 'RAM_Usage_MB']).to_csv(
                self.resource_usage_file, index=False
            )

    def rule_mapping(self, dq_rule):
        for rule in predefined_rules:
            if rule["rule_name"] == dq_rule:
                return rule["class"]

    def _get_expectation(self):
        class_obj = globals()[self.rule_mapping()]
        return class_obj(self.extractor_args)

    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()

    def connect_db(self, db_name):
        db = DatabaseManager(self.db_config)
        return db.get_connection(db_name)

    def log_system_usage(self, timestamp):
        """Log CPU and RAM usage with timestamp to console and CSV"""
        cpu_usage = psutil.cpu_percent(interval=1)
        ram_usage_mb = psutil.virtual_memory().used / (1024 * 1024)  # Convert bytes to MB
        
        # Print to console
        console.print(f"[{timestamp}] CPU Usage: {cpu_usage:.2f}%, RAM Usage: {ram_usage_mb:.2f} MB")
        
        # Append to CSV
        resource_data = pd.DataFrame({
            'Timestamp': [timestamp],
            'CPU_Usage_Percent': [cpu_usage],
            'RAM_Usage_MB': [ram_usage_mb]
        })
        resource_data.to_csv(self.resource_usage_file, mode='a', header=False, index=False)

    def read_data_from_db(self, rule_config):
        summary_result = []
        db = self.connect_db(rule_config["database"])
        
        for config in rule_config["tables"]:
            validation_result = []
            select = ",".join(config["dq_column"])
            start_time = datetime.now()
            self.log_system_usage(start_time.strftime("%Y-%m-%d_%H-%M-%S"))
            
            ge_df = db.query_data(f"SELECT {select} FROM {config['table_name']}")
            
            print("\n**************************************************************")
            console.print(f"Reading the data from table name: [bold]{config['table_name']}[/bold]")
            print("***************************************************************")

            if self.data_profiling_check == "yes":
                self.data_profiling.generate_column_profile(ge_df, config["table_name"])
            else:
                print("skipping data profiling")

            expected_configuration = {
                "source_db": config['data_source'],
                "table_name": config['table_name'],
                "primary_key": config['primary_key']
            }

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            for column in config["columns"]:
                if not column["dq_rule(s)"]:
                    continue

                console.print(f"\n--------------------------------------------------------------", style="bold")
                console.print(f"Performing DQ check for column name: {column['column_name']}")
                console.print("--------------------------------------------------------------", style="bold")

                for dq_rule in column["dq_rule(s)"]:
                    check_time = datetime.now()
                    self.log_system_usage(check_time.strftime("%Y-%m-%d_%H-%M-%S"))
                    
                    expectation_instance = self.rule_mapping(dq_rule["rule_name"])(
                        column["column_name"],
                        expected_configuration,
                        dq_rule["add_info"]
                    )

                    df_result = expectation_instance.test(ge_df)
                    if not df_result.empty:
                        df_result['failed_rule'] = dq_rule["rule_name"]
                        df_result['rule_description'] = dq_rule.get("description", "No description provided")
                        validation_result.append(df_result)

                    total_percentage = (len(df_result) / len(ge_df)) * 100
                    good_percentage = float(100 - total_percentage)
                    summary_result.append({
                        "Source Name": config['data_source'],
                        "Table Name": config["table_name"],
                        "Column Name": column["column_name"],
                        "DQ rule Name": dq_rule["rule_name"],
                        "Dimension": dq_rule["rule_dimension"],
                        "Total Scanned Records": str(len(ge_df)),
                        "Failed Records": str(len(df_result)),
                        "Failed Records Percentage": f"{total_percentage:.2f}%",
                        "Passed Records Percentage": f"{good_percentage:.2f}%",
                    })

            # Process results
            if validation_result:
                failed_df = pd.concat(validation_result)
                failed_ids = failed_df[config['primary_key']].unique()
                clean_df = ge_df[~ge_df[config['primary_key']].isin(failed_ids)]
                
                clean_file = f"{self.output_directory}{config['table_name']}_clean_{timestamp}.csv"
                failed_file = f"{self.output_directory}{config['table_name']}_failed_{timestamp}.csv"
                
                clean_df.to_csv(clean_file, index=False)
                failed_df.to_csv(failed_file, index=False)
                
                logger.info(f"Created clean records file: {clean_file}")
                logger.info(f"Created failed records file: {failed_file}")
            else:
                clean_file = f"{self.output_directory}{config['table_name']}_clean_{timestamp}.csv"
                ge_df.to_csv(clean_file, index=False)
                logger.info(f"Created clean records file (all records passed): {clean_file}")
            
            end_time = datetime.now()
            self.log_system_usage(end_time.strftime("%Y-%m-%d_%H-%M-%S"))

        return summary_result

    def read_data_from_csv(self, rule_config):
        summary_result = []
        with Progress() as progress:
            for config in rule_config["files"]:
                validation_result = []
                select = ",".join(config["dq_column"])
                file_path = f"{settings['input_data_path']}{config['file_name']}"
                
                start_time = datetime.now()
                self.log_system_usage(start_time.strftime("%Y-%m-%d_%H-%M-%S"))
                
                ge_df = load_data(file_path)
                
                print("\n**************************************************************")
                print(f"Reading the data from file name: {config['file_name']}")
                print("***************************************************************")

                if self.data_profiling_check == "yes":
                    self.data_profiling.generate_column_profile(ge_df, config['file_name'])
                else:
                    print("skipping data profiling")

                expected_configuration = {
                    "source_db": config['data_source'],
                    "table_name": config['file_name'],
                    "primary_key": config['primary_key']
                }

                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

                for column in config["columns"]:
                    if not column["dq_rule(s)"]:
                        continue

                    print(f"\n--------------------------------------------------------------")
                    print(f"Performing DQ check for column name: {column['column_name']}")
                    print("--------------------------------------------------------------")

                    for dq_rule in column["dq_rule(s)"]:
                        check_time = datetime.now()
                        self.log_system_usage(check_time.strftime("%Y-%m-%d_%H-%M-%S"))
                        
                        expectation_instance = self.rule_mapping(dq_rule["rule_name"])(
                            column["column_name"],
                            expected_configuration,
                            dq_rule["add_info"]
                        )

                        df_result = expectation_instance.test(ge_df)
                        if not df_result.empty:
                            df_result['failed_rule'] = dq_rule["rule_name"]
                            df_result['rule_description'] = dq_rule.get("description", "No description provided")
                            validation_result.append(df_result)

                        total_percentage = (len(df_result) / len(ge_df)) * 100
                        good_percentage = float(100 - total_percentage)
                        summary_result.append({
                            "Source Name": config['data_source'],
                            "Table Name": config["file_name"],
                            "Column Name": column["column_name"],
                            "DQ rule Name": dq_rule["rule_name"],
                            "Dimension": dq_rule["rule_dimension"],
                            "Total Scanned Records": str(len(ge_df)),
                            "Failed Records": str(len(df_result)),
                            "Failed Records Percentage": f"{total_percentage:.2f}%",
                            "Passed Records Percentage": f"{good_percentage:.2f}%",
                        })

                # Process results
                if validation_result:
                    failed_df = pd.concat(validation_result)
                    failed_ids = failed_df[config['primary_key']].unique()
                    clean_df = ge_df[~ge_df[config['primary_key']].isin(failed_ids)]
                    
                    clean_file = f"{self.output_directory}{config['file_name']}_clean_{timestamp}.csv"
                    failed_file = f"{self.output_directory}{config['file_name']}_failed_{timestamp}.csv"
                    
                    clean_df.to_csv(clean_file, index=False)
                    failed_df.to_csv(failed_file, index=False)
                    
                    logger.info(f"Created clean records file: {clean_file}")
                    logger.info(f"Created failed records file: {failed_file}")
                else:
                    clean_file = f"{self.output_directory}{config['file_name']}_clean_{timestamp}.csv"
                    ge_df.to_csv(clean_file, index=False)
                    logger.info(f"Created clean records file (all records passed): {clean_file}")
                
                end_time = datetime.now()
                self.log_system_usage(end_time.strftime("%Y-%m-%d_%H-%M-%S"))

        return summary_result

    def run_test(self):
        try:
            merged_array = []
            rule_config = self.read_config()
            start_time = datetime.now()
            self.log_system_usage(start_time.strftime("%Y-%m-%d_%H-%M-%S"))
            
            for cnf in rule_config:
                if cnf["type"] == "db":
                    summary_result = self.read_data_from_db(cnf)
                    merged_array.extend(summary_result)
                if cnf["type"] == "csv":
                    summary_result = self.read_data_from_csv(cnf)
                    merged_array.extend(summary_result)
            
            end_time = datetime.now()
            self.log_system_usage(end_time.strftime("%Y-%m-%d_%H-%M-%S"))
            
        except Exception as e:
            print(f"An error occurred in the mapping DQ rule function: {e}")
        return merged_array

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example instantiation
    dq = DataQuality(
        config_path="path/to/config.json",
        db_config="path/to/db_config.json",
        dataprofile="yes",
        output_directory="data/output/"
    )
    
    # Run the tests
    results = dq.run_test()
    
    # Print summary results
    if results:
        print("\nSummary Results:")
        for result in results:
            print(result)
