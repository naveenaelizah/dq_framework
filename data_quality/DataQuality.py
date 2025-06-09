import pandas as pd
from dask.distributed import Client
import dask.dataframe as dd
from data_quality.DataProfiling import DataProfiling
from reader.JSONFileReader import JSONFileReader
from utils.utils import log, load_data
from modules.rules.predefined_rule import predefined_rules
from data_loader.db_loader import DatabaseManager
from rich.console import Console
from rich.progress import Progress
from datetime import datetime
import logging
import os
import psutil
from config.setting import settings

logger = logging.getLogger("dq_logger")
console = Console()

def initialize_dask_client():
    try:
        client = Client(
            n_workers=2,
            threads_per_worker=8,
            memory_limit='4GB',
            processes=True,
            scheduler_port=8788,
            nanny=False
        )
        print(f"Dask Client initialized: {client}")
        return client
    except Exception as e:
        print(f"Failed to initialize Dask Client: {e}")
        return None

class DataQuality:
    def __init__(self, config_path, db_config, dataprofile, output_directory="data/output/"):
        self.config_path = config_path
        self.db_config = db_config
        self.data_profiling_check = dataprofile
        self.data_profiling = DataProfiling(output_directory)
        self.output_directory = output_directory
        os.makedirs(self.output_directory, exist_ok=True)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.resource_log_file = os.path.join(self.output_directory, f"resource_usage_log_{timestamp}.csv")
        with open(self.resource_log_file, 'w') as f:
            f.write("Timestamp,CPU_Usage_Percent,RAM_Usage_MB\n")

    def rule_mapping(self, dq_rule):
        for rule in predefined_rules:
            if rule["rule_name"] == dq_rule:
                return rule["class"]

    def get_rule_description(self, rule_name):
        for rule in predefined_rules:
            if rule["rule_name"] == rule_name:
                return rule.get("description", "No description available")
        return "Rule not found"

    def log_resource_usage(self):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        cpu_usage = psutil.cpu_percent(interval=1)
        ram_usage = psutil.virtual_memory().used / (1024 * 1024)
        log_entry = f"{timestamp},{cpu_usage:.2f},{ram_usage:.2f}\n"
        with open(self.resource_log_file, 'a') as f:
            f.write(log_entry)
        console.print(f"[cyan]Resource Usage at {timestamp}:[/cyan] CPU: {cpu_usage:.2f}%, RAM: {ram_usage:.2f} MB")

    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()

    def connect_db(self, db_name):
        db = DatabaseManager(self.db_config)
        return db.get_connection(db_name)

    def read_data_from_db(self, rule_config):
        summary_result = []
        config_prefix = rule_config.get("config_name", "default_config")
        db = self.connect_db(rule_config["database"])

        for config in rule_config["tables"]:
            self.log_resource_usage()
            select = ",".join(config["dq_column"])
            pandas_df = db.query_data(f"SELECT {select} FROM {config['table_name']}")
            ge_df = dd.from_pandas(pandas_df, npartitions=4)

            console.print(f"Reading data from table: [bold]{config['table_name']}[/bold]")

            if self.data_profiling_check == "yes":
                self.data_profiling.generate_column_profile(ge_df.compute(), config["table_name"])

            expected_configuration = {
                "source_db": config['data_source'],
                "table_name": config['table_name'],
                "primary_key": config['primary_key']
            }

            all_failed_records = []

            for column in config["columns"]:
                console.print(f"Checking column: [bold]{column['column_name']}[/bold]")
                if column["dq_rule(s)"] is None:
                    continue

                for dq_rule in column["dq_rule(s)"]:
                    rule_name = dq_rule["rule_name"]
                    expectation_instance = self.rule_mapping(rule_name)(
                        column["column_name"], expected_configuration, dq_rule["add_info"]
                    )
                    df_result = ge_df.map_partitions(expectation_instance.test).compute()
                    if not df_result.empty:
                        df_result['Failed_DQ_Rule'] = rule_name
                        df_result['DQ_Rule_Description'] = self.get_rule_description(rule_name)
                        all_failed_records.append(df_result)

                    total_percentage = (len(df_result) / len(pandas_df)) * 100
                    good_percentage = float(100 - total_percentage)
                    summary_result.append({
                        "Config Name": config_prefix,
                        "Source Name": config['data_source'],
                        "Table Name": config["table_name"],
                        "Column Name": column["column_name"],
                        "DQ rule Name": rule_name,
                        "Dimension": dq_rule["rule_dimension"],
                        "Total Scanned Records": str(len(pandas_df)),
                        "Failed Records": str(len(df_result)),
                        "Failed Records Percentage": f"{total_percentage:.2f}%",
                        "Passed Records Percentage": f"{good_percentage:.2f}%"
                    })

                    self.log_resource_usage()

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            table_name = config["table_name"]

            if all_failed_records:
                final_failed_records = pd.concat(all_failed_records).drop_duplicates(subset=[config['primary_key']]).reset_index(drop=True)
                failed_keys = final_failed_records[config['primary_key']]
                clean_records = pandas_df[~pandas_df[config['primary_key']].isin(failed_keys)].reset_index(drop=True)
            else:
                final_failed_records = pd.DataFrame()
                clean_records = pandas_df.copy()

            clean_file_name = os.path.join(self.output_directory, f"{config_prefix}_{table_name}_clean_records_{timestamp}.csv")
            failed_file_name = os.path.join(self.output_directory, f"{config_prefix}_{table_name}_failed_records_{timestamp}.csv")

            console.print(f"\u2714 Clean Records File: [green]{clean_file_name}[/green]")
            console.print(f"\u2716 Failed Records File: [red]{failed_file_name}[/red]")

            clean_records.to_csv(clean_file_name, index=False)
            if not final_failed_records.empty:
                final_failed_records.to_csv(failed_file_name, index=False)
            else:
                pd.DataFrame(columns=pandas_df.columns.tolist() + ['Failed_DQ_Rule', 'DQ_Rule_Description']).to_csv(failed_file_name, index=False)

        return summary_result

    def read_data_from_csv(self, rule_config):
        summary_result = []
        config_prefix = rule_config.get("config_name", "default_config")
        with Progress() as progress:
            for config in rule_config["files"]:
                validation_result = []
                file_path = f"{settings['input_data_path']}{config['file_name']}"
                print(f"Reading the data from file name: {config['file_name']}")
                try:
                    ge_df = load_data(file_path)
                    print(f"Data read successfully from file {config['file_name']}")
                except Exception as e:
                    print(f"Error reading data from file {config['file_name']}: {e}")
                    continue

                if self.data_profiling_check == "yes":
                    print(f"Generating data profile for file {config['file_name']}...")
                    self.data_profiling.generate_column_profile(ge_df, config['file_name'])
                else:
                    print("Skipping data profiling")

                expected_configuration = {
                    "source_db": config['data_source'],
                    "table_name": config['file_name'],
                    "primary_key": config['primary_key']
                }

                for column in config["columns"]:
                    print(f"Checking data quality for column {column['column_name']}...")
                    if column["dq_rule(s)"] is None:
                        continue
                    for dq_rule in column["dq_rule(s)"]:
                        expectation_instance = self.rule_mapping(dq_rule["rule_name"])(
                            column["column_name"], expected_configuration, dq_rule["add_info"]
                        )
                        df_result = expectation_instance.test(ge_df)

                        if not df_result.empty:
                            df_result["DQ_Status"] = "Failed"
                            df_result["DQ_Description"] = dq_rule["rule_name"]
                            validation_result.append(df_result)

                        total_percentage = (len(df_result) / len(ge_df)) * 100
                        good_percentage = float(100 - total_percentage)
                        summary_result.append({
                            "Config Name": config_prefix,
                            "Source Name": config['data_source'],
                            "Table Name": config["file_name"],
                            "Column Name": column["column_name"],
                            "DQ rule Name": dq_rule["rule_name"],
                            "Dimension": dq_rule["rule_dimension"],
                            "Total Scanned Records": str(len(ge_df)),
                            "Failed Records": str(len(df_result)),
                            "Failed Records Percentage": f"{total_percentage:.2f}%",
                            "Passed Records Percentage": f"{good_percentage:.2f}%"
                        })

                if validation_result:
                    failed_df = pd.concat(validation_result).drop_duplicates().reset_index(drop=True)
                else:
                    failed_df = pd.DataFrame()

                if not failed_df.empty:
                    clean_df = ge_df[~ge_df.index.isin(failed_df.index)].copy()
                    clean_df["DQ_Status"] = "Passed"
                    clean_df["DQ_Description"] = ""
                else:
                    clean_df = ge_df.copy()
                    clean_df["DQ_Status"] = "Passed"
                    clean_df["DQ_Description"] = ""

                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                failed_file_name = os.path.join(self.output_directory, f"{config_prefix}_{config['file_name']}_failed_records_{timestamp}.csv")
                clean_file_name = os.path.join(self.output_directory, f"{config_prefix}_{config['file_name']}_clean_records_{timestamp}.csv")

                console.print(f"\u2714 Clean Records File: [green]{clean_file_name}[/green]")
                console.print(f"\u2716 Failed Records File: [red]{failed_file_name}[/red]")

                failed_df.to_csv(failed_file_name, index=False)
                clean_df.to_csv(clean_file_name, index=False)

        return summary_result

    def run_test(self):
        try:
            all_results = []
            rule_config = self.read_config()
            for cnf in rule_config:
                self.log_resource_usage()
                if cnf["type"] == "db":
                    all_results.extend(self.read_data_from_db(cnf))
                elif cnf["type"] == "csv":
                    all_results.extend(self.read_data_from_csv(cnf))
                self.log_resource_usage()
            return all_results
        except Exception as e:
            print(f"Error during DQ test: {e}")
            return []

if __name__ == "__main__":
    client = initialize_dask_client()
    if client:
        dq = DataQuality(
            config_path="config/customer_dq_rules/config.json",
            db_config={"some": "db_config"},
            dataprofile="yes"
        )
        result = dq.run_test()
        print(result)
    else:
        print("Dask Client initialization failed. Aborting.")
