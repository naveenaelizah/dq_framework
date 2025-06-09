from data_quality.DataQuality import DataQuality
from utils.utils import create_df_from_dq_results, log, show
from data_loader.db_loader import DatabaseManager
from reader.JSONFileReader import JSONFileReader
import pandas as pd
import time
from pathlib import Path
import logging
from rich.console import Console
from rich.table import Table
from datetime import datetime
import json
import psycopg2

console = Console()
logger = logging.getLogger("dq_null_values_check")

def check_dq_status():
    db_json_path = "config/db_sources/db.json"
    print(f"Checking DQ status using DB config: {Path(db_json_path).resolve()}")
    with open(db_json_path, "r") as f:
        db_info = json.load(f)[0]

    conn = psycopg2.connect(
        dbname=db_info['database'],
        user=db_info['username'],
        password=db_info['password'],
        host=db_info['host'],
        port=db_info['port']
    )
    cur = conn.cursor()
    cur.execute("SELECT id, status, created_at FROM dq_rule_config ORDER BY created_at DESC LIMIT 1")
    result = cur.fetchone()
    cur.close()
    conn.close()
    if result:
        dq_id, status, created_at = result
        console.print(f"Latest DQ Config ID: [bold]{dq_id}[/bold], Status: [bold]{status}[/bold], Created At: [bold]{created_at}[/bold]")
        if status.lower() == 'pending':
            console.print("[green]DQ Status is Pending. Starting DQ process...[/green]")
            return dq_id
        else:
            console.print("[yellow]DQ Status is not Pending. Exiting DQ process.[/yellow]")
    else:
        console.print("[red]No records found in dq_rule_config table.[/red]")
    return None

def update_dq_status(dq_id):
    db_json_path = "config/db_sources/db.json"
    print(f"Updating DQ status to Completed using DB config: {Path(db_json_path).resolve()}")
    with open(db_json_path, "r") as f:
        db_info = json.load(f)[0]

    conn = psycopg2.connect(
        dbname=db_info['database'],
        user=db_info['username'],
        password=db_info['password'],
        host=db_info['host'],
        port=db_info['port']
    )
    cur = conn.cursor()
    # Update status to 'Completed' and set created_at to now()
    cur.execute("""
        UPDATE dq_rule_config 
        SET status = 'Completed', created_at = NOW()
        WHERE id = %s
    """, (dq_id,))
    conn.commit()
    cur.close()
    conn.close()
    console.print("[green]DQ Status updated to Completed.[/green]")

def main():
    start_time = time.time()
    print("")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    console.print("              Initializing the [bold]Hoonartek DQ Solution[/bold]           ")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("")

    data_profiling = "yes"
    config_path = "config/customer_dq_rules/config.json"
    db_config_path = "config/db_sources/db.json"

    # Print resolved absolute paths for debugging
    print(f"Using DQ config JSON: {Path(config_path).resolve()}")
    print(f"Using DB config JSON: {Path(db_config_path).resolve()}")

    dq_id = check_dq_status()

    if dq_id:
        dq = DataQuality(config_path, db_config_path, data_profiling)
        try:
            dq_results = dq.run_test()
        except Exception as e:
            console.print(f"[red]Error during DQ test: {e}[/red]")
            return

        time.sleep(0.5)
        update_dq_status(dq_id)

        # Read config to get config_name prefix for file name
        json_reader = JSONFileReader(config_path)
        config_data = json_reader.read()
        config_name_prefix = config_data[0].get("config_name", "default_config")  # Use default if missing

        # Generate summary file name with config_name prefix
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{config_name_prefix}_summary_{timestamp}.csv"

        # Create summary DataFrame
        dq_df = create_df_from_dq_results(pd, dq_results, file_name)

        # Print completion message
        print("")
        console.print(f"[bold green]Data quality check completed successfully[/bold green]")
        print("")
        print("------------------------------------------------------------------------------------------\t\t")
        file_path = Path(f"data/output/{file_name}").resolve()
        print(f"Summary Result: \033]8;;{file_path}\033\\{file_path}\033]8;;\033\\")
        print("------------------------------------------------------------------------------------------\t\t")
        print("")

        # Create rich table to display summary result
        table = Table(title="Data Quality Summary")

        # Add table columns dynamically
        for column in dq_df.columns:
            table.add_column(column, justify="center", style="bold")

        # Add table rows
        for index, row in dq_df.iterrows():
            table.add_row(*[str(value) for value in row])

        # Display table
        console.print(table)

        # Execution time
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution Time: {execution_time:.2f} seconds")

    else:
        console.print("[yellow]DQ check not initiated due to status not being Pending.[/yellow]")


if __name__ == "__main__":
    main()

