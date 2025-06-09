import sys
import datetime
import pandas as pd
import dask.dataframe as dd
from config.setting import settings
from rich.table import Table

def log(msg):
    now = datetime.datetime.now()
    print(str(now) + "\t" + msg)
    return

def error(msg):
    log("ERROR: " + msg)
    sys.exit(1)
    return

def warning(msg):
    log("WARNING: " + msg)
    return

def load_data(file_path):
    """
    Loads data from a CSV file into a pandas DataFrame.
    """
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        error(f"Failed to load data from {file_path}: {e}")

def create_df_from_dq_results(dask, dq_results, file_name):
    """
    This function creates a Dask DataFrame from dq_results and writes it to a CSV file.
    dq_results is expected to be a list of dictionaries or a pandas DataFrame.
    """
    if isinstance(dq_results, list):
        dq_results = pd.DataFrame(dq_results)
    elif not isinstance(dq_results, pd.DataFrame):
        raise TypeError("dq_results must be a list of dictionaries or a pandas DataFrame.")

    dq_df = dd.from_pandas(dq_results, npartitions=3)

    output_csv = f"{settings['output_path']}/{file_name}"
    dq_df.to_csv(output_csv, index=False, single_file=True)

    return dq_df

def create_df_from_bad_dq_results(pandas, dq_result, file_name):
    """
    This function creates a pandas DataFrame from bad dq_results and writes it to a CSV file.
    """
    dq_df = pandas.DataFrame(dq_result)
    output_csv = f"{settings['output_path']}/{file_name}"
    dq_df.to_csv(output_csv, index=False)
    return []

def show(df):
    """
    This function generates a rich table and displays the data from a DataFrame.
    """
    table = Table(title="Summary")

    table.add_column("Table Name", justify="right", style="cyan", no_wrap=True)
    table.add_column("Column Name", justify="center", style="magenta")
    table.add_column("DQ rule Name", justify="left", style="green")
    table.add_column("Dimension", justify="left", style="green")
    table.add_column("Total Scanned Records", justify="left", style="green")
    table.add_column("Failed Records", justify="left", style="green")
    table.add_column("Failed Records Percentage", justify="left", style="green")
    table.add_column("Passed Records Percentage", justify="left", style="green")

    records = df.to_dict(orient='records')
    for row in records:
        table.add_row(
            row["Table Name"], row["Column Name"], row["DQ rule Name"],
            row["Dimension"], row["Total Scanned Records"], row["Failed Records"],
            row["Failed Records Percentage"], row["Passed Records Percentage"]
        )

    print(table)

def clean_records(df, validation_result, file_name):
    """
    Placeholder for any specific logic to clean records before saving.
    """
    return ""

