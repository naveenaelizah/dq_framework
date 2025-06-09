import schedule
from data_quality.DataQuality import DataQuality
from utils.utils import create_df_from_dq_results,log,show
from data_loader.db_loader import DatabaseManager
from reader.JSONFileReader import JSONFileReader
import pandas as pd
import time
from pathlib import Path
import os
from datetime import datetime

def job():
    print("-------------------------++ Cron Job Started ++---------------------------------------------")
    log("Hoonartek DQ Job Started")
    dq = DataQuality("config/customer_dq_rules/config.json", "config/db_sources/db.json")
    dq_results = dq.run_test()
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"summary_{timestamp}.csv"
    dq_df = create_df_from_dq_results(pd, dq_results,file_name)
    log("Hoonartek DQ Job Completed")
    #file_path = Path("data/output/summary.csv").resolve()
    #print(f"Summary Result: {file_path}")


# Schedule the job
schedule.every(10).seconds.do(job)
# schedule.every().hour.do(job)
# schedule.every().day.at("10:30").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
