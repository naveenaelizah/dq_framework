import pandas as pd
import dask.dataframe as dd
import os
# from ydata_profiling import ProfileReport  # Keep commented if not generating profiles

class DataProfiling:
    def __init__(self, output_directory="data/output/"):
        self.output_directory = output_directory
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

    def generate_column_profile(self, df, table_name):
        try:
            # If the input is a Dask DataFrame, convert to Pandas
            if isinstance(df, dd.DataFrame):
                df = df.compute()

            # 🔴 Profiling is currently disabled
            print(f"⚠️ Skipping YData Profiling Report generation for {table_name}")

            # If you want to re-enable, uncomment below:
            # profile = ProfileReport(
            #     df,
            #     title=f"Data Profile Report: {table_name}",
            #     explorative=True,
            #     minimal=True  # Set to False for full report (slower)
            # )
            #
            # output_file = os.path.join(self.output_directory, f"{table_name}_profile.html")
            # profile.to_file(output_file)
            #
            # print(f"✅ YData Profiling Report generated for {table_name}: {output_file}")

        except Exception as e:
            print(f"❌ Error during profiling for {table_name}: {e}")

