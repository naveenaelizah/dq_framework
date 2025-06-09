from flask import Flask, jsonify, send_file, request
import os
import pandas as pd
import psycopg2
import glob
from datetime import datetime
from flask_cors import CORS
from data_quality.DataQuality import DataQuality

app = Flask(__name__)
CORS(app)

# Output directory
OUTPUT_DIR = "data/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="shreyas",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        raise Exception(f"Database connection error: {str(e)}")

# Extend DataQuality
class DataQualityAPI(DataQuality):
    def __init__(self, config_path, db_config, dataprofile, output_directory=OUTPUT_DIR):
        super().__init__(config_path, db_config, dataprofile, output_directory)

    def get_latest_file(self, pattern):
        matched_files = glob.glob(pattern)
        if not matched_files:
            return None
        matched_files.sort(key=os.path.getmtime, reverse=True)
        return matched_files[0]

    def get_metadata_and_files(self, config_prefix):
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            query = """
                SELECT id, configuration_name, rule_details, created_at
                FROM dq_rule_config
                WHERE configuration_name = %s;
            """
            cursor.execute(query, (config_prefix,))
            rows = cursor.fetchall()

            if not rows:
                raise Exception(f"No metadata found for config: {config_prefix}")

            metadata = []
            for row in rows:
                metadata.append({
                    "id": row[0],
                    "configuration_name": row[1],
                    "rule_details": row[2],
                    "created_date": row[3].strftime("%Y-%m-%d %H:%M:%S") if row[3] else None
                })

            # File patterns
            clean_pattern = os.path.join(self.output_directory, f"{config_prefix}_*clean_records_*.csv")
            failed_pattern = os.path.join(self.output_directory, f"{config_prefix}_*failed_records_*.csv")
            summary_pattern = os.path.join(self.output_directory, f"{config_prefix}_summary_*.csv")

            clean_files = glob.glob(clean_pattern)
            failed_files = glob.glob(failed_pattern)
            summary_file = self.get_latest_file(summary_pattern)

            clean_files.sort(key=os.path.getmtime, reverse=True)
            failed_files.sort(key=os.path.getmtime, reverse=True)

            tables = set()
            for path in clean_files + failed_files:
                filename = os.path.basename(path)
                parts = filename.replace(f"{config_prefix}_", "").split("_")
                if len(parts) >= 1:
                    tables.add(parts[0])

            files_info = []
            for table in tables:
                clean_file = self.get_latest_file(os.path.join(self.output_directory, f"{config_prefix}_*_clean_records_*.csv"))
                failed_file = self.get_latest_file(os.path.join(self.output_directory, f"{config_prefix}_*_failed_records_*.csv"))

                files_info.append({
                    "table": table,
                    "clean_file": clean_file,
                    "failed_file": failed_file
                })

            # Read summary CSV to JSON if exists
            summary_data = []
            if summary_file:
                df_summary = pd.read_csv(summary_file)
                summary_data = df_summary.to_dict(orient="records")

            return metadata, files_info, summary_data

        except Exception as e:
            raise Exception(f"Error generating metadata and files: {str(e)}")
        finally:
            if conn:
                conn.close()

# Main endpoint
@app.route('/send_dq_files/<string:config_prefix>', methods=['GET'])
def send_dq_files(config_prefix):
    try:
        dq_api = DataQualityAPI(
            config_path="path/to/config.json",
            db_config={"some": "db_config"},
            dataprofile="yes"
        )

        metadata, files_info, summary_data = dq_api.get_metadata_and_files(config_prefix)
        base_url = request.host_url.rstrip('/')

        files_payload = []
        for info in files_info:
            if info["clean_file"] and info["failed_file"]:
                files_payload.append({
                    "table": info["table"],
                    "clean_file": f"{base_url}/download/clean/{os.path.basename(info['clean_file'])}",
                    "failed_file": f"{base_url}/download/failed/{os.path.basename(info['failed_file'])}"
                })
            else:
                files_payload.append({
                    "table": info["table"],
                    "message": "Clean or failed file not found."
                })

        return jsonify({
            "metadata": metadata,
            "files": files_payload,
            "summary_data": summary_data,
            "message": "DQ Files, Metadata, and Summary fetched successfully"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# File download endpoints
@app.route('/download/clean/<string:filename>', methods=['GET'])
def download_clean_file(filename):
    try:
        file_path = os.path.join(OUTPUT_DIR, filename)
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download/failed/<string:filename>', methods=['GET'])
def download_failed_file(filename):
    try:
        file_path = os.path.join(OUTPUT_DIR, filename)
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run app
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8007)

