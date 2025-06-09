import os
import json
import shutil
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, root_validator
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import Json

# --- Constants ---
CONFIG_FILE_PATH = r"/home/htu708/dq_solution/config/customer_dq_rules/config.json"
UPLOAD_DIR = r"/home/htu708/dq_solution/data/input"

# --- App Initialization ---
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(UPLOAD_DIR, exist_ok=True)

# --- PostgreSQL Connection ---
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="shreyas",
    host="localhost",
    port="5432"
)
conn.autocommit = False

# --- Helper Functions ---
def load_config():
    if not os.path.exists(CONFIG_FILE_PATH):
        return []
    with open(CONFIG_FILE_PATH, "r") as f:
        return json.load(f)

def save_config(config):
    with open(CONFIG_FILE_PATH, "w") as f:
        json.dump(config, f, indent=2)

def get_rule_dimension(rule_name: str) -> str:
    return "Completeness" if "null" in rule_name.lower() else "Validity"

# --- Pydantic Models ---
class RuleDetail(BaseModel):
    rule_name: str
    type: Optional[str]
    add_info: Optional[Dict[str, Any]] = {}

class DQRuleItem(BaseModel):
    column_name: str
    dq_rule: List[RuleDetail]

class TableConfig(BaseModel):
    table_name: str
    columns: List[str]
    dq_rules: List[DQRuleItem]

class FileConfig(BaseModel):
    file_name: str
    columns: List[str]
    dq_rules: List[DQRuleItem]
    primary_key: Optional[str] = "id"
    data_source: Optional[str] = "CSV_Source"

class ConfigRequest(BaseModel):
    config_name: str
    tables: Optional[List[TableConfig]] = None
    files: Optional[List[FileConfig]] = None

    @root_validator(skip_on_failure=True)
    def check_one_of_tables_or_files(cls, values):
        if not values.get("tables") and not values.get("files"):
            raise ValueError("Either 'tables' or 'files' must be provided.")
        if values.get("tables") and values.get("files"):
            raise ValueError("Only one of 'tables' or 'files' should be provided, not both.")
        return values

# --- Routes ---
@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_location = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"message": "File uploaded successfully", "file_path": file_location}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")

@app.post("/update-config")
def update_config(config: ConfigRequest):
    base_config = []

    if config.tables:
        base_config.append({"type": "db", "config_name": config.config_name, "database": "postgres", "tables": []})
        for incoming_table in config.tables:
            new_table = {
                "table_name": incoming_table.table_name,
                "primary_key": "id",
                "data_source": "HDFC_DQ_RULE",
                "dq_column": incoming_table.columns,
                "columns": [
                    {
                        "column_name": rule.column_name,
                        "dq_rule(s)": [
                            {
                                "rule_name": rule_detail.rule_name,
                                "rule_dimension": get_rule_dimension(rule_detail.rule_name),
                                "add_info": rule_detail.add_info or {}
                            }
                            for rule_detail in rule.dq_rule
                        ]
                    }
                    for rule in incoming_table.dq_rules
                ]
            }
            base_config[0]["tables"].append(new_table)

    elif config.files:
        base_config.append({"type": "csv", "config_name": config.config_name, "files": []})
        for incoming_file in config.files:
            new_file = {
                "file_name": incoming_file.file_name,
                "primary_key": incoming_file.primary_key,
                "data_source": incoming_file.data_source,
                "dq_column": incoming_file.columns,
                "columns": [
                    {
                        "column_name": rule.column_name,
                        "dq_rule(s)": [
                            {
                                "rule_name": rule_detail.rule_name,
                                "rule_dimension": get_rule_dimension(rule_detail.rule_name),
                                "add_info": rule_detail.add_info or {}
                            }
                            for rule_detail in rule.dq_rule
                        ]
                    }
                    for rule in incoming_file.dq_rules
                ]
            }
            base_config[0]["files"].append(new_file)

    save_config(base_config)

    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO dq_rule_config (configuration_name, table_name, rule_details, status)
            VALUES (%s, %s, %s, %s)
        """

        item_names = [item.table_name if config.tables else item.file_name for item in (config.tables or config.files)]
        rule_payload = []
        for item in (config.tables or config.files):
            rule_payload.append({
                "table_name" if config.tables else "file_name": item.table_name if config.tables else item.file_name,
                "columns": item.columns,
                "dq_rules": [
                    {
                        "column_name": rule.column_name,
                        "dq_rule": [
                            {
                                "rule_name": rule_detail.rule_name,
                                "add_info": rule_detail.add_info or {}
                            }
                            for rule_detail in rule.dq_rule
                        ]
                    }
                    for rule in item.dq_rules
                ]
            })

        cursor.execute(insert_query, (
            config.config_name,
            item_names,
            Json(rule_payload),
            "Pending"
        ))
        conn.commit()
        cursor.close()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    return {
        "message": "Configuration updated and saved successfully.",
        "updated_config": base_config
    }

