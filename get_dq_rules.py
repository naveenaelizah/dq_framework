from fastapi import FastAPI, HTTPException, Header, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
import json
from fastapi.responses import JSONResponse
from typing import List 

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace "*" with specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection config
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="shreyas",
    host="localhost",
    port="5432"
)

# Predefined API key
API_KEY = "my-secret-api-key"

# Pydantic model
class DQRule(BaseModel):
    rule_name: str
    rule_target_function: str
    applicable_types: List[str]

@app.get("/dq_rules")
def get_dq_rules():
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, rule_name, rule_target_function, applicable_types, created_at FROM dq_rules;")
        rows = cursor.fetchall()

        results = []
        for row in rows:
            result = {
                "id": row[0],
                "name": row[1],
                "targetFunction": row[2],
                "applicable_types": json.loads(row[3]),
                "createdAt": row[4].strftime("%Y-%m-%d")
            }
            results.append(result)

        return JSONResponse(content=results)
    
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/run_dq")
def run_dq(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    try:
        return JSONResponse(content={"message": "Data quality checks executed successfully."})
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while running the DQ checks: {str(e)}"
        )

