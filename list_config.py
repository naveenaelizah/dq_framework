from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with allowed domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection setup
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="shreyas",
    host="localhost",
    port="5432"
)

@app.get("/list_configurations")
def list_configurations():
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, configuration_name, table_name, created_at, status FROM dq_rule_config ORDER BY id DESC;"
        )
        rows = cursor.fetchall()

        configurations = []
        for row in rows:
            configurations.append({
                "id": row[0],
                "name": row[1],
                "table_name": row[2],
                "createdAt": row[3].strftime("%Y-%m-%d") if row[3] else None,
                "status": row[4]
            })

        cursor.close()
        return JSONResponse(content=configurations)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

