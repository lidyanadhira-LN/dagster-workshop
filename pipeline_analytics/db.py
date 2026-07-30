import os
from sqlalchemy import create_engine

def get_engine():
    user = os.getenv("WAREHOUSE_USER", "warehouse_user")
    password = os.getenv("WAREHOUSE_PASSWORD", "warehouse_password")
    host = os.getenv("WAREHOUSE_HOST", "warehouse_postgresql")
    port = os.getenv("WAREHOUSE_PORT", "5432")
    db_name = os.getenv("WAREHOUSE_DB", "warehouse")

    url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(url)