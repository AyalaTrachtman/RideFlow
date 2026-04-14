import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, MetaData

ENV_PATH = Path("C:/Users/Home/rideFlowProject/RideFlow/.env")
load_dotenv(dotenv_path=ENV_PATH)

db_user     = os.getenv("DB_USER_SECRET")
db_password = os.getenv("DB_PASSWORD_SECRET")
db_name     = os.getenv("DB_NAME_SECRET")

DATABASE_URL = f"postgresql://{db_user}:{db_password}@localhost:5432/{db_name}"
engine = create_engine(DATABASE_URL)

inspector = inspect(engine)
tables = inspector.get_table_names()
print("Tables:", tables)

if tables:
    for t in tables:
        print(f"\nInspecting table {t}:")
        print("PK:", inspector.get_pk_constraint(t))
        print("FK:", inspector.get_foreign_keys(t))
        print("Cols:")
        for c in inspector.get_columns(t):
            print("  -", c['name'], type(c['type']), c.get('autoincrement'))
