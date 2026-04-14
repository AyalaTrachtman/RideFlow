import os
import re
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import ENUM

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

db_user     = os.getenv("DB_USER_SECRET")
db_password = os.getenv("DB_PASSWORD_SECRET")
db_name     = os.getenv("DB_NAME_SECRET")

DATABASE_URL = f"postgresql://{db_user}:{db_password}@localhost:5432/{db_name}"
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

for t in inspector.get_table_names():
    print(f"\n================ TABLE: {t} ================")
    try:
        checks = inspector.get_check_constraints(t)
        for ch in checks:
            print("CHECK:", ch)
    except NotImplementedError:
        print("CHECK constraints not supported")
    
    cols = inspector.get_columns(t)
    for c in cols:
        print(f"Col {c['name']} (Type: {c['type']}, Type.__class__: {c['type'].__class__.__name__})")
        if isinstance(c['type'], ENUM):
            print("  ENUM ENUMS:", c['type'].enums)
