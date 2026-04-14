import os
import sys
import random
import re
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# Faker import and initialization
try:
    from faker import Faker
    fake = Faker("he_IL")
except ImportError:
    print("Faker not installed. Please install with: pip install faker")
    sys.exit(1)

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

db_user     = os.getenv("DB_USER_SECRET")
db_password = os.getenv("DB_PASSWORD_SECRET")
db_name     = os.getenv("DB_NAME_SECRET")

# Maintaining existing connection logic
DATABASE_URL = f"postgresql://{db_user}:{db_password}@localhost:5432/{db_name}"
engine = create_engine(DATABASE_URL)

def get_random_fk_value(connection, referred_table, referred_column):
    """Query the parent table to get existing IDs and return a random one."""
    query = text(f'SELECT "{referred_column}" FROM "{referred_table}"')
    result = connection.execute(query).fetchall()
    if not result:
        return None
    return random.choice(result)[0]

def analyze_check_constraints(inspector, table_name):
    """Attempt to parse CHECK constraints for IN (...) logic."""
    constraints_map = {}
    try:
        checks = inspector.get_check_constraints(table_name)
        for ch in checks:
            sqltext = ch.get('sqltext', '')
            tokens = list(set(re.findall(r"'([^']+)'", sqltext)))
            for col in inspector.get_columns(table_name):
                if col['name'] in sqltext and tokens:
                    if col['name'] not in constraints_map:
                        constraints_map[col['name']] = []
                    for t in tokens:
                        if t not in constraints_map[col['name']]:
                            constraints_map[col['name']].append(t)
    except Exception:
        pass
    return constraints_map

def sample_existing_data(connection, table_name, columns):
    """
    Sample existing rows to find categorical columns or typical formats.
    """
    sampled_data = {}
    try:
        query = text(f'SELECT * FROM "{table_name}" LIMIT 200')
        result = connection.execute(query).fetchall()
        if not result:
            return sampled_data
            
        for c_idx, col in enumerate(columns):
            col_name = col['name']
            values = [row[c_idx] for row in result if row[c_idx] is not None]
            if not values:
                continue
            unique_values = set(values)
            if len(unique_values) <= 15 and len(values) >= len(unique_values):
                str_type = str(col['type']).upper()
                if 'VARCHAR' in str_type or 'TEXT' in str_type or 'CHAR' in str_type:
                    sampled_data[col_name] = list(unique_values)
    except Exception:
        pass
    return sampled_data

def generate_fake_data(col_name, col_type, constraints_map, sampled_data):
    """Generate realistic data based on constraints, existing data, or intelligent faker mapping."""
    if col_name in constraints_map and constraints_map[col_name]:
        return random.choice(constraints_map[col_name])
    if col_name in sampled_data and sampled_data[col_name]:
        return random.choice(sampled_data[col_name])
        
    name_lower = col_name.lower()
    type_str = str(col_type).upper()
    
    max_length = 255
    match = re.search(r'\((\d+)\)', type_str)
    if match:
        max_length = int(match.group(1))
        
    val = None
    
    # Adding large random suffix to unique fields to ensure no collisions
    if 'email' in name_lower:
        val = f"{fake.user_name()}_{random.randint(100000, 9999999)}@{fake.free_email_domain()}"
    elif 'phone' in name_lower:
        val = f"05{random.randint(0,9)}-{random.randint(1000000, 9999999)}"
    elif 'name' in name_lower or 'fullname' in name_lower:
        val = fake.name()
    elif 'date' in name_lower and 'time' not in name_lower:
        val = fake.date_this_year()
    elif 'time' in name_lower and 'date' not in name_lower:
        val = fake.time()  # e.g., '14:30:00'
    elif 'plate' in name_lower or 'license_plate' in name_lower:
        if random.choice([True, False]): 
            val = f"{random.randint(10, 99)}-{random.randint(100, 999)}-{random.randint(10, 99)}"
        else:
            val = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(100, 999)}"
    elif 'id_number' in name_lower or 'tz' in name_lower:
        val = str(random.randint(100000000, 999999999))
    elif 'capacity' in name_lower:
        val = random.choice([10, 15, 20, 25, 40, 50, 60, 80])
    elif 'VARCHAR' in type_str or 'TEXT' in type_str or 'CHAR' in type_str:
        val = f"{fake.word()}{random.randint(1, 9999)}"
    elif 'INT' in type_str:
        val = random.randint(10000, 999999)
    elif 'BOOL' in type_str:
        val = fake.boolean()
    elif 'DATE' in type_str:
        val = fake.date_this_year()
    elif 'TIMESTAMP' in type_str:
        val = fake.date_time_this_year()
    elif 'FLOAT' in type_str or 'NUMERIC' in type_str or 'DECIMAL' in type_str:
        val = round(random.uniform(1.0, 1000.0), 2)
    else:
        val = fake.word()
        
    # Strictly truncate string variables that exceed max_length
    if isinstance(val, str) and len(val) > max_length:
        val = val[:max_length]
        
    return val

def get_table_dependencies(inspector):
    """Retrieve foreign key dependencies as a map: {child_table: set(parent_tables)}."""
    tables = inspector.get_table_names()
    deps = {t: set() for t in tables}
    for t in tables:
        for fk in inspector.get_foreign_keys(t):
            if fk['referred_table'] != t:
                deps[t].add(fk['referred_table'])
    return deps

def topological_sort(tables_to_sort, deps):
    """Sort table generation order based on foreign key dependencies using topological sort."""
    result = []
    visited = set()
    visiting = set()
    
    def visit(node):
        if node in visiting: return
        if node in visited: return
        visiting.add(node)
        for dep in deps.get(node, set()):
            visit(dep)
        visiting.remove(node)
        visited.add(node)
        result.append(node)
        
    for t in tables_to_sort:
        visit(t)
        
    return [t for t in result if t in tables_to_sort]

def generate_for_table(selected_table, num_rows, inspector, connection):
    print(f"\n[+] Processing {selected_table}...")
    
    pk_constraint = inspector.get_pk_constraint(selected_table)
    pk_cols = pk_constraint['constrained_columns'] if pk_constraint else []
    fks = inspector.get_foreign_keys(selected_table)
    fk_dict = {} 
    
    for fk in fks:
        for constrained_col, referred_col in zip(fk['constrained_columns'], fk['referred_columns']):
            fk_dict[constrained_col] = (fk['referred_table'], referred_col)
            
    columns = inspector.get_columns(selected_table)
    constraints_map = analyze_check_constraints(inspector, selected_table)
    sampled_data = sample_existing_data(connection, selected_table, columns)
        
    # Pre-fetch existing primary keys if it's a single PK column system
    existing_pks = set()
    if len(pk_cols) == 1:
        pk_col = pk_cols[0]
        try:
            query = text(f'SELECT "{pk_col}" FROM "{selected_table}"')
            existing_pks = {row[0] for row in connection.execute(query).fetchall()}
        except SQLAlchemyError:
            pass
            
    success_count = 0
    skipped_count = 0
    
    for i in range(num_rows):
        row_data = {}
        skip_table = False
        
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            
            # 1. Skip auto-incrementing/serial primary keys
            if col_name in pk_cols and col.get('autoincrement') == True:
                continue
                
            # 2. Foreign Keys (Strict Bounds: Existing Only)
            if col_name in fk_dict:
                ref_table, ref_col = fk_dict[col_name]
                fk_val = get_random_fk_value(connection, ref_table, ref_col)
                if fk_val is None:
                    if i == 0:
                        print(f"    --> Error: Parent table '{ref_table}' is empty. Please fill table '{ref_table}' first.")
                    skip_table = True
                    break
                row_data[col_name] = fk_val
                continue
                
            # 3. Pure Mock High-Range for Non-Auto-Incrementing PKs (Avoiding MAX+1)
            if col_name in pk_cols:
                type_str = str(col_type).upper()
                pk_val = None
                for _ in range(50):
                    if 'INT' in type_str:
                        pk_val = random.randint(10000, 999999)
                    else:
                        pk_val = generate_fake_data(col_name, col_type, constraints_map, sampled_data)
                    
                    if pk_val not in existing_pks:
                        break
                existing_pks.add(pk_val)
                row_data[col_name] = pk_val
                continue
                
            # 4. Randomness with Constraints
            if hasattr(col_type, 'enums') and col_type.enums:
                row_data[col_name] = random.choice(col_type.enums)
            else:
                row_data[col_name] = generate_fake_data(col_name, col_type, constraints_map, sampled_data)
                
        if skip_table:
            # Table is fully skipped due to missing parent data
            skipped_count += (num_rows - i)
            break
            
        cols_str = ', '.join([f'"{k}"' for k in row_data.keys()])
        params_str = ', '.join([f':{k}' for k in row_data.keys()])
        query = text(f'INSERT INTO "{selected_table}" ({cols_str}) VALUES ({params_str})')
        
        try:
            with connection.begin_nested():
                connection.execute(query, row_data)
            success_count += 1
        except IntegrityError:
            skipped_count += 1
        except SQLAlchemyError:
            skipped_count += 1
            
    # Quiet Mode Single Output
    print(f"    [Result] Success: {success_count} | Failed/Skipped: {skipped_count}")

def main():
    inspector = inspect(engine)
    all_tables = inspector.get_table_names()
    
    if not all_tables:
        print("No tables found in the database. Please ensure your schema is set up.")
        return

    deps = get_table_dependencies(inspector)
    
    print("\n=============================================")
    print(" Welcome to RideFlow DB Data Generator UI")
    print("=============================================\n")
    
    while True:
        print("\nAvailable tables:")
        for i, t in enumerate(all_tables, 1):
            print(f"{i}. {t}")
            
        print("\nOptions:")
        print(" - Enter numbers separated by comma (e.g., '1, 3, 5')")
        print(" - Enter 'A' to generate data for ALL tables")
        print(" - Enter 'q' or '0' to Quit")
        
        user_input = input("\nSelect your choice: ").strip().lower()
        
        if user_input in ('q', '0', 'quit', 'exit'):
            print("Exiting generator. Goodbye!")
            break
            
        selected_tables = []
        if user_input == 'a':
            selected_tables = all_tables
        else:
            indices = [x.strip() for x in user_input.split(',') if x.strip()]
            for idx_str in indices:
                try:
                    idx = int(idx_str) - 1
                    if 0 <= idx < len(all_tables):
                        if all_tables[idx] not in selected_tables:
                            selected_tables.append(all_tables[idx])
                    else:
                        print(f"Warning: Index '{idx_str}' is out of range.")
                except ValueError:
                    print(f"Warning: '{idx_str}' is not a valid number.")
                    
        if not selected_tables:
            print("No valid tables selected. Try again.")
            continue
            
        ordered_tables = topological_sort(selected_tables, deps)
            
        # Ask for row counts
        table_row_counts = {}
        if len(ordered_tables) > 1:
            ans = input("\nGenerate the SAME number of rows for all selected tables? (y/n) [default: y]: ").strip().lower()
            if ans != 'n':
                try:
                    num = int(input("How many rows per table? "))
                    if num <= 0: raise ValueError
                    table_row_counts = {t: num for t in ordered_tables}
                except ValueError:
                    print("Invalid input. Cancelling generation.")
                    continue
            else:
                for t in ordered_tables:
                    try:
                        num = int(input(f"Rows to generate for '{t}': "))
                        if num <= 0: raise ValueError
                        table_row_counts[t] = num
                    except ValueError:
                        print("Invalid input. Cancelling generation.")
                        table_row_counts = {}
                        break
        else:
            try:
                num = int(input(f"\nHow many rows to insert into '{ordered_tables[0]}'? "))
                if num <= 0: raise ValueError
                table_row_counts[ordered_tables[0]] = num
            except ValueError:
                print("Invalid input. Cancelling generation.")
                continue
                
        if not table_row_counts:
            continue
            
        with engine.connect() as connection:
            for table in ordered_tables:
                num_rows = table_row_counts[table]
                if num_rows > 0:
                    generate_for_table(table, num_rows, inspector, connection)
            connection.commit()

if __name__ == "__main__":
    main()