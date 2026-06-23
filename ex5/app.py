import os
import re
import traceback
from flask import Flask, jsonify, request, send_from_directory
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__, static_folder='static', static_url_path='')

# 100% Match to your actual database table names (lowercase)
TABLES = {
    'driver': 'driver',
    'passenger': 'passenger',
    'vehicle': 'vehicle',
    'route': 'route',
    'stop': 'stop',
    'includes': 'includes',
    'routestop': 'routestop',
    'trip': 'trip',
    'registration': 'registration'
}

# Database connection helper
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="merged",
        user="rideflow_user",
        password="rideflowsecret"
    )

# Auto-heal the schema by ensuring status column exists in the trip table if needed
def ensure_trip_status_column():
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        trip_table = TABLES['trip']
        # Use IF NOT EXISTS to avoid DuplicateColumn error
        cur.execute(f"ALTER TABLE public.{trip_table} ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'Active';")
        print(f"Status column ensured in table '{trip_table}'.")
        cur.close()
    except Exception as e:
        print(f"Note: Status column check skipped: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

ensure_trip_status_column()

# Serve the frontend home page
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

# --- 1. STATISTICS FOR DASHBOARD ---
@app.route('/api/stats', methods=['GET'])
def get_stats():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        stats = {}
        for table_key, table_val in TABLES.items():
            try:
                cur.execute(f"SELECT COUNT(*) FROM public.{table_val};")
                stats[table_key] = cur.fetchone()[0]
            except Exception:
                stats[table_key] = 0
                
        cur.close()
        return jsonify(stats)
    except Exception as e:
        print("API Error in /api/stats:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- 2. FOREIGN KEY OPTIONS FOR DROPDOWNS ---
@app.route('/api/options/<table_name>', methods=['GET'])
def get_table_options(table_name):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        opt_limit = min(int(request.args.get('limit', 300)), 500)
        if table_name == 'driver':
            cur.execute(f"SELECT driver_id, driver_fullname FROM public.{TABLES['driver']} ORDER BY driver_fullname LIMIT %s;", (opt_limit,))
        elif table_name == 'passenger':
            cur.execute(f"SELECT pass_id, pass_fullname FROM public.{TABLES['passenger']} ORDER BY pass_fullname LIMIT %s;", (opt_limit,))
        elif table_name == 'route':
            cur.execute(f"SELECT route_id, route_name FROM public.{TABLES['route']} ORDER BY route_name LIMIT %s;", (opt_limit,))
        elif table_name == 'stop':
            cur.execute(f"SELECT stop_id, stop_name FROM public.{TABLES['stop']} ORDER BY stop_name LIMIT %s;", (opt_limit,))
        elif table_name == 'vehicle':
            cur.execute(f"SELECT plate_number, capacity FROM public.{TABLES['vehicle']} ORDER BY plate_number LIMIT %s;", (opt_limit,))
        elif table_name == 'trip':
            cur.execute(f"""
                SELECT t.trip_id, t.trip_date, r.route_name 
                FROM public.{TABLES['trip']} t 
                JOIN public.{TABLES['route']} r ON t.route_id = r.route_id 
                ORDER BY t.trip_id DESC LIMIT %s;
            """, (opt_limit,))
        else:
            return jsonify({"error": "Unknown options table"}), 400
            
        rows = cur.fetchall()
        cur.close()
        return jsonify(rows)
    except Exception as e:
        print(f"API Error in /api/options/{table_name}:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- 3. DYNAMIC DATA LISTING ---
@app.route('/api/data/<table_name>', methods=['GET'])
def get_table_data(table_name):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        limit = min(int(request.args.get('limit', 100)), 500)  # max 500 rows
        if table_name == 'driver':
            cur.execute(f"SELECT driver_id, driver_fullname, licensetype FROM public.{TABLES['driver']} ORDER BY driver_id DESC LIMIT %s;", (limit,))
        elif table_name == 'passenger':
            cur.execute(f"SELECT pass_id, pass_fullname, email, phone, sector FROM public.{TABLES['passenger']} ORDER BY pass_id DESC LIMIT %s;", (limit,))
        elif table_name == 'vehicle':
            cur.execute(f"SELECT plate_number, capacity, vehicle_type FROM public.{TABLES['vehicle']} ORDER BY plate_number DESC LIMIT %s;", (limit,))
        elif table_name == 'route':
            cur.execute(f"SELECT route_id, route_name FROM public.{TABLES['route']} ORDER BY route_id DESC LIMIT %s;", (limit,))
        elif table_name == 'stop':
            cur.execute(f"SELECT stop_id, stop_name FROM public.{TABLES['stop']} ORDER BY stop_id DESC LIMIT %s;", (limit,))
        elif table_name == 'includes':
            cur.execute(f"""
                SELECT i.route_id, r.route_name, i.stop_id, s.stop_name
                FROM public.{TABLES['includes']} i
                JOIN public.{TABLES['route']} r ON i.route_id = r.route_id
                JOIN public.{TABLES['stop']} s ON i.stop_id = s.stop_id
                ORDER BY i.route_id DESC LIMIT %s;
            """, (limit,))
        elif table_name == 'routestop':
            cur.execute(f"""
                SELECT rs.route_id, r.route_name, rs.stop_id, s.stop_name, rs.stop_order
                FROM public.{TABLES['routestop']} rs
                JOIN public.{TABLES['route']} r ON rs.route_id = r.route_id
                JOIN public.{TABLES['stop']} s ON rs.stop_id = s.stop_id
                ORDER BY rs.route_id DESC, rs.stop_order ASC LIMIT %s;
            """, (limit,))
        elif table_name == 'trip':
            cur.execute(f"""
                SELECT t.trip_id, t.trip_date, t.departure_time, t.available_seats,
                       t.route_id, r.route_name,
                       t.driver_id, d.driver_fullname,
                       t.plate_number
                FROM public.{TABLES['trip']} t
                LEFT JOIN public.{TABLES['route']} r ON t.route_id = r.route_id
                LEFT JOIN public.{TABLES['driver']} d ON t.driver_id = d.driver_id
                ORDER BY t.trip_id DESC LIMIT %s;
            """, (limit,))
        elif table_name == 'registration':
            cur.execute(f"""
                SELECT reg.reg_id, reg.status,
                       reg.pass_id, p.pass_fullname,
                       reg.trip_id, t.trip_date, r.route_name,
                       reg.boarding_stop_id, s_board.stop_name as boarding_stop_name,
                       reg.dropoff_stop_id, s_drop.stop_name as dropoff_stop_name
                FROM public.{TABLES['registration']} reg
                LEFT JOIN public.{TABLES['passenger']} p ON reg.pass_id = p.pass_id
                LEFT JOIN public.{TABLES['trip']} t ON reg.trip_id = t.trip_id
                LEFT JOIN public.{TABLES['route']} r ON t.route_id = r.route_id
                LEFT JOIN public.{TABLES['stop']} s_board ON reg.boarding_stop_id = s_board.stop_id
                LEFT JOIN public.{TABLES['stop']} s_drop ON reg.dropoff_stop_id = s_drop.stop_id
                ORDER BY reg.reg_id DESC LIMIT %s;
            """, (limit,))
        else:
            return jsonify({"error": "Unknown table"}), 400
            
        rows = cur.fetchall()
        cur.close()
        return jsonify(rows)
    except Exception as e:
        print(f"API Error in /api/data/{table_name}:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- 4. FETCH SINGLE RECORD DETAILS FOR UPDATE ---
@app.route('/api/data/<table_name>/fetch', methods=['GET'])
def fetch_single_record(table_name):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        pk_fields = {
            'driver': 'driver_id',
            'passenger': 'pass_id',
            'vehicle': 'plate_number',
            'route': 'route_id',
            'stop': 'stop_id',
            'trip': 'trip_id',
            'includes': 'route_id,stop_id',
            'routestop': 'route_id,stop_id',
            'registration': 'reg_id,pass_id'
        }
        
        if table_name not in pk_fields:
            return jsonify({"error": "Table does not support single fetch"}), 400
            
        pk_field = pk_fields[table_name]
        resolved_table = TABLES[table_name]
        
        if ',' in pk_field:
            keys = pk_field.split(',')
            where_clauses = []
            params = []
            for key in keys:
                val = request.args.get(key)
                if not val:
                    return jsonify({"error": f"Missing parameter {key}"}), 400
                where_clauses.append(f"{key} = %s")
                params.append(val)
            query = f"SELECT * FROM public.{resolved_table} WHERE " + " AND ".join(where_clauses)
            cur.execute(query, tuple(params))
        else:
            val = request.args.get('id')
            if not val:
                return jsonify({"error": "Missing parameter 'id'"}), 400
            query = f"SELECT * FROM public.{resolved_table} WHERE {pk_field} = %s"
            cur.execute(query, (val,))
            
        row = cur.fetchone()
        cur.close()
        
        if not row:
            return jsonify({"error": "Record not found"}), 404
            
        return jsonify(row)
    except Exception as e:
        print(f"API Error in /api/data/{table_name}/fetch:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- 5. INSERT (CREATE) RECORD ---
@app.route('/api/data/<table_name>', methods=['POST'])
def create_record(table_name):
    conn = None
    try:
        data = request.json
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        resolved_table = TABLES[table_name]
        
        if table_name == 'driver':
            cur.execute(f"SELECT COALESCE(MAX(driver_id), 0) + 1 FROM public.{resolved_table};")
            new_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO public.{resolved_table} (driver_id, driver_fullname, licensetype) VALUES (%s, %s, %s);", 
                        (new_id, data['driver_fullname'], data['licensetype']))
        elif table_name == 'passenger':
            cur.execute(f"SELECT COALESCE(MAX(pass_id), 0) + 1 FROM public.{resolved_table};")
            new_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO public.{resolved_table} (pass_id, pass_fullname, email, phone, sector) VALUES (%s, %s, %s, %s, %s);", 
                        (new_id, data['pass_fullname'], data.get('email'), data['phone'], data.get('sector')))
        elif table_name == 'vehicle':
            cur.execute(f"INSERT INTO public.{resolved_table} (plate_number, capacity, vehicle_type) VALUES (%s, %s, %s);", 
                        (data['plate_number'], data['capacity'], data.get('vehicle_type')))
        elif table_name == 'route':
            cur.execute(f"SELECT COALESCE(MAX(route_id), 0) + 1 FROM public.{resolved_table};")
            new_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO public.{resolved_table} (route_id, route_name) VALUES (%s, %s);", (new_id, data['route_name']))
        elif table_name == 'stop':
            cur.execute(f"SELECT COALESCE(MAX(stop_id), 0) + 1 FROM public.{resolved_table};")
            new_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO public.{resolved_table} (stop_id, stop_name) VALUES (%s, %s);", (new_id, data['stop_name']))
        elif table_name == 'includes':
            cur.execute(f"INSERT INTO public.{resolved_table} (route_id, stop_id) VALUES (%s, %s);", (data['route_id'], data['stop_id']))
        elif table_name == 'routestop':
            cur.execute(f"INSERT INTO public.{resolved_table} (route_id, stop_id, stop_order) VALUES (%s, %s, %s);", 
                        (data['route_id'], data['stop_id'], data['stop_order']))
        elif table_name == 'trip':
            cur.execute(f"SELECT COALESCE(MAX(trip_id), 0) + 1 FROM public.{resolved_table};")
            new_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO public.{resolved_table} (trip_id, trip_date, departure_time, available_seats, route_id, driver_id, plate_number) VALUES (%s, %s, %s, %s, %s, %s, %s);", 
                        (new_id, data['trip_date'], data['departure_time'], data['available_seats'], data['route_id'], data['driver_id'], data['plate_number']))
        elif table_name == 'registration':
            cur.execute(f"SELECT COALESCE(MAX(reg_id), 0) + 1 FROM public.{resolved_table};")
            new_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO public.{resolved_table} (reg_id, pass_id, trip_id, boarding_stop_id, dropoff_stop_id, status) VALUES (%s, %s, %s, %s, %s, %s);", 
                        (new_id, data['pass_id'], data['trip_id'], data['boarding_stop_id'], data['dropoff_stop_id'], data.get('status', 'Confirmed')))
        else:
            return jsonify({"error": "Unknown table"}), 400
            
        cur.close()
        return jsonify({"success": True, "message": f"Record created in {table_name}"})
    except Exception as e:
        print(f"API Error in create_record for {table_name}:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

# --- 6. UPDATE RECORD ---
@app.route('/api/data/<table_name>', methods=['PUT'])
def update_record(table_name):
    conn = None
    try:
        data = request.json
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        resolved_table = TABLES[table_name]
        
        if table_name == 'driver':
            cur.execute(f"UPDATE public.{resolved_table} SET driver_fullname = %s, licensetype = %s WHERE driver_id = %s;", 
                        (data['driver_fullname'], data['licensetype'], data['driver_id']))
        elif table_name == 'passenger':
            cur.execute(f"UPDATE public.{resolved_table} SET pass_fullname = %s, email = %s, phone = %s, sector = %s WHERE pass_id = %s;", 
                        (data['pass_fullname'], data.get('email'), data['phone'], data.get('sector'), data['pass_id']))
        elif table_name == 'vehicle':
            cur.execute(f"UPDATE public.{resolved_table} SET capacity = %s, vehicle_type = %s WHERE plate_number = %s;", 
                        (data['capacity'], data.get('vehicle_type'), data['plate_number']))
        elif table_name == 'route':
            cur.execute(f"UPDATE public.{resolved_table} SET route_name = %s WHERE route_id = %s;", (data['route_name'], data['route_id']))
        elif table_name == 'stop':
            cur.execute(f"UPDATE public.{resolved_table} SET stop_name = %s WHERE stop_id = %s;", (data['stop_name'], data['stop_id']))
        elif table_name == 'routestop':
            cur.execute(f"UPDATE public.{resolved_table} SET stop_order = %s WHERE route_id = %s AND stop_id = %s;", 
                        (data['stop_order'], data['route_id'], data['stop_id']))
        elif table_name == 'trip':
            cur.execute(f"UPDATE public.{resolved_table} SET trip_date = %s, departure_time = %s, available_seats = %s, route_id = %s, driver_id = %s, plate_number = %s WHERE trip_id = %s;", 
                        (data['trip_date'], data['departure_time'], data['available_seats'], data['route_id'], data['driver_id'], data['plate_number'], data['trip_id']))
        elif table_name == 'registration':
            cur.execute(f"UPDATE public.{resolved_table} SET status = %s, boarding_stop_id = %s, dropoff_stop_id = %s WHERE reg_id = %s AND pass_id = %s;", 
                        (data['status'], data['boarding_stop_id'], data['dropoff_stop_id'], data['reg_id'], data['pass_id']))
        else:
            return jsonify({"error": "Unknown table"}), 400
            
        cur.close()
        return jsonify({"success": True, "message": f"Record updated in {table_name}"})
    except Exception as e:
        print(f"API Error in update_record for {table_name}:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

# --- 7. DELETE RECORD ---
@app.route('/api/data/<table_name>', methods=['DELETE'])
def delete_record(table_name):
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        resolved_table = TABLES[table_name]
        
        if table_name == 'driver':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE driver_id = %s;", (request.args.get('driver_id'),))
        elif table_name == 'passenger':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE pass_id = %s;", (request.args.get('pass_id'),))
        elif table_name == 'vehicle':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE plate_number = %s;", (request.args.get('plate_number'),))
        elif table_name == 'route':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE route_id = %s;", (request.args.get('route_id'),))
        elif table_name == 'stop':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE stop_id = %s;", (request.args.get('stop_id'),))
        elif table_name == 'includes':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE route_id = %s AND stop_id = %s;", 
                        (request.args.get('route_id'), request.args.get('stop_id')))
        elif table_name == 'routestop':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE route_id = %s AND stop_id = %s;", 
                        (request.args.get('route_id'), request.args.get('stop_id')))
        elif table_name == 'trip':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE trip_id = %s;", (request.args.get('trip_id'),))
        elif table_name == 'registration':
            cur.execute(f"DELETE FROM public.{resolved_table} WHERE reg_id = %s AND pass_id = %s;", 
                        (request.args.get('reg_id'), request.args.get('pass_id')))
        else:
            return jsonify({"error": "Unknown table"}), 400
            
        cur.close()
        return jsonify({"success": True, "message": f"Record deleted from {table_name}"})
    except Exception as e:
        print(f"API Error in delete_record for {table_name}:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

# --- 8. STEP 2 CUSTOM QUERIES ---
@app.route('/api/query/<query_id>', methods=['GET'])
def get_custom_query(query_id):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if query_id == '1':
            cur.execute(f"""
                SELECT t.trip_id, r.route_name, t.trip_date, COUNT(reg.reg_id) AS registration_count
                FROM public.{TABLES['trip']} t
                LEFT JOIN public.{TABLES['registration']} reg ON t.trip_id = reg.trip_id
                JOIN public.{TABLES['route']} r ON t.route_id = r.route_id
                GROUP BY t.trip_id, r.route_name, t.trip_date
                HAVING COUNT(reg.reg_id) > (
                    SELECT AVG(cnt)
                    FROM (
                        SELECT t2.trip_id, COUNT(r2.reg_id) AS cnt
                        FROM public.{TABLES['trip']} t2
                        LEFT JOIN public.{TABLES['registration']} r2 ON t2.trip_id = r2.trip_id
                        GROUP BY t2.trip_id
                    ) AS sub
                )
                ORDER BY registration_count DESC;
            """)
        elif query_id == '2':
            cur.execute(f"""
                SELECT d.driver_fullname, d.driver_id, d.licensetype, COUNT(t.trip_id) AS trip_count
                FROM public.{TABLES['driver']} d
                JOIN public.{TABLES['trip']} t ON d.driver_id = t.driver_id
                GROUP BY d.driver_fullname, d.driver_id, d.licensetype
                HAVING COUNT(t.trip_id) >= 1
                ORDER BY trip_count DESC;
            """)
        elif query_id == '3':
            cur.execute(f"""
                SELECT r.route_id, r.route_name, COUNT(reg.reg_id) as total_registrations
                FROM public.{TABLES['route']} r 
                LEFT JOIN public.{TABLES['trip']} t ON r.route_id = t.route_id 
                LEFT JOIN public.{TABLES['registration']} reg ON t.trip_id = reg.trip_id 
                GROUP BY r.route_id, r.route_name 
                ORDER BY total_registrations DESC;
            """)
        else:
            return jsonify({"error": "Unknown query ID"}), 400
            
        rows = cur.fetchall()
        cur.close()
        return jsonify(rows)
    except Exception as e:
        print(f"API Error in custom query {query_id}:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- 9. STEP 4 PROCEDURES & FUNCTIONS ---
@app.route('/api/routine/cancel_trip', methods=['POST'])
def cancel_trip_routine():
    conn = None
    try:
        data = request.json
        trip_id = data.get('trip_id')
        if not trip_id:
            return jsonify({"error": "Missing trip_id"}), 400
            
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("CALL cancel_trip_and_notify(%s);", (trip_id,))
        
        notices = [n.strip() for n in conn.notices] if conn.notices else []
        cur.close()
        return jsonify({
            "success": True, 
            "message": f"Trip {trip_id} cancellation procedure finished.",
            "notices": notices
        })
    except Exception as e:
        print("API Error in cancel_trip_routine:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

@app.route('/api/routine/register_passenger', methods=['POST'])
def register_passenger_routine():
    conn = None
    try:
        data = request.json
        pass_id = data.get('pass_id')
        trip_id = data.get('trip_id')
        boarding_stop = data.get('boarding_stop_id')
        dropoff_stop = data.get('dropoff_stop_id')
        
        if not all([pass_id, trip_id, boarding_stop, dropoff_stop]):
            return jsonify({"error": "Missing parameters"}), 400
            
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("CALL register_passenger_to_trip(%s, %s, %s, %s);", 
                    (pass_id, trip_id, boarding_stop, dropoff_stop))
        
        notices = [n.strip() for n in conn.notices] if conn.notices else []
        cur.close()
        return jsonify({
            "success": True, 
            "message": "Passenger registration complete.",
            "notices": notices
        })
    except Exception as e:
        print("API Error in register_passenger_routine:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

@app.route('/api/routine/passenger_trips', methods=['GET'])
def get_passenger_trips_routine():
    conn = None
    try:
        pass_id = request.args.get('pass_id')
        if not pass_id:
            return jsonify({"error": "Missing passenger ID"}), 400
            
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT get_passenger_active_trips(%s);", (pass_id,))
        cur.execute('FETCH ALL FROM "passenger_trips_cursor";')
        rows = cur.fetchall()
        
        cur.close()
        conn.commit()
        return jsonify(rows)
    except Exception as e:
        print("API Error in get_passenger_trips_routine:")
        traceback.print_exc()
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

@app.route('/api/routine/route_seats', methods=['GET'])
def get_route_seats_routine():
    conn = None
    try:
        route_id = request.args.get('route_id')
        if not route_id:
            return jsonify({"error": "Missing route ID"}), 400
            
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT get_route_available_seats(%s);", (route_id,))
        result = cur.fetchone()[0]
        
        cur.close()
        return jsonify({"route_id": route_id, "available_seats": result})
    except Exception as e:
        print("API Error in get_route_seats_routine:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)