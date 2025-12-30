import streamlit as st
from streamlit_autorefresh import st_autorefresh
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import os
import pytz
from weakref import WeakSet

from assistant import suggest_best_driver, calculate_distance

_pool_connections = WeakSet()

def get_scalar_result(cursor):
    result = cursor.fetchone()
    if result is None:
        return None
    return list(result. values())[0]

TIMEZONE = pytz.timezone('Europe/Paris')

st.set_page_config(
    page_title="Transport DanGE - Planning",
    page_icon="🚖",
    layout="wide"
)

@st.cache_resource
def get_connection_pool():
    try:
        supabase = st.secrets. get("supabase", {}) or {}
        if "connection_string" in supabase and supabase["connection_string"]: 
            return pool.SimpleConnectionPool(1, 10, supabase["connection_string"])
        else:
            return pool.SimpleConnectionPool(
                1, 10,
                host=supabase.get("host"),
                database=supabase.get("database"),
                user=supabase.get("user"),
                password=supabase.get("password"),
                port=supabase. get("port"),
                sslmode='require'
            )
    except Exception as e:
        st.error(f"Erreur pool connexion:  {e}")
        return None

def release_db_connection(conn):
    global _pool_connections
    try:
        if not conn: 
            return
        conn_pool = get_connection_pool()
        if conn in _pool_connections and conn_pool:
            try:
                conn_pool.putconn(conn)
                _pool_connections.discard(conn)
            except Exception: 
                try:
                    conn. close()
                    _pool_connections.discard(conn)
                except Exception:
                    pass
        else:
            try:
                conn. close()
            except Exception:
                pass
    except Exception as e:
        print(f"Erreur release_db_connection: {e}")

def get_db_connection():
    global _pool_connections
    try:
        conn_pool = get_connection_pool()
        if conn_pool: 
            conn = conn_pool.getconn()
            conn. cursor_factory = RealDictCursor
            _pool_connections.add(conn)
            return conn
        supabase = st.secrets.get("supabase", {}) or {}
        if "connection_string" in supabase and supabase["connection_string"]: 
            conn = psycopg2.connect(supabase["connection_string"], cursor_factory=RealDictCursor)
        else:
            conn = psycopg2.connect(
                host=supabase.get("host"),
                database=supabase.get("database"),
                user=supabase.get("user"),
                password=supabase.get("password"),
                port=supabase.get("port"),
                sslmode='require',
                cursor_factory=RealDictCursor
            )
        return conn
    except Exception as e: 
        st.error(f"Erreur de connexion à la base de données: {e}")
        return None

def init_db():
    init_notifications_table()

def hash_password(password):
    return hashlib.sha256(password. encode()).hexdigest()

def login(username, password):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor()
    hashed_password = hash_password(password)
    cursor.execute('''
        SELECT id, username, role, full_name
        FROM users
        WHERE username = %s AND password_hash = %s
    ''', (username, hashed_password))
    user = cursor.fetchone()
    release_db_connection(conn)
    if user:
        return {
            'id': user['id'],
            'username': user['username'],
            'role': user['role'],
            'full_name': user['full_name']
        }
    return None

def get_chauffeurs():
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, full_name, username
        FROM users
        WHERE role = 'chauffeur'
        ORDER BY full_name
    ''')
    chauffeurs = cursor.fetchall()
    release_db_connection(conn)
    return [{'id': c['id'], 'full_name': c['full_name'], 'username': c['username']} for c in chauffeurs]

def init_notifications_table():
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS notifications (
            id SERIAL PRIMARY KEY,
            chauffeur_id INTEGER REFERENCES users(id),
            course_id INTEGER,
            message TEXT,
            type VARCHAR(50),
            lu BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    release_db_connection(conn)

def create_notification(chauffeur_id, course_id, message, notification_type='nouvelle_course'):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO notifications (chauffeur_id, course_id, message, type)
        VALUES (%s, %s, %s, %s)
    ''', (chauffeur_id, course_id, message, notification_type))
    conn.commit()
    release_db_connection(conn)
    return True

def get_unread_notifications(chauffeur_id):
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    cursor.execute('''
        SELECT n.id, n.message, n.type, n.created_at, n.course_id,
               c.nom_client, c.adresse_pec, c.lieu_depose, c.heure_pec_prevue
        FROM notifications n
        LEFT JOIN courses c ON n.course_id = c.id
        WHERE n.chauffeur_id = %s AND n.lu = FALSE
        ORDER BY n.created_at DESC
        LIMIT 20
    ''', (chauffeur_id,))
    notifs = cursor.fetchall()
    release_db_connection(conn)
    return [dict(n) for n in notifs]

def mark_notifications_as_read(chauffeur_id):
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE notifications
        SET lu = TRUE
        WHERE chauffeur_id = %s AND lu = FALSE
    ''', (chauffeur_id,))
    conn.commit()
    release_db_connection(conn)

def get_unread_count(chauffeur_id):
    conn = get_db_connection()
    if not conn:
        return 0
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM notifications
        WHERE chauffeur_id = %s AND lu = FALSE
    ''', (chauffeur_id,))
    result = cursor.fetchone()
    release_db_connection(conn)
    return list(result. values())[0] if result else 0

def create_client_regulier(data):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO clients_reguliers (
            nom_complet, telephone, adresse_pec_habituelle, adresse_depose_habituelle,
            type_course_habituel, tarif_habituel, km_habituels, remarques
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        data['nom_complet'],
        data. get('telephone'),
        data.get('adresse_pec_habituelle'),
        data.get('adresse_depose_habituelle'),
        data.get('type_course_habituel'),
        data.get('tarif_habituel'),
        data.get('km_habituels'),
        data.get('remarques')
    ))
    client_id = cursor.lastrowid
    conn.commit()
    release_db_connection(conn)
    return client_id

def get_clients_reguliers(search_term=None):
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    if search_term:
        cursor.execute('''
            SELECT * FROM clients_reguliers
            WHERE actif = 1 AND nom_complet LIKE %s
            ORDER BY nom_complet
        ''', (f'%{search_term}%',))
    else:
        cursor.execute('''
            SELECT * FROM clients_reguliers
            WHERE actif = 1
            ORDER BY nom_complet
        ''')
    clients = cursor.fetchall()
    release_db_connection(conn)
    result = []
    for client in clients:
        result.append({
            'id': client['id'],
            'nom_complet':  client['nom_complet'],
            'telephone': client['telephone'],
            'adresse_pec_habituelle': client['adresse_pec_habituelle'],
            'adresse_depose_habituelle': client['adresse_depose_habituelle'],
            'type_course_habituel':  client['type_course_habituel'],
            'tarif_habituel': client['tarif_habituel'],
            'km_habituels': client['km_habituels'],
            'remarques': client['remarques']
        })
    return result

def get_client_regulier(client_id):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM clients_reguliers WHERE id = %s', (client_id,))
    client = cursor.fetchone()
    release_db_connection(conn)
    if client:
        return {
            'id': client['id'],
            'nom_complet':  client['nom_complet'],
            'telephone': client['telephone'],
            'adresse_pec_habituelle': client['adresse_pec_habituelle'],
            'adresse_depose_habituelle': client['adresse_depose_habituelle'],
            'type_course_habituel':  client['type_course_habituel'],
            'tarif_habituel': client['tarif_habituel'],
            'km_habituels': client['km_habituels'],
            'remarques': client['remarques']
        }
    return None

def update_client_regulier(client_id, data):
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE clients_reguliers
        SET nom_complet = %s, telephone = %s, adresse_pec_habituelle = %s,
            adresse_depose_habituelle = %s, type_course_habituel = %s,
            tarif_habituel = %s, km_habituels = %s, remarques = %s
        WHERE id = %s
    ''', (
        data['nom_complet'],
        data.get('telephone'),
        data.get('adresse_pec_habituelle'),
        data.get('adresse_depose_habituelle'),
        data.get('type_course_habituel'),
        data.get('tarif_habituel'),
        data.get('km_habituels'),
        data.get('remarques'),
        client_id
    ))
    conn.commit()
    release_db_connection(conn)

def delete_client_regulier(client_id):
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute('UPDATE clients_reguliers SET actif = 0 WHERE id = %s', (client_id,))
    conn.commit()
    release_db_connection(conn)

def create_course(data):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn. cursor()
    heure_prevue = data['heure_prevue']
    if isinstance(heure_prevue, str):
        heure_prevue = datetime.fromisoformat(heure_prevue. replace('Z', '+00:00'))
    if heure_prevue. tzinfo is None:
        heure_prevue = TIMEZONE.localize(heure_prevue)
    else:
        heure_prevue = heure_prevue.astimezone(TIMEZONE)
    date_course = heure_prevue.date()
    date_aujourdhui = datetime.now(TIMEZONE).date()
    visible_chauffeur = (date_course <= date_aujourdhui)
    cursor.execute('''
        INSERT INTO courses (
            chauffeur_id, nom_client, telephone_client, adresse_pec,
            lieu_depose, heure_prevue, heure_pec_prevue, temps_trajet_minutes,
            heure_depart_calculee, type_course, tarif_estime,
            km_estime, commentaire, created_by, client_regulier_id, visible_chauffeur
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    ''', (
        data['chauffeur_id'],
        data['nom_client'],
        data['telephone_client'],
        data['adresse_pec'],
        data['lieu_depose'],
        data['heure_prevue'],
        data. get('heure_pec_prevue'),
        data.get('temps_trajet_minutes'),
        data.get('heure_depart_calculee'),
        data['type_course'],
        data['tarif_estime'],
        data['km_estime'],
        data['commentaire'],
        data['created_by'],
        data. get('client_regulier_id'),
        visible_chauffeur
    ))
    result = cursor.fetchone()
    course_id = result['id'] if result else None
    conn.commit()
    release_db_connection(conn)
    return course_id

def format_date_fr(date_input):
    if not date_input:
        return ""
    if isinstance(date_input, datetime):
        date_str = date_input.strftime('%Y-%m-%d')
    else:
        date_str = str(date_input)
    if len(date_str) < 10:
        return date_str
    annee, mois, jour = date_str[0:10].split('-')
    return f"{jour}/{mois}/{annee}"

def format_datetime_fr(datetime_input):
    if not datetime_input: 
        return ""
    try:
        if isinstance(datetime_input, datetime):
            datetime_str = datetime_input.strftime('%Y-%m-%d %H:%M:%S')
        else:
            datetime_str = str(datetime_input)
        datetime_str = datetime_str.replace('T', ' ')
        if len(datetime_str) >= 16:
            date_part = datetime_str[0:10]
            time_part = datetime_str[11:16]
            annee, mois, jour = date_part.split('-')
            return f"{jour}/{mois}/{annee} {time_part}"
        else:
            return format_date_fr(datetime_input)
    except:
        return str(datetime_input)

def extract_time_str(datetime_input):
    if not datetime_input:
        return ""
    if isinstance(datetime_input, datetime):
        return datetime_input.strftime('%H:%M')
    datetime_str = str(datetime_input)
    if len(datetime_str) >= 16:
        return datetime_str[11:16]
    return ""

def get_courses(chauffeur_id=None, date_filter=None, role=None, days_back=30, limit=100):
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    query = '''
        SELECT c.*, u.full_name as chauffeur_name
        FROM courses c
        JOIN users u ON c.chauffeur_id = u.id
        WHERE 1=1
    '''
    params = []
    try:
        if date_filter:
            if isinstance(date_filter, datetime):
                param_date = date_filter.date().strftime('%Y-%m-%d')
            else:
                s = str(date_filter).strip()
                s = s.replace('/', '-').replace('T', ' ')
                try:
                    dt = datetime.fromisoformat(s)
                    param_date = dt.date().strftime('%Y-%m-%d')
                except Exception:
                    param_date = s[0:10]
            query += " AND DATE(c.heure_prevue) = CAST(%s AS date)"
            params.append(param_date)
        else:
            date_limite = (datetime.now(TIMEZONE) - timedelta(days=days_back)).date()
            param_date = date_limite.strftime("%Y-%m-%d")
            query += " AND DATE(c. heure_prevue) >= CAST(%s AS date)"
            params.append(param_date)
        if chauffeur_id:
            query += " AND c.chauffeur_id = %s"
            params.append(chauffeur_id)
        if role == "chauffeur":
            query += " AND c.visible_chauffeur = true"
        query += """
            ORDER BY
                DATE(c.heure_prevue) ASC,
                COALESCE(
                    c.heure_pec_prevue:: time,
                    (c.heure_prevue AT TIME ZONE 'Europe/Paris')::time
                ) ASC
        """
        query += f" LIMIT {limit}"
        try:
            cursor.execute(query, params)
            courses = cursor.fetchall()
        except Exception as e:
            print("get_courses SQL error:", e)
            print("SQL query:", query)
            print("params:", params)
            release_db_connection(conn)
            return []
    except Exception as e:
        print("get_courses error (normalisation):", e)
        release_db_connection(conn)
        return []
    release_db_connection(conn)
    result = []
    for course in courses:
        result.append({
            'id': course. get('id'),
            'chauffeur_id': course.get('chauffeur_id'),
            'nom_client': course.get('nom_client'),
            'telephone_client': course.get('telephone_client'),
            'adresse_pec': course.get('adresse_pec'),
            'lieu_depose': course.get('lieu_depose'),
            'heure_prevue': course.get('heure_prevue'),
            'heure_pec_prevue': course.get('heure_pec_prevue'),
            'temps_trajet_minutes': course.get('temps_trajet_minutes'),
            'heure_depart_calculee': course.get('heure_depart_calculee'),
            'type_course': course.get('type_course'),
            'tarif_estime':  course.get('tarif_estime'),
            'km_estime': course.get('km_estime'),
            'commentaire': course.get('commentaire'),
            'commentaire_chauffeur': course.get('commentaire_chauffeur'),
            'statut': course.get('statut'),
            'date_creation': course.get('date_creation'),
            'date_confirmation': course.get('date_confirmation'),
            'date_pec': course.get('date_pec'),
            'date_depose': course.get('date_depose'),
            'created_by': course.get('created_by'),
            'client_regulier_id': course. get('client_regulier_id'),
            'chauffeur_name': course.get('chauffeur_name'),
            'visible_chauffeur':  course.get('visible_chauffeur', True),
            'km_reel': course.get('km_reel'),
            'tarif_reel': course.get('tarif_reel')
        })
    return result

def distribute_courses_for_date(date_str):
    try:
        conn = get_db_connection()
        if not conn:
            return {'success': False, 'count': 0, 'message': "Erreur de connexion"}
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE courses
            SET visible_chauffeur = true
            WHERE DATE(heure_prevue AT TIME ZONE 'Europe/Paris') = %s
            AND visible_chauffeur = false
        ''', (date_str,))
        count = cursor.rowcount
        conn.commit()
        release_db_connection(conn)
        return {
            'success': True,
            'count': count,
            'message': f"✅ {count} course(s) du {date_str} distribuée(s) !"
        }
    except Exception as e:
        return {
            'success': False,
            'count': 0,
            'message': f"❌ Erreur :  {str(e)}"
        }

def export_week_to_excel(week_start_date):
    try:
        from io import BytesIO
        from openpyxl. styles import Font, PatternFill, Alignment
        conn = get_db_connection()
        if not conn:
            return {'success': False, 'error': 'Erreur de connexion'}
        cursor = conn. cursor()
        week_end_date = week_start_date + timedelta(days=6)
        cursor.execute('''
            SELECT 
                u.full_name,
                c.nom_client,
                c.telephone_client,
                c.adresse_pec,
                c.lieu_depose,
                c.heure_prevue,
                c.heure_pec_prevue,
                c.type_course,
                c.tarif_estime,
                c.km_estime,
                c.statut,
                c.commentaire,
                c.commentaire_chauffeur,
                c.date_confirmation,
                c.date_pec,
                c.date_depose
            FROM courses c
            JOIN users u ON c.chauffeur_id = u.id
            WHERE c.heure_prevue >= %s AND c.heure_prevue < %s + INTERVAL '1 day'
            ORDER BY c.heure_prevue
        ''', (week_start_date, week_end_date))
        rows = cursor.fetchall()
        release_db_connection(conn)
        if not rows or len(rows) == 0:
            return {
                'success': False,
                'error': f'Aucune course trouvée pour la semaine du {week_start_date.strftime("%d/%m/%Y")} au {week_end_date. strftime("%d/%m/%Y")}'
            }
        data = []
        for row in rows:
            data.append({
                'Chauffeur': row['full_name'],
                'Client': row['nom_client'],
                'Téléphone': row['telephone_client'],
                'Adresse PEC': row['adresse_pec'],
                'Lieu dépose': row['lieu_depose'],
                'Date/Heure': row['heure_prevue'],
                'Heure PEC': row['heure_pec_prevue'],
                'Type':  row['type_course'],
                'Tarif (€)': row['tarif_estime'],
                'Km':  row['km_estime'],
                'Statut': row['statut'],
                'Commentaire secrétaire': row['commentaire'],
                'Commentaire chauffeur': row['commentaire_chauffeur'],
                'Date confirmation': row['date_confirmation'],
                'Date PEC réelle': row['date_pec'],
                'Date dépose':  row['date_depose']
            })
        df = pd.DataFrame(data)
        date_columns = ['Date/Heure', 'Date confirmation', 'Date PEC réelle', 'Date dépose']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Courses')
            worksheet = writer.sheets['Courses']
            for cell in worksheet[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.alignment = Alignment(horizontal="center")
            for i, col in enumerate(df.columns):
                max_length = max(df[col].astype(str).apply(len).max(), len(col)) + 2
                col_letter = chr(65 + i)
                worksheet. column_dimensions[col_letter]. width = min(max_length, 50)
        buffer.seek(0)
        excel_data = buffer.getvalue()
        week_number = week_start_date.isocalendar()[1]
        year = week_start_date.year
        filename = f"semaine_{week_number: 02d}_{year}. xlsx"
        return {
            'success': True,
            'excel_data': excel_data,
            'count': len(df),
            'filename': filename
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

def purge_week_courses(week_start_date):
    try:
        conn = get_db_connection()
        if not conn:
            return {'success': False, 'error': 'Erreur de connexion'}
        cursor = conn.cursor()
        week_end_date = week_start_date + timedelta(days=6)
        cursor.execute('''
            SELECT id FROM courses
            WHERE heure_prevue >= %s AND heure_prevue < %s + INTERVAL '1 day'
        ''', (week_start_date, week_end_date))
        course_ids = [row['id'] for row in cursor.fetchall()]
        if not course_ids:
            release_db_connection(conn)
            return {'success': True, 'count': 0}
        cursor.execute('''
            DELETE FROM courses
            WHERE id = ANY(%s)
        ''', (course_ids,))
        count = cursor.rowcount
        conn.commit()
        release_db_connection(conn)
        return {'success': True, 'count': count}
    except Exception as e:
        return {'success':  False, 'error': str(e)}

def update_course_status(course_id, new_status, km_reel=None, tarif_reel=None):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    now_paris = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    timestamp_field = {
        'confirmee': 'date_confirmation',
        'pec': 'date_pec',
        'deposee': 'date_depose'
    }
    if new_status == 'deposee' and km_reel is not None and tarif_reel is not None:
        cursor.execute('''
            UPDATE courses
            SET 
                statut = %s, 
                date_depose = %s,
                km_reel = %s,
                tarif_reel = %s
            WHERE id = %s
        ''', (new_status, now_paris, km_reel, tarif_reel, course_id))
    elif new_status in timestamp_field:
        cursor.execute(f'''
            UPDATE courses
            SET statut = %s, {timestamp_field[new_status]} = %s
            WHERE id = %s
        ''', (new_status, now_paris, course_id))
    else:
        cursor.execute('''
            UPDATE courses
            SET statut = %s
            WHERE id = %s
        ''', (new_status, course_id))
    conn.commit()
    release_db_connection(conn)
    return True

def update_commentaire_chauffeur(course_id, commentaire):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE courses
        SET commentaire_chauffeur = %s
        WHERE id = %s
    ''', (commentaire, course_id))
    conn.commit()
    release_db_connection(conn)
    return True

def update_heure_pec_prevue(course_id, nouvelle_heure):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE courses
        SET heure_pec_prevue = %s
        WHERE id = %s
    ''', (nouvelle_heure, course_id))
    conn.commit()
    release_db_connection(conn)
    return True

def delete_course(course_id):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM courses
        WHERE id = %s
    ''', (course_id,))
    conn.commit()
    release_db_connection(conn)
    return True

def update_course_details(course_id, nouvelle_heure_pec, nouveau_chauffeur_id):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE courses
        SET heure_pec_prevue = %s, chauffeur_id = %s
        WHERE id = %s
    ''', (nouvelle_heure_pec, nouveau_chauffeur_id, course_id))
    conn.commit()
    release_db_connection(conn)
    return True

def create_user(username, password, role, full_name):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    hashed_password = hash_password(password)
    try:
        cursor.execute('''
            INSERT INTO users (username, password_hash, role, full_name)
            VALUES (%s, %s, %s, %s)
        ''', (username, hashed_password, role, full_name))
        conn.commit()
        release_db_connection(conn)
        return True
    except psycopg2.IntegrityError:
        release_db_connection(conn)
        return False

def delete_user(user_id):
    conn = get_db_connection()
    if not conn:
        return False, "Erreur de connexion"
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
        admin_count = get_scalar_result(cursor)
        cursor.execute("SELECT role FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if user and user['role'] == 'admin' and admin_count <= 1:
            release_db_connection(conn)
            return False, "Impossible de supprimer le dernier administrateur"
        cursor.execute('DELETE FROM users WHERE id = %s', (user_id,))
        conn.commit()
        release_db_connection(conn)
        return True, "Utilisateur supprimé avec succès"
    except Exception as e:
        release_db_connection(conn)
        return False, f"Erreur:  {str(e)}"

def get_all_users():
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, username, role, full_name, created_at
        FROM users
        ORDER BY role, full_name
    ''')
    users = cursor.fetchall()
    release_db_connection(conn)
    return users

def reassign_course_to_driver(course_id, new_chauffeur_id):
    conn = get_db_connection()
    if not conn:
        return {'success': False, 'error':  'Erreur de connexion'}
    cursor = conn.cursor()
    cursor.execute('''
        SELECT c.chauffeur_id, c.nom_client, u.full_name 
        FROM courses c
        JOIN users u ON c.chauffeur_id = u.id
        WHERE c.id = %s
    ''', (course_id,))
    result = cursor.fetchone()
    if result:
        old_chauffeur_id, nom_client, old_chauffeur_name = result['chauffeur_id'], result['nom_client'], result['full_name']
        cursor.execute('SELECT full_name FROM users WHERE id = %s', (new_chauffeur_id,))
        new_chauffeur_name = get_scalar_result(cursor)
        cursor.execute('''
            UPDATE courses 
            SET chauffeur_id = %s
            WHERE id = %s
        ''', (new_chauffeur_id, course_id))
        conn.commit()
        release_db_connection(conn)
        return {
            'success': True,
            'course_id': course_id,
            'nom_client': nom_client,
            'old_chauffeur_id': old_chauffeur_id,
            'old_chauffeur_name': old_chauffeur_name,
            'new_chauffeur_id': new_chauffeur_id,
            'new_chauffeur_name':  new_chauffeur_name
        }
    else:
        release_db_connection(conn)
        return {'success': False, 'error': 'Course non trouvée'}


# ============================================
# INTERFACES UTILISATEUR
# ============================================

def login_page():
    st.title("Transport DanGE - Planning des courses")
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.subheader("Connexion")
        username = st.text_input("Nom d'utilisateur")
        password = st.text_input("Mot de passe", type="password")
        if st.button("Se connecter", use_container_width=True):
            user = login(username, password)
            if user:
                st.session_state. user = user
                st.rerun()
            else:
                st.error("Nom d'utilisateur ou mot de passe incorrect")
def admin_page():
    st.title("🔧 Administration - Transport DanGE")
    st.markdown(f"**Connecté en tant que :** {st.session_state. user['full_name']} (Admin)")
    
    col_deconnexion, col_refresh = st.columns([1, 6])
    
    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
            if "user" in st.session_state:
                del st.session_state.user
            st.rerun()
    with col_refresh:
        if st.button("🔄 Actualiser"):
            st.rerun()
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Planning Global", "👥 Gestion des Comptes", "📈 Statistiques", "💾 Export"])
    
    with tab1:
        st.subheader("Planning Global de toutes les courses")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            show_all = st.checkbox("Afficher toutes les courses", value=True)
            if not show_all:
                date_filter = st.date_input("Filtrer par date", value=datetime.now())
            else:
                date_filter = None
        with col2:
            chauffeur_filter = st.selectbox("Filtrer par chauffeur", ["Tous"] + [c['full_name'] for c in get_chauffeurs()])
        with col3:
            statut_filter = st.selectbox("Filtrer par statut", ["Tous", "Nouvelle", "Confirmée", "PEC", "Déposée"])
        with col4:
            st.metric("Total courses", len(get_courses()))
        
        chauffeur_id = None
        if chauffeur_filter != "Tous":
            chauffeurs = get_chauffeurs()
            for c in chauffeurs:
                if c['full_name'] == chauffeur_filter:
                    chauffeur_id = c['id']
                    break
        
        date_filter_str = None
        if not show_all and date_filter: 
            date_filter_str = date_filter.strftime('%Y-%m-%d')
        
        courses = get_courses(chauffeur_id=chauffeur_id, date_filter=date_filter_str)
        
        st.info(f"📊 {len(courses)} course(s) trouvée(s)")
        
        if courses: 
            for course in courses:
                statut_mapping = {'Nouvelle': 'nouvelle', 'Confirmée': 'confirmee', 'PEC': 'pec', 'Déposée': 'deposee'}
                
                if statut_filter != "Tous":
                    statut_reel = statut_mapping. get(statut_filter, statut_filter. lower())
                    if course['statut']. lower() != statut_reel. lower():
                        continue
                
                statut_colors = {
                    'nouvelle': '🔵',
                    'confirmee': '🟡',
                    'pec':  '🔴',
                    'deposee': '🟢'
                }
                
                date_fr = format_date_fr(course['heure_prevue'])
                heure_affichage = course. get('heure_pec_prevue', extract_time_str(course['heure_prevue']))
                titre_course = f"{statut_colors. get(course['statut'], '⚪')} {date_fr} {heure_affichage} - {course['nom_client']} ({course['chauffeur_name']})"
                
                with st.expander(titre_course):
                    col1, col2 = st. columns(2)
                    with col1:
                        st. write(f"**Client :** {course['nom_client']}")
                        st.write(f"**Téléphone :** {course['telephone_client']}")
                        st.write(f"**📅 Date PEC :** {format_date_fr(course['heure_prevue'])}")
                        if course. get('heure_pec_prevue'):
                            st.success(f"⏰ **Heure PEC prévue :  {course['heure_pec_prevue']}**")
                        st.write(f"**PEC :** {course['adresse_pec']}")
                        st.write(f"**Dépose :** {course['lieu_depose']}")
                        st.write(f"**Type :** {course['type_course']}")
                    with col2:
                        st. write(f"**Chauffeur :** {course['chauffeur_name']}")
                        st.write(f"**Tarif estimé :** {course['tarif_estime']}€")
                        st.write(f"**Km estimé :** {course['km_estime']} km")
                        st.write(f"**Statut :** {course['statut']. upper()}")
                        if course['commentaire']:
                            st. write(f"**Commentaire secrétaire :** {course['commentaire']}")
                    
                    if course.get('commentaire_chauffeur'):
                        st. warning(f"💭 **Commentaire chauffeur** : {course['commentaire_chauffeur']}")
                    
                    if course['date_confirmation']:
                        st. info(f"✅ Confirmée le : {format_datetime_fr(course['date_confirmation'])}")
                    if course['date_pec']: 
                        st.info(f"📍 PEC effectuée le : {format_datetime_fr(course['date_pec'])}")
                    if course['date_depose']:
                        st.success(f"🏁 Déposée le :  {format_datetime_fr(course['date_depose'])}")
        else:
            st.info("Aucune course pour cette sélection")
    
    with tab2:
        st.subheader("Gestion des comptes utilisateurs")
        
        with st.expander("➕ Créer un nouveau compte"):
            new_username = st.text_input("Nom d'utilisateur", key="new_user")
            new_password = st. text_input("Mot de passe", type="password", key="new_pass")
            new_full_name = st.text_input("Nom complet", key="new_name")
            new_role = st.selectbox("Rôle", ["chauffeur", "secretaire", "admin"], key="new_role")
            
            if st.button("Créer le compte"):
                if new_username and new_password and new_full_name:
                    if create_user(new_username, new_password, new_role, new_full_name):
                        st.success(f"Compte créé avec succès pour {new_full_name}")
                        st.rerun()
                    else:
                        st.error("Ce nom d'utilisateur existe déjà")
                else: 
                    st.warning("Veuillez remplir tous les champs")
        
        st.markdown("### Liste des utilisateurs")
        users = get_all_users()
        
        for user in users:
            role_icons = {
                'admin': '👑',
                'secretaire': '📝',
                'chauffeur': '🚖'
            }
            
            col1, col2 = st. columns([4, 1])
            with col1:
                st.markdown(f"{role_icons. get(user['role'], '👤')} **{user['full_name']}** - {user['username']} ({user['role']})")
            with col2:
                if user['id'] != st.session_state.user['id']:
                    if st.button("🗑️ Supprimer", key=f"delete_{user['id']}"):
                        success, message = delete_user(user['id'])
                        if success:
                            st.success(message)
                            st.rerun()
                        else:
                            st.error(message)
                else:
                    st.info("(Vous)")
    
    with tab3:
        st.subheader("📈 Statistiques")
        
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                cursor.execute("SELECT COUNT(*) FROM courses")
                total_courses = get_scalar_result(cursor)
                st.metric("Total courses", total_courses)
            
            with col2:
                cursor.execute("SELECT COUNT(*) FROM courses WHERE statut = 'deposee'")
                courses_terminees = get_scalar_result(cursor)
                st.metric("Courses terminées", courses_terminees)
            
            with col3:
                cursor.execute("SELECT COUNT(*) FROM courses WHERE statut IN ('nouvelle', 'confirmee', 'pec')")
                courses_en_cours = get_scalar_result(cursor)
                st.metric("Courses en cours", courses_en_cours)
            
            with col4:
                cursor. execute("SELECT SUM(tarif_estime) FROM courses WHERE statut = 'deposee'")
                ca_total = get_scalar_result(cursor) or 0
                st.metric("CA réalisé", f"{ca_total:.2f}€")
            
            release_db_connection(conn)
    
    with tab4:
        st.subheader("💾 Export des données")
        st.write("Exporter les courses en CSV pour analyse ou comptabilité")
        
        export_date_debut = st.date_input("Date de début", value=datetime.now() - timedelta(days=30))
        export_date_fin = st.date_input("Date de fin", value=datetime.now())
        
        if st. button("Exporter en CSV"):
            conn = get_db_connection()
            if conn:
                query = '''
                    SELECT 
                        c.id,
                        c.heure_prevue as "Date/Heure",
                        u.full_name as "Chauffeur",
                        c. nom_client as "Client",
                        c.telephone_client as "Téléphone",
                        c.adresse_pec as "Adresse PEC",
                        c.lieu_depose as "Lieu dépose",
                        c.type_course as "Type",
                        c.tarif_estime as "Tarif",
                        c.km_estime as "Km",
                        c.statut as "Statut",
                        c. date_confirmation as "Date confirmation",
                        c.date_pec as "Date PEC",
                        c.date_depose as "Date dépose"
                    FROM courses c
                    JOIN users u ON c.chauffeur_id = u.id
                    WHERE DATE(c.heure_prevue) BETWEEN %s AND %s
                    ORDER BY c.heure_prevue
                '''
                df = pd.read_sql_query(query, conn, params=(export_date_debut, export_date_fin))
                release_db_connection(conn)
                
                csv = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 Télécharger le CSV",
                    data=csv,
                    file_name=f"courses_export_{export_date_debut}_{export_date_fin}.csv",
                    mime="text/csv"
                )


def secretaire_page():
    st.title("📝 Secrétariat - Planning des courses")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Secrétaire)")
    
    col_deconnexion, col_refresh = st.columns([1, 6])
    
    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
            if "user" in st.session_state:
                del st.session_state.user
            st. rerun()
    with col_refresh:
        if st.button("🔄 Actualiser"):
            st.rerun()
    
    st.markdown("---")
    
    st.info("🔧 **Interface Secrétaire** : Conservez votre code existant pour cette fonction complète avec tous les onglets (Nouvelle Course, Planning Global, Planning Semaine, Planning du Jour, Assistant)")


def chauffeur_page():
    if 'user' not in st.session_state:
        st.rerun()
        return
    
    count = st_autorefresh(interval=30000, key="chauffeur_autorefresh")
    
    st.title("🚖 Mes courses")
    st.markdown(f"**Connecté en tant que :** {st.session_state. user['full_name']} (Chauffeur)")
    
    NOTIFICATION_SOUND_BASE64 = """UklGRiQEAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQAEAAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//AAD//wAA//8AAP//"""
    
    unread_count = get_unread_count(st.session_state.user['id'])
    
    if unread_count > 0:
        if 'last_notif_count' not in st.session_state:
            st.session_state.last_notif_count = 0
        
        if unread_count > st.session_state.last_notif_count:
            st.markdown(f"""
                <audio autoplay>
                    <source src="data:audio/wav;base64,{NOTIFICATION_SOUND_BASE64}" type="audio/wav">
                </audio>
            """, unsafe_allow_html=True)
            st.session_state.last_notif_count = unread_count
    else:
        st.session_state.last_notif_count = 0
    
    if unread_count > 0:
        st.markdown(f"""
        <div style="background:  linear-gradient(135deg, #FF4444 0%, #CC0000 100%); 
                    color: white; padding: 15px 25px; 
                    border-radius: 30px; display: inline-block; font-weight: bold;
                    margin-bottom: 20px; animation: pulse 2s infinite;
                    box-shadow: 0 4px 15px rgba(255,68,68,0.4);">
            🔔 {unread_count} nouvelle(s) notification(s) !
        </div>
        <style>
        @keyframes pulse {{{{
            0% {{{{ opacity: 1; transform: scale(1); }}}}
            50% {{{{ opacity: 0.8; transform: scale(1.05); }}}}
            100% {{{{ opacity: 1; transform: scale(1); }}}}
        }}}}
        </style>
        """, unsafe_allow_html=True)
        
        with st.expander("📋 Voir les notifications", expanded=True):
            notifications = get_unread_notifications(st.session_state.user['id'])
            
            for notif in notifications:
                icon = {
                    'nouvelle_course': '🆕',
                    'modification':  '✏️',
                    'changement_chauffeur': '🔄',
                    'annulation': '❌'
                }.get(notif['type'], '📢')
                
                st.info(f"{icon} **{notif['message']}**")
                
                if notif. get('nom_client'):
                    heure = notif. get('heure_pec_prevue', 'N/A')
                    st. caption(f"👤 {notif['nom_client']} | ⏰ {heure}")
                    st.caption(f"📍 {notif. get('adresse_pec', 'N/A')} → {notif.get('lieu_depose', 'N/A')}")
            
            if st.button("✅ Marquer tout comme lu", use_container_width=True):
                mark_notifications_as_read(st.session_state.user['id'])
                st.rerun()
    
    col_deconnexion, col_refresh = st.columns([1, 6])
    
    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
            if "user" in st.session_state:
                del st. session_state.user
            st.rerun()
    
    with col_refresh:
        if st.button("🔄 Actualiser (auto:  30s)", use_container_width=True):
            st.rerun()
    
    st.markdown("---")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        show_all_chauff = st.checkbox("Toutes mes courses", value=False)
        if not show_all_chauff: 
            date_filter = st.date_input("Date", value=datetime.now())
        else:
            date_filter = None
    
    date_filter_str = None
    if not show_all_chauff and date_filter:
        date_filter_str = date_filter.strftime('%Y-%m-%d')
    
    courses = get_courses(chauffeur_id=st.session_state.user['id'], date_filter=date_filter_str, role='chauffeur')
    
    with col2:
        st.metric("Mes courses", len([c for c in courses if c['statut'] != 'deposee']))
    with col3:
        st.metric("Terminées", len([c for c in courses if c['statut'] == 'deposee']))
    
    if not courses:
        st.info("Aucune course")
    else:
        for course in courses:
            statut_colors = {
                'nouvelle': '🔵',
                'confirmee': '🟡',
                'pec': '🔴',
                'deposee':  '🟢'
            }
            
            statut_text = {
                'nouvelle': 'NOUVELLE',
                'confirmee':  'CONFIRMÉE',
                'pec': 'PRISE EN CHARGE',
                'deposee': 'TERMINÉE'
            }
            
            date_fr = format_date_fr(course['heure_prevue'])
            heure_affichage = course. get('heure_pec_prevue', extract_time_str(course['heure_prevue']))
            titre = f"{statut_colors. get(course['statut'], '⚪')} {date_fr} {heure_affichage} - {course['nom_client']} - {statut_text. get(course['statut'], course['statut']. upper())}"
            
            with st.expander(titre):
                col1, col2 = st. columns(2)
                with col1:
                    st.write(f"**Client :** {course['nom_client']}")
                    st.write(f"**Tel :** {course['telephone_client']}")
                    st.write(f"**📅 Date :** {date_fr}")
                    
                    if course. get('heure_pec_prevue'):
                        st.success(f"⏰ **Heure PEC :  {course['heure_pec_prevue']}**")
                    st.write(f"**PEC :** {course['adresse_pec']}")
                
                with col2:
                    st.write(f"**Dépose :** {course['lieu_depose']}")
                    st. write(f"**Type :** {course['type_course']}")
                    st.write(f"**Tarif :** {course['tarif_estime']}€")
                    st.write(f"**Km :** {course['km_estime']} km")
                
                if course['date_confirmation']:
                    st. caption(f"✅ Confirmée :  {format_datetime_fr(course['date_confirmation'])}")
                if course['date_pec']: 
                    st.info(f"📍 **PEC :  {extract_time_str(course['date_pec'])}**")
                if course['date_depose']:
                    st.caption(f"🏁 Déposée : {format_datetime_fr(course['date_depose'])}")
                
                if course['commentaire']:
                    st.info(f"💬 **Secrétaire :** {course['commentaire']}")
                
                st.markdown("---")
                st.markdown("**💭 Commentaire**")
                
                if course.get('commentaire_chauffeur'):
                    st. success(f"📝 {course['commentaire_chauffeur']}")
                
                new_comment = st.text_area(
                    "Ajouter/modifier",
                    value=course.get('commentaire_chauffeur', ''),
                    key=f"comment_{course['id']}",
                    height=80
                )
                
                if st.button("💾 Enregistrer", key=f"save_comment_{course['id']}"):
                    update_commentaire_chauffeur(course['id'], new_comment)
                    st. rerun()
                
                st.markdown("---")
                
                if course['statut'] == 'nouvelle':
                    if st.button("✅ Confirmer", key=f"confirm_{course['id']}", use_container_width=True):
                        update_course_status(course['id'], 'confirmee')
                        st.rerun()
                
                elif course['statut'] == 'confirmee':
                    if st.button("📍 PEC", key=f"pec_{course['id']}", use_container_width=True):
                        update_course_status(course['id'], 'pec')
                        st.rerun()
                
                elif course['statut'] == 'pec': 
                    st.markdown("**📊 Km & Tarif réels**")
                    
                    col_km, col_tarif = st.columns(2)
                    with col_km:
                        km_reel = st.number_input(
                            "Km réels", 
                            min_value=0.0, 
                            step=1.0, 
                            value=float(course['km_estime']),
                            key=f"km_{course['id']}"
                        )
                    with col_tarif:
                        tarif_reel = st. number_input(
                            "Tarif réel (€)", 
                            min_value=0.0, 
                            step=1.0, 
                            value=float(course['tarif_estime']),
                            key=f"tarif_{course['id']}"
                        )
                    
                    if st. button("🏁 Déposé", key=f"depose_{course['id']}", use_container_width=True):
                        update_course_status(course['id'], 'deposee', km_reel, tarif_reel)
                        st. rerun()
                
                elif course['statut'] == 'deposee':
                    st. success("✅ Course terminée")
                    
                    if course.get('km_reel') or course.get('tarif_reel'):
                        st.caption(f"**Km réels :** {course. get('km_reel', course['km_estime'])} km")
                        st. caption(f"**Tarif réel :** {course.get('tarif_reel', course['tarif_estime'])}€")


def main():
    init_db()
    
    if 'user' not in st. session_state:
        login_page()
    else:
        if st.session_state.user['role'] == 'admin':
            admin_page()
        elif st.session_state.user['role'] == 'secretaire':
            secretaire_page()
        elif st.session_state.user['role'] == 'chauffeur':
            chauffeur_page()


if __name__ == "__main__":
    main()
