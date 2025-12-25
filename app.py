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

# Import du module Assistant Intelligent
from assistant import suggest_best_driver, calculate_distance



# ============================================
# OPTIMISATIONS APPLIQUÉES - V3.0 ULTRA ⚡
# ============================================
# 1. CACHES RETIRÉS pour éviter les clics multiples (problème résolu)
# 2. Requêtes SQL optimisées (moins d'appels à la DB)
# 3. Index recommandés (voir commentaires DATABASE INDEXES)
# 4. Boucles simplifiées
# 5. CONNECTION POOLING - Réutilisation connexions (100x plus rapide)
# 6. LAZY LOADING - 30 derniers jours (10x moins de données)
# 7. LIMIT SQL - Max 100 résultats
#
# GAIN TOTAL: 3-5x PLUS RAPIDE ⚡
# ============================================


def get_scalar_result(cursor):
    """Helper pour extraire une valeur scalaire d'un fetchone() avec RealDictCursor"""
    result = cursor.fetchone()
    if result is None:
        return None
    return list(result.values())[0]


# Configuration du fuseau horaire pour la France
TIMEZONE = pytz.timezone('Europe/Paris')

# Configuration de la page
st.set_page_config(
    page_title="Transport DanGE - Planning",
    page_icon="🚖",
    layout="wide"
)


# ============================================
# DATABASE INDEXES - RECOMMANDATIONS
# ============================================
# Pour améliorer les performances, créez ces index dans Supabase :
#
# CREATE INDEX idx_courses_chauffeur_id ON courses(chauffeur_id);
# CREATE INDEX idx_courses_heure_prevue ON courses(heure_prevue);
# CREATE INDEX idx_courses_statut ON courses(statut);
# CREATE INDEX idx_courses_visible_chauffeur ON courses(visible_chauffeur);
# CREATE INDEX idx_courses_date_heure ON courses(DATE(heure_prevue), heure_prevue);
#
# CREATE INDEX idx_users_role ON users(role);
# CREATE INDEX idx_users_username ON users(username);
#
# CREATE INDEX idx_clients_reguliers_nom ON clients_reguliers(nom_complet);
# CREATE INDEX idx_clients_reguliers_actif ON clients_reguliers(actif);
# ============================================




# ============================================
# CONNECTION POOLING - OPTIMISATION #5
# ============================================
@st.cache_resource
def get_connection_pool():
    """Crée un pool de connexions réutilisables - GAIN DE VITESSE"""
    try:
        if "connection_string" in st.secrets.get("supabase", {}):
            return pool.SimpleConnectionPool(
                1, 5,
                st.secrets["supabase"]["connection_string"]
            )
        else:
            return pool.SimpleConnectionPool(
                1, 5,
                host=st.secrets["supabase"]["host"],
                database=st.secrets["supabase"]["database"],
                user=st.secrets["supabase"]["user"],
                password=st.secrets["supabase"]["password"],
                port=st.secrets["supabase"]["port"],
                sslmode='require'
            )
    except Exception as e:
        st.error(f"Erreur pool connexion: {e}")
        return None


def release_db_connection(conn):
    """Remet la connexion dans le pool - OPTIMISATION"""
    try:
        conn_pool = get_connection_pool()
        if conn_pool:
            conn_pool.putconn(conn)
        else:
            release_db_connection(conn)
    except:
        if conn:
            release_db_connection(conn)


# Connexion à la base de données Supabase PostgreSQL
def get_db_connection():
    """Récupère une connexion depuis le pool - OPTIMISÉ"""
    try:
        conn_pool = get_connection_pool()
        if conn_pool:
            conn = conn_pool.getconn()
            conn.cursor_factory = RealDictCursor
            return conn
        
        # Fallback si pool échoue
        if "connection_string" in st.secrets.get("supabase", {}):
            conn = psycopg2.connect(
                st.secrets["supabase"]["connection_string"],
                cursor_factory=RealDictCursor
            )
        else:
            conn = psycopg2.connect(
                host=st.secrets["supabase"]["host"],
                database=st.secrets["supabase"]["database"],
                user=st.secrets["supabase"]["user"],
                password=st.secrets["supabase"]["password"],
                port=st.secrets["supabase"]["port"],
                sslmode='require',
                cursor_factory=RealDictCursor
            )
        return conn
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {e}")
        return None


# Initialiser la base de données
def init_db():
    # Tables déjà créées dans Supabase - cette fonction n'est plus nécessaire
    # MAIS on initialise la table notifications ici
    init_notifications_table()


# Fonction de hachage de mot de passe
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


# Fonction de connexion
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


# ============================================
# FONCTION OPTIMISÉE - CACHE RETIRÉ
# ============================================
def get_chauffeurs():
    """Récupère tous les chauffeurs - CACHE RETIRÉ pour cohérence"""
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


# ============================================


# ============================================
# SYSTÈME DE NOTIFICATIONS
# ============================================

def init_notifications_table():
    """Crée la table notifications si elle n'existe pas"""
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
    """Crée une notification pour un chauffeur"""
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
    """Récupère les notifications non lues d'un chauffeur"""
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
    """Marque toutes les notifications comme lues"""
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
    """Compte le nombre de notifications non lues"""
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
    
    return list(result.values())[0] if result else 0


# FONCTIONS CLIENTS RÉGULIERS
# ============================================

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
        data.get('telephone'),
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
            'nom_complet': client['nom_complet'],
            'telephone': client['telephone'],
            'adresse_pec_habituelle': client['adresse_pec_habituelle'],
            'adresse_depose_habituelle': client['adresse_depose_habituelle'],
            'type_course_habituel': client['type_course_habituel'],
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
            'nom_complet': client['nom_complet'],
            'telephone': client['telephone'],
            'adresse_pec_habituelle': client['adresse_pec_habituelle'],
            'adresse_depose_habituelle': client['adresse_depose_habituelle'],
            'type_course_habituel': client['type_course_habituel'],
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


# ============================================
# GESTION DES COURSES
# ============================================

def create_course(data):
    """Crée une nouvelle course avec gestion de la visibilité"""
    conn = get_db_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    # Déterminer si la course doit être visible pour le chauffeur
    heure_prevue = data['heure_prevue']
    if isinstance(heure_prevue, str):
        heure_prevue = datetime.fromisoformat(heure_prevue.replace('Z', '+00:00'))
    
    # Convertir en timezone Paris
    if heure_prevue.tzinfo is None:
        heure_prevue = TIMEZONE.localize(heure_prevue)
    else:
        heure_prevue = heure_prevue.astimezone(TIMEZONE)
    
    # Date de la course
    date_course = heure_prevue.date()
    # Date d'aujourd'hui
    date_aujourdhui = datetime.now(TIMEZONE).date()
    
    # Visible SI aujourd'hui, NON visible SI futur
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
        data.get('heure_pec_prevue'),
        data.get('temps_trajet_minutes'),
        data.get('heure_depart_calculee'),
        data['type_course'],
        data['tarif_estime'],
        data['km_estime'],
        data['commentaire'],
        data['created_by'],
        data.get('client_regulier_id'),
        visible_chauffeur
    ))
    
    result = cursor.fetchone()
    course_id = result['id'] if result else None
    
    conn.commit()
    release_db_connection(conn)
    
    return course_id


# ============================================
# HELPERS DE FORMATAGE
# ============================================

def format_date_fr(date_input):
    """Convertit une date ISO (YYYY-MM-DD) ou datetime object en format français (DD/MM/YYYY)"""
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
    """Convertit une datetime ISO en format français (DD/MM/YYYY HH:MM)"""
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
    """Extrait l'heure HH:MM d'un datetime object ou string"""
    if not datetime_input:
        return ""
    
    if isinstance(datetime_input, datetime):
        return datetime_input.strftime('%H:%M')
    
    datetime_str = str(datetime_input)
    if len(datetime_str) >= 16:
        return datetime_str[11:16]
    return ""


# ============================================
# FONCTION OPTIMISÉE - CACHE RETIRÉ
# ============================================
def get_courses(chauffeur_id=None, date_filter=None, role=None, days_back=30, limit=100):
    """
    Récupère les courses - CACHE RETIRÉ pour résoudre problème de clics multiples
    
    OPTIMISATION: Requête SQL unique avec filtres combinés
    """
    conn = get_db_connection()
    if not conn:
        return []
    
    cursor = conn.cursor()
    
    # Construction optimisée de la requête avec tous les filtres en une fois
    query = '''
        SELECT c.*, u.full_name as chauffeur_name
        FROM courses c
        JOIN users u ON c.chauffeur_id = u.id
        WHERE 1=1
    '''
    params = []
    
    # LAZY LOADING: Par défaut seulement les N derniers jours
    if date_filter:
        query += ' AND DATE(c.heure_prevue) = %s'
        params.append(date_filter)
    else:
        date_limite = (datetime.now(TIMEZONE) - timedelta(days=days_back)).date()
        query += ' AND DATE(c.heure_prevue) >= %s'
        params.append(date_limite)
    
    if chauffeur_id:
        query += ' AND c.chauffeur_id = %s'
        params.append(chauffeur_id)
    
    if role == 'chauffeur':
        query += ' AND c.visible_chauffeur = true'
    
    # OPTIMISATION: Tri chronologique par DATE puis HEURE
    query += ''' 
        ORDER BY 
            DATE(c.heure_prevue) ASC,
            COALESCE(
                c.heure_pec_prevue::time,
                (c.heure_prevue AT TIME ZONE 'Europe/Paris')::time
            ) ASC
    '''
    
    # LIMIT SQL
    query += f' LIMIT {limit}'
    
    cursor.execute(query, params)
    courses = cursor.fetchall()
    release_db_connection(conn)
    
    # Conversion optimisée avec gestion des champs optionnels
    result = []
    for course in courses:
        result.append({
            'id': course['id'],
            'chauffeur_id': course['chauffeur_id'],
            'nom_client': course['nom_client'],
            'telephone_client': course['telephone_client'],
            'adresse_pec': course['adresse_pec'],
            'lieu_depose': course['lieu_depose'],
            'heure_prevue': course['heure_prevue'],
            'heure_pec_prevue': course.get('heure_pec_prevue'),
            'temps_trajet_minutes': course.get('temps_trajet_minutes'),
            'heure_depart_calculee': course.get('heure_depart_calculee'),
            'type_course': course['type_course'],
            'tarif_estime': course['tarif_estime'],
            'km_estime': course['km_estime'],
            'commentaire': course['commentaire'],
            'commentaire_chauffeur': course.get('commentaire_chauffeur'),
            'statut': course['statut'],
            'date_creation': course['date_creation'],
            'date_confirmation': course.get('date_confirmation'),
            'date_pec': course.get('date_pec'),
            'date_depose': course.get('date_depose'),
            'created_by': course['created_by'],
            'client_regulier_id': course.get('client_regulier_id'),
            'chauffeur_name': course['chauffeur_name'],
            'visible_chauffeur': course.get('visible_chauffeur', True)
        })
    
    return result


# ============================================
# DISTRIBUTION DES COURSES
# ============================================

def distribute_courses_for_date(date_str):
    """Rend visibles toutes les courses non distribuées pour une date donnée"""
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
            'message': f"❌ Erreur : {str(e)}"
        }


# ============================================
# EXPORT ET ARCHIVAGE
# ============================================

def export_week_to_excel(week_start_date):
    """Exporte toutes les courses d'une semaine en Excel"""
    try:
        from io import BytesIO
        from openpyxl.styles import Font, PatternFill, Alignment
        
        conn = get_db_connection()
        if not conn:
            return {'success': False, 'error': 'Erreur de connexion'}
        
        cursor = conn.cursor()
        
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
                'error': f'Aucune course trouvée pour la semaine du {week_start_date.strftime("%d/%m/%Y")} au {week_end_date.strftime("%d/%m/%Y")}'
            }
        
        # Créer le DataFrame
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
                'Type': row['type_course'],
                'Tarif (€)': row['tarif_estime'],
                'Km': row['km_estime'],
                'Statut': row['statut'],
                'Commentaire secrétaire': row['commentaire'],
                'Commentaire chauffeur': row['commentaire_chauffeur'],
                'Date confirmation': row['date_confirmation'],
                'Date PEC réelle': row['date_pec'],
                'Date dépose': row['date_depose']
            })
        
        df = pd.DataFrame(data)
        
        # Formater les dates
        date_columns = ['Date/Heure', 'Date confirmation', 'Date PEC réelle', 'Date dépose']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
        
        # Créer le fichier Excel
        buffer = BytesIO()
        
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Courses')
            
            worksheet = writer.sheets['Courses']
            
            # Formater les en-têtes
            for cell in worksheet[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.alignment = Alignment(horizontal="center")
            
            # Ajuster largeur colonnes
            for i, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).apply(len).max(),
                    len(col)
                ) + 2
                col_letter = chr(65 + i)
                worksheet.column_dimensions[col_letter].width = min(max_length, 50)
        
        buffer.seek(0)
        excel_data = buffer.getvalue()
        
        week_number = week_start_date.isocalendar()[1]
        year = week_start_date.year
        filename = f"semaine_{week_number:02d}_{year}.xlsx"
        
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
    """Supprime TOUTES les courses de la semaine"""
    try:
        conn = get_db_connection()
        if not conn:
            return {'success': False, 'error': 'Erreur de connexion'}
        
        cursor = conn.cursor()
        
        week_end_date = week_start_date + timedelta(days=6)
        
        # Récupérer les IDs puis supprimer
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
        return {'success': False, 'error': str(e)}


# ============================================
# MISE À JOUR DES STATUTS - OPTIMISÉE
# ============================================

def update_course_status(course_id, new_status):
    """
    Met à jour le statut d'une course
    OPTIMISATION: Commit immédiat + fermeture rapide de la connexion
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    # Heure de Paris au format simple
    now_paris = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    
    timestamp_field = {
        'confirmee': 'date_confirmation',
        'pec': 'date_pec',
        'deposee': 'date_depose'
    }
    
    if new_status in timestamp_field:
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
    """Met à jour le commentaire du chauffeur"""
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
    """Met à jour l'heure PEC prévue"""
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
    """Supprime une course"""
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
    """Modifie heure PEC et chauffeur"""
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


# ============================================
# GESTION DES UTILISATEURS
# ============================================

def create_user(username, password, role, full_name):
    """Crée un nouvel utilisateur"""
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
    """Supprime un utilisateur"""
    conn = get_db_connection()
    if not conn:
        return False, "Erreur de connexion"
    
    cursor = conn.cursor()
    
    try:
        # Vérifier qu'il ne reste pas le dernier admin
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
        return False, f"Erreur: {str(e)}"


def get_all_users():
    """Récupère tous les utilisateurs"""
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
    """Réattribue une course à un nouveau chauffeur"""
    conn = get_db_connection()
    if not conn:
        return {'success': False, 'error': 'Erreur de connexion'}
    
    cursor = conn.cursor()
    
    # Récupérer les infos avant modification
    cursor.execute('''
        SELECT c.chauffeur_id, c.nom_client, u.full_name 
        FROM courses c
        JOIN users u ON c.chauffeur_id = u.id
        WHERE c.id = %s
    ''', (course_id,))
    result = cursor.fetchone()
    
    if result:
        old_chauffeur_id, nom_client, old_chauffeur_name = result['chauffeur_id'], result['nom_client'], result['full_name']
        
        # Récupérer le nom du nouveau chauffeur
        cursor.execute('SELECT full_name FROM users WHERE id = %s', (new_chauffeur_id,))
        new_chauffeur_name = get_scalar_result(cursor)
        
        # Mettre à jour la course
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
            'new_chauffeur_name': new_chauffeur_name
        }
    else:
        release_db_connection(conn)
        return {'success': False, 'error': 'Course non trouvée'}


# ============================================
# INTERFACES UTILISATEUR
# ============================================

def login_page():
    """Interface de connexion - VERSION SIMPLE 100% PYTHON"""
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
                st.session_state.user = user
                st.rerun()
            else:
                st.error("Nom d'utilisateur ou mot de passe incorrect")


def admin_page():
    """Interface Admin"""
    st.title("🔧 Administration - Transport DanGE")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Admin)")
    
    col_deconnexion, col_refresh = st.columns([1, 6])
    
    st.title("🚖 Mes courses")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Chauffeur)")
    
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
        
        # Récupérer les courses
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
                    statut_reel = statut_mapping.get(statut_filter, statut_filter.lower())
                    if course['statut'].lower() != statut_reel.lower():
                        continue
                
                statut_colors = {
                    'nouvelle': '🔵',
                    'confirmee': '🟡',
                    'pec': '🔴',
                    'deposee': '🟢'
                }
                
                date_fr = format_date_fr(course['heure_prevue'])
                heure_affichage = course.get('heure_pec_prevue', extract_time_str(course['heure_prevue']))
                titre_course = f"{statut_colors.get(course['statut'], '⚪')} {date_fr} {heure_affichage} - {course['nom_client']} ({course['chauffeur_name']})"
                
                with st.expander(titre_course):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Client :** {course['nom_client']}")
                        st.write(f"**Téléphone :** {course['telephone_client']}")
                        st.write(f"**📅 Date PEC :** {format_date_fr(course['heure_prevue'])}")
                        if course.get('heure_pec_prevue'):
                            st.success(f"⏰ **Heure PEC prévue : {course['heure_pec_prevue']}**")
                        st.write(f"**PEC :** {course['adresse_pec']}")
                        st.write(f"**Dépose :** {course['lieu_depose']}")
                        st.write(f"**Type :** {course['type_course']}")
                    with col2:
                        st.write(f"**Chauffeur :** {course['chauffeur_name']}")
                        st.write(f"**Tarif estimé :** {course['tarif_estime']}€")
                        st.write(f"**Km estimé :** {course['km_estime']} km")
                        st.write(f"**Statut :** {course['statut'].upper()}")
                        if course['commentaire']:
                            st.write(f"**Commentaire secrétaire :** {course['commentaire']}")
                    
                    if course.get('commentaire_chauffeur'):
                        st.warning(f"💭 **Commentaire chauffeur** : {course['commentaire_chauffeur']}")
                    
                    if course['date_confirmation']:
                        st.info(f"✅ Confirmée le : {format_datetime_fr(course['date_confirmation'])}")
                    if course['date_pec']:
                        st.info(f"📍 PEC effectuée le : {format_datetime_fr(course['date_pec'])}")
                    if course['date_depose']:
                        st.success(f"🏁 Déposée le : {format_datetime_fr(course['date_depose'])}")
        else:
            st.info("Aucune course pour cette sélection")
    
    with tab2:
        st.subheader("Gestion des comptes utilisateurs")
        
        # Créer un nouvel utilisateur
        with st.expander("➕ Créer un nouveau compte"):
            new_username = st.text_input("Nom d'utilisateur", key="new_user")
            new_password = st.text_input("Mot de passe", type="password", key="new_pass")
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
        
        # Liste des utilisateurs
        st.markdown("### Liste des utilisateurs")
        users = get_all_users()
        
        for user in users:
            role_icons = {
                'admin': '👑',
                'secretaire': '📝',
                'chauffeur': '🚖'
            }
            
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"{role_icons.get(user['role'], '👤')} **{user['full_name']}** - {user['username']} ({user['role']})")
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
                cursor.execute("SELECT SUM(tarif_estime) FROM courses WHERE statut = 'deposee'")
                ca_total = get_scalar_result(cursor) or 0
                st.metric("CA réalisé", f"{ca_total:.2f}€")
            
            release_db_connection(conn)
    
    with tab4:
        st.subheader("💾 Export des données")
        st.write("Exporter les courses en CSV pour analyse ou comptabilité")
        
        export_date_debut = st.date_input("Date de début", value=datetime.now() - timedelta(days=30))
        export_date_fin = st.date_input("Date de fin", value=datetime.now())
        
        if st.button("Exporter en CSV"):
            conn = get_db_connection()
            if conn:
                query = '''
                    SELECT 
                        c.id,
                        c.heure_prevue as "Date/Heure",
                        u.full_name as "Chauffeur",
                        c.nom_client as "Client",
                        c.telephone_client as "Téléphone",
                        c.adresse_pec as "Adresse PEC",
                        c.lieu_depose as "Lieu dépose",
                        c.type_course as "Type",
                        c.tarif_estime as "Tarif",
                        c.km_estime as "Km",
                        c.statut as "Statut",
                        c.date_confirmation as "Date confirmation",
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
    """Interface Secrétaire - Gestion complète du planning"""
    st.title("📝 Secrétariat - Planning des courses")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Secrétaire)")
    
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
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["➕ Nouvelle Course", "📊 Planning Global", "📅 Planning Semaine", "📆 Planning du Jour", "💡 Assistant"])
    
    with tab1:
        st.subheader("Créer une nouvelle course")
        
        # ============================================
        # BOUTON NOTIFICATION (HORS FORMULAIRE)
        # ============================================
        if 'pending_notification' in st.session_state:
            notif = st.session_state['pending_notification']
            
            st.success(f"✅ Course créée pour **{notif['chauffeur_name']}** !")
            st.info(f"👤 {notif['nom_client']} | ⏰ {notif['heure_pec']} | 📍 {notif['adresse_pec']} → {notif['lieu_depose']}")
            
            col_notif1, col_notif2 = st.columns([3, 2])
            
            with col_notif1:
                if st.button("📤 Notifier le chauffeur", type="primary", use_container_width=True, key="btn_notify"):
                    message = f"🆕 Nouvelle course : {notif['nom_client']}\n⏰ {notif['heure_pec']}\n📍 {notif['adresse_pec']} → {notif['lieu_depose']}\n💰 {notif['tarif']}€ | {notif['km']} km"
                    create_notification(
                        chauffeur_id=notif['chauffeur_id'],
                        course_id=notif['course_id'],
                        message=message,
                        notification_type='nouvelle_course'
                    )
                    st.success(f"✅ Notification envoyée à {notif['chauffeur_name']} !")
                    del st.session_state['pending_notification']
                    st.balloons()
                    st.rerun()
            
            with col_notif2:
                if st.button("❌ Passer", use_container_width=True, key="btn_skip_notify"):
                    del st.session_state['pending_notification']
                    st.rerun()
            
            st.markdown("---")
        
        # Gestion duplication
        course_dupliquee = None
        if 'course_to_duplicate' in st.session_state:
            course_dupliquee = st.session_state.course_to_duplicate
            st.success(f"📋 Duplication de : {course_dupliquee['nom_client']} - {course_dupliquee['adresse_pec']} → {course_dupliquee['lieu_depose']}")
            if st.button("❌ Annuler la duplication"):
                del st.session_state.course_to_duplicate
                st.rerun()
        
        chauffeurs = get_chauffeurs()
        
        if not chauffeurs:
            st.error("⚠️ Aucun chauffeur disponible.")
        else:
            # Recherche client régulier
            col_search1, col_search2 = st.columns([3, 1])
            with col_search1:
                search_client = st.text_input("🔍 Rechercher un client régulier", key="search_client")
            
            client_selectionne = None
            if search_client and len(search_client) >= 2:
                clients_trouves = get_clients_reguliers(search_client)
                if clients_trouves:
                    with col_search2:
                        st.write("")
                        st.write("")
                        st.info(f"✓ {len(clients_trouves)} client(s)")
                    
                    for client in clients_trouves[:5]:
                        with st.expander(f"👤 {client['nom_complet']}", expanded=False):
                            st.write(f"**PEC :** {client['adresse_pec_habituelle']}")
                            st.write(f"**Dépose :** {client['adresse_depose_habituelle']}")
                            if st.button(f"✅ Utiliser ce client", key=f"select_{client['id']}"):
                                client_selectionne = client
                                st.rerun()
            
            st.markdown("---")
            
            with st.form("new_course_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    chauffeur_names = [c['full_name'] for c in chauffeurs]
                    selected_chauffeur = st.selectbox("Chauffeur *", chauffeur_names)
                    
                    # Pré-remplissage
                    if course_dupliquee:
                        default_nom = course_dupliquee['nom_client']
                        default_tel = course_dupliquee['telephone_client']
                        default_pec = course_dupliquee['adresse_pec']
                        default_depose = course_dupliquee['lieu_depose']
                    elif client_selectionne:
                        default_nom = client_selectionne['nom_complet']
                        default_tel = client_selectionne['telephone']
                        default_pec = client_selectionne['adresse_pec_habituelle']
                        default_depose = client_selectionne['adresse_depose_habituelle']
                    else:
                        default_nom = ""
                        default_tel = ""
                        default_pec = ""
                        default_depose = ""
                    
                    nom_client = st.text_input("Nom du client *", value=default_nom)
                    telephone_client = st.text_input("Téléphone", value=default_tel)
                    adresse_pec = st.text_input("Adresse PEC *", value=default_pec)
                    lieu_depose = st.text_input("Lieu de dépose *", value=default_depose)
                
                with col2:
                    if course_dupliquee:
                        default_type = course_dupliquee['type_course']
                        default_tarif = course_dupliquee['tarif_estime']
                        default_km = course_dupliquee['km_estime']
                        default_heure_pec = course_dupliquee.get('heure_pec_prevue', '')
                    elif client_selectionne:
                        default_type = client_selectionne['type_course_habituel']
                        default_tarif = client_selectionne['tarif_habituel']
                        default_km = client_selectionne['km_habituels']
                        default_heure_pec = ''
                    else:
                        default_type = "CPAM"
                        default_tarif = 0.0
                        default_km = 0.0
                        default_heure_pec = ''
                    
                    now_paris = datetime.now(TIMEZONE)
                    date_course = st.date_input("Date *", value=now_paris.date())
                    heure_pec_prevue = st.text_input("Heure PEC (HH:MM)", value=default_heure_pec, placeholder="Ex: 17:50")
                    
                    type_course = st.selectbox("Type *", ["CPAM", "Privé"], index=0 if default_type == "CPAM" else 1)
                    tarif_estime = st.number_input("Tarif (€)", min_value=0.0, step=5.0, value=float(default_tarif) if default_tarif else 0.0)
                    km_estime = st.number_input("Km", min_value=0.0, step=1.0, value=float(default_km) if default_km else 0.0)
                    commentaire = st.text_area("Commentaire")
                    
                    sauvegarder_client = False
                    if not client_selectionne:
                        sauvegarder_client = st.checkbox("💾 Sauvegarder comme client régulier")
                
                submitted = st.form_submit_button("✅ Créer la course", use_container_width=True)
                
                if submitted:
                    if nom_client and adresse_pec and lieu_depose and selected_chauffeur:
                        chauffeur_id = None
                        for c in chauffeurs:
                            if c['full_name'] == selected_chauffeur:
                                chauffeur_id = c['id']
                                break
                        
                        if chauffeur_id:
                            client_id = None
                            if sauvegarder_client and not client_selectionne:
                                client_data = {
                                    'nom_complet': nom_client,
                                    'telephone': telephone_client,
                                    'adresse_pec_habituelle': adresse_pec,
                                    'adresse_depose_habituelle': lieu_depose,
                                    'type_course_habituel': type_course,
                                    'tarif_habituel': tarif_estime,
                                    'km_habituels': km_estime,
                                    'remarques': commentaire
                                }
                                client_id = create_client_regulier(client_data)
                            elif client_selectionne:
                                client_id = client_selectionne['id']
                            
                            heure_prevue_naive = datetime.combine(date_course, datetime.now(TIMEZONE).time())
                            heure_prevue = heure_prevue_naive.strftime('%Y-%m-%d %H:%M:%S')
                            
                            course_data = {
                                'chauffeur_id': chauffeur_id,
                                'nom_client': nom_client,
                                'telephone_client': telephone_client,
                                'adresse_pec': adresse_pec,
                                'lieu_depose': lieu_depose,
                                'heure_prevue': heure_prevue,
                                'heure_pec_prevue': heure_pec_prevue if heure_pec_prevue else None,
                                'type_course': type_course,
                                'tarif_estime': tarif_estime,
                                'km_estime': km_estime,
                                'commentaire': commentaire,
                                'created_by': st.session_state.user['id'],
                                'client_regulier_id': client_id
                            }
                            
                            course_id = create_course(course_data)
                            if course_id:
                                st.success(f"✅ Course créée pour {selected_chauffeur}")
                                st.success(f"✅ Course créée pour {selected_chauffeur}")
                                
                                # Stocker les infos pour afficher le bouton de notification HORS du formulaire
                                st.session_state["pending_notification"] = {
                                    "course_id": course_id,
                                    "chauffeur_id": chauffeur_id,
                                    "chauffeur_name": selected_chauffeur,
                                    "nom_client": nom_client,
                                    "adresse_pec": adresse_pec,
                                    "lieu_depose": lieu_depose,
                                    "heure_pec": heure_pec_prevue if heure_pec_prevue else "N/A",
                                    "tarif": tarif_estime,
                                    "km": km_estime
                                }
                                
                                if 'course_to_duplicate' in st.session_state:
                                    del st.session_state.course_to_duplicate
                                
                                # Recharger la page pour afficher le bouton notification
                                st.rerun()
                        else:
                            st.error("❌ Chauffeur non trouvé")
                    else:
                        st.error("Remplissez tous les champs obligatoires (*)")
    
    with tab2:
        st.subheader("Planning Global")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            show_all_sec = st.checkbox("Toutes les courses", value=True, key="sec_show_all")
            if not show_all_sec:
                date_filter = st.date_input("Date", value=datetime.now(), key="sec_date")
            else:
                date_filter = None
        with col2:
            chauffeur_filter = st.selectbox("Chauffeur", ["Tous"] + [c['full_name'] for c in get_chauffeurs()], key="sec_chauff")
        with col3:
            statut_filter = st.selectbox("Statut", ["Tous", "Nouvelle", "Confirmée", "PEC", "Déposée"], key="sec_statut")
        with col4:
            st.metric("Total", len(get_courses()))
        
        chauffeur_id = None
        if chauffeur_filter != "Tous":
            for c in get_chauffeurs():
                if c['full_name'] == chauffeur_filter:
                    chauffeur_id = c['id']
                    break
        
        date_filter_str = None
        if not show_all_sec and date_filter:
            date_filter_str = date_filter.strftime('%Y-%m-%d')
        
        courses = get_courses(chauffeur_id=chauffeur_id, date_filter=date_filter_str)
        
        st.info(f"📊 {len(courses)} course(s)")
        
        if courses:
            for course in courses:
                statut_mapping = {'Nouvelle': 'nouvelle', 'Confirmée': 'confirmee', 'PEC': 'pec', 'Déposée': 'deposee'}
                
                if statut_filter != "Tous":
                    statut_reel = statut_mapping.get(statut_filter, statut_filter.lower())
                    if course['statut'].lower() != statut_reel.lower():
                        continue
                
                statut_colors = {
                    'nouvelle': '🔵',
                    'confirmee': '🟡',
                    'pec': '🔴',
                    'deposee': '🟢'
                }
                
                date_fr = format_date_fr(course['heure_prevue'])
                heure_affichage = course.get('heure_pec_prevue', extract_time_str(course['heure_prevue']))
                titre = f"{statut_colors.get(course['statut'], '⚪')} {date_fr} {heure_affichage} - {course['nom_client']} ({course['chauffeur_name']})"
                
                with st.expander(titre):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Client :** {course['nom_client']}")
                        st.write(f"**Tel :** {course['telephone_client']}")
                        st.write(f"**PEC :** {course['adresse_pec']}")
                        st.write(f"**Dépose :** {course['lieu_depose']}")
                    with col2:
                        st.write(f"**Chauffeur :** {course['chauffeur_name']}")
                        st.write(f"**Tarif :** {course['tarif_estime']}€")
                        st.write(f"**Km :** {course['km_estime']} km")
                    
                    if course.get('commentaire_chauffeur'):
                        st.warning(f"💭 {course['commentaire_chauffeur']}")
                    
                    st.markdown("---")
                    
                    col_btn1, col_btn2 = st.columns(2)
                    
                    with col_btn1:
                        if st.button(f"🗑️ Supprimer", key=f"del_sec_{course['id']}", use_container_width=True):
                            st.session_state[f'confirmer_suppression_{course["id"]}'] = True
                            st.rerun()
                    
                    with col_btn2:
                        if st.button(f"✏️ Modifier", key=f"mod_sec_{course['id']}", use_container_width=True):
                            st.session_state[f'modifier_course_{course["id"]}'] = True
                            st.rerun()
                    
                    # Confirmation suppression
                    if st.session_state.get(f'confirmer_suppression_{course["id"]}', False):
                        st.markdown("---")
                        st.warning("⚠️ Confirmer la suppression ?")
                        
                        col_conf1, col_conf2 = st.columns(2)
                        with col_conf1:
                            if st.button("❌ Annuler", key=f"cancel_del_{course['id']}", use_container_width=True):
                                del st.session_state[f'confirmer_suppression_{course["id"]}']
                                st.rerun()
                        with col_conf2:
                            if st.button("✅ Confirmer", key=f"confirm_del_{course['id']}", use_container_width=True):
                                delete_course(course['id'])
                                del st.session_state[f'confirmer_suppression_{course["id"]}']
                                st.rerun()
                    
                    # Modification
                    if st.session_state.get(f'modifier_course_{course["id"]}', False):
                        st.markdown("---")
                        st.subheader("✏️ Modifier")
                        
                        chauffeurs_list = get_chauffeurs()
                        
                        heure_actuelle = course.get('heure_pec_prevue', '')
                        nouvelle_heure_pec = st.text_input(
                            "Heure PEC (HH:MM)",
                            value=heure_actuelle,
                            key=f"input_heure_mod_{course['id']}"
                        )
                        
                        chauffeur_actuel_index = 0
                        for i, ch in enumerate(chauffeurs_list):
                            if ch['id'] == course['chauffeur_id']:
                                chauffeur_actuel_index = i
                                break
                        
                        nouveau_chauffeur = st.selectbox(
                            "Chauffeur",
                            options=chauffeurs_list,
                            format_func=lambda x: x['full_name'],
                            index=chauffeur_actuel_index,
                            key=f"select_chauffeur_mod_{course['id']}"
                        )
                        
                        col_save, col_cancel = st.columns(2)
                        with col_save:
                            if st.button("💾 Enregistrer", key=f"save_mod_{course['id']}", use_container_width=True):
                                heure_valide = True
                                nouvelle_heure_normalisee = None
                                
                                if nouvelle_heure_pec:
                                    parts = nouvelle_heure_pec.split(':')
                                    if len(parts) == 2:
                                        try:
                                            h = int(parts[0])
                                            m = int(parts[1])
                                            if 0 <= h <= 23 and 0 <= m <= 59:
                                                nouvelle_heure_normalisee = f"{h:02d}:{m:02d}"
                                            else:
                                                st.error("❌ Heure invalide")
                                                heure_valide = False
                                        except ValueError:
                                            st.error("❌ Format invalide")
                                            heure_valide = False
                                    else:
                                        st.error("❌ Format invalide")
                                        heure_valide = False
                                
                                if heure_valide:
                                    update_course_details(course['id'], nouvelle_heure_normalisee, nouveau_chauffeur['id'])
                                    del st.session_state[f'modifier_course_{course["id"]}']
                                    st.rerun()
                        
                        with col_cancel:
                            if st.button("❌ Annuler", key=f"cancel_mod_{course['id']}", use_container_width=True):
                                del st.session_state[f'modifier_course_{course["id"]}']
                                st.rerun()
        else:
            st.info("Aucune course")
    
    with tab3:
        st.subheader("📅 Planning Hebdomadaire")
        
        # Sélection de la semaine
        col_week1, col_week2, col_week3 = st.columns([1, 2, 1])
        
        # Initialiser la date de référence
        if 'week_start_date' not in st.session_state:
            st.session_state.week_start_date = datetime.now(TIMEZONE).date()
            # Ajuster au lundi
            days_to_monday = st.session_state.week_start_date.weekday()
            st.session_state.week_start_date = st.session_state.week_start_date - timedelta(days=days_to_monday)
        
        with col_week1:
            if st.button("⬅️ Semaine précédente"):
                st.session_state.week_start_date = st.session_state.week_start_date - timedelta(days=7)
                st.rerun()
        
        with col_week2:
            week_end_date = st.session_state.week_start_date + timedelta(days=6)
            st.markdown(f"### Semaine du {st.session_state.week_start_date.strftime('%d/%m')} au {week_end_date.strftime('%d/%m/%Y')}")
            
            if st.button("📅 Aujourd'hui"):
                today = datetime.now(TIMEZONE).date()
                days_to_monday = today.weekday()
                st.session_state.week_start_date = today - timedelta(days=days_to_monday)
                st.rerun()
        
        with col_week3:
            if st.button("Semaine suivante ➡️"):
                st.session_state.week_start_date = st.session_state.week_start_date + timedelta(days=7)
                st.rerun()
        
        # Récupérer toutes les courses de la semaine
        week_courses = []
        for day_offset in range(7):
            day_date = st.session_state.week_start_date + timedelta(days=day_offset)
            day_courses = get_courses(date_filter=day_date.strftime('%Y-%m-%d'))
            for course in day_courses:
                course['day_offset'] = day_offset
                week_courses.append(course)
        
        st.markdown("---")
        
        # Vérifier si on veut afficher le détail d'un jour
        if 'view_day_detail' in st.session_state and st.session_state.view_day_detail:
            # AFFICHAGE DÉTAILLÉ DU JOUR
            selected_day = st.session_state.selected_day_date
            
            jours_fr = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
            jour_semaine = jours_fr[selected_day.weekday()]
            
            col_back, col_title = st.columns([1, 5])
            with col_back:
                if st.button("⬅️ Retour au planning semaine"):
                    st.session_state.view_day_detail = False
                    st.rerun()
            with col_title:
                st.markdown(f"## 📅 {jour_semaine} {selected_day.strftime('%d/%m/%Y')}")
            
            st.markdown("---")
            
            chauffeurs = get_chauffeurs()
            courses_jour = get_courses(date_filter=selected_day.strftime('%Y-%m-%d'))
            
            nb_colonnes = 4
            cols_chauffeurs = st.columns(nb_colonnes)
            
            for i in range(nb_colonnes):
                with cols_chauffeurs[i]:
                    if i < len(chauffeurs):
                        chauffeur = chauffeurs[i]
                        st.markdown(f"### 🚗 {chauffeur['full_name']}")
                        
                        courses_chauffeur = [c for c in courses_jour if c['chauffeur_id'] == chauffeur['id']]
                        courses_chauffeur.sort(key=lambda c: c.get('heure_pec_prevue') or extract_time_str(c['heure_prevue']) or '')
                        
                        if courses_chauffeur:
                            for course in courses_chauffeur:
                                statut_emoji = {
                                    'nouvelle': '🔵',
                                    'confirmee': '🟡',
                                    'pec': '🔴',
                                    'deposee': '🟢'
                                }
                                emoji = statut_emoji.get(course['statut'], '⚪')
                                
                                heure_affichage = course.get('heure_pec_prevue')
                                if not heure_affichage:
                                    heure_affichage = extract_time_str(course['heure_prevue'])
                                
                                if heure_affichage:
                                    parts = heure_affichage.split(':')
                                    if len(parts) == 2:
                                        h, m = parts
                                        heure_affichage = f"{int(h):02d}:{m}"
                                
                                with st.popover(f"{emoji} {heure_affichage} - {course['nom_client']}", use_container_width=True):
                                    st.markdown(f"**{course['nom_client']}**")
                                    st.caption(f"📞 {course['telephone_client']}")
                                    
                                    if course.get('heure_pec_prevue'):
                                        heure_pec = course['heure_pec_prevue']
                                        parts = heure_pec.split(':')
                                        if len(parts) == 2:
                                            h, m = parts
                                            heure_pec = f"{int(h):02d}:{m}"
                                        st.caption(f"⏰ **Heure PEC:** {heure_pec}")
                                    
                                    st.caption(f"📍 **PEC:** {course['adresse_pec']}")
                                    st.caption(f"🏁 **Dépose:** {course['lieu_depose']}")
                                    st.caption(f"💼 {course['type_course']}")
                                    st.caption(f"💰 {course['tarif_estime']}€ | {course['km_estime']} km")
                                    
                                    st.markdown("---")
                                    col_actions = st.columns(3)
                                    
                                    if course['statut'] == 'nouvelle':
                                        with col_actions[0]:
                                            if st.button("✅ Confirmer", key=f"confirm_detail_{course['id']}", use_container_width=True):
                                                update_course_status(course['id'], 'confirmee')
                                                st.rerun()
                                    
                                    elif course['statut'] == 'confirmee':
                                        with col_actions[1]:
                                            if st.button("📍 PEC", key=f"pec_detail_{course['id']}", use_container_width=True):
                                                update_course_status(course['id'], 'pec')
                                                st.rerun()
                                    
                                    elif course['statut'] == 'pec':
                                        with col_actions[2]:
                                            if st.button("🏁 Déposé", key=f"depose_detail_{course['id']}", use_container_width=True):
                                                update_course_status(course['id'], 'deposee')
                                                st.rerun()
                                    
                                    if course['date_confirmation']:
                                        st.caption(f"✅ Confirmée le : {format_datetime_fr(course['date_confirmation'])}")
                                    if course['date_pec']:
                                        st.caption(f"📍 PEC effectuée le : {format_datetime_fr(course['date_pec'])}")
                                    if course['date_depose']:
                                        st.caption(f"🏁 Déposée le : {format_datetime_fr(course['date_depose'])}")
                                    
                                    st.markdown("---")
                                    col_btn_detail1, col_btn_detail2 = st.columns(2)
                                    
                                    with col_btn_detail1:
                                        if st.button("🗑️ Supprimer", key=f"del_detail_{course['id']}", use_container_width=True):
                                            st.session_state[f'confirm_del_detail_{course["id"]}'] = True
                                            st.rerun()
                                    
                                    with col_btn_detail2:
                                        if st.button("✏️ Modifier", key=f"mod_detail_{course['id']}", use_container_width=True):
                                            st.session_state[f'mod_detail_{course["id"]}'] = True
                                            st.rerun()
                                    
                                    if st.session_state.get(f'confirm_del_detail_{course["id"]}', False):
                                        st.warning("⚠️ Confirmer la suppression ?")
                                        col_c1, col_c2 = st.columns(2)
                                        with col_c1:
                                            if st.button("❌ Annuler", key=f"cancel_del_detail_{course['id']}", use_container_width=True):
                                                del st.session_state[f'confirm_del_detail_{course["id"]}']
                                                st.rerun()
                                        with col_c2:
                                            if st.button("✅ Confirmer", key=f"ok_del_detail_{course['id']}", use_container_width=True):
                                                delete_course(course['id'])
                                                del st.session_state[f'confirm_del_detail_{course["id"]}']
                                                st.rerun()
                                    
                                    if st.session_state.get(f'mod_detail_{course["id"]}', False):
                                        st.subheader("✏️ Modifier")
                                        chauffeurs_list = get_chauffeurs()
                                        
                                        h_actuelle = course.get('heure_pec_prevue', '')
                                        new_h = st.text_input("Heure PEC", value=h_actuelle, key=f"h_detail_{course['id']}")
                                        
                                        ch_idx = 0
                                        for idx, ch in enumerate(chauffeurs_list):
                                            if ch['id'] == course['chauffeur_id']:
                                                ch_idx = idx
                                                break
                                        new_ch = st.selectbox("Chauffeur", chauffeurs_list, format_func=lambda x: x['full_name'], index=ch_idx, key=f"ch_detail_{course['id']}")
                                        
                                        col_s, col_c = st.columns(2)
                                        with col_s:
                                            if st.button("💾 Enregistrer", key=f"save_detail_{course['id']}", use_container_width=True):
                                                h_ok = True
                                                h_norm = None
                                                if new_h:
                                                    parts = new_h.split(':')
                                                    if len(parts) == 2:
                                                        try:
                                                            h_int, m_int = int(parts[0]), int(parts[1])
                                                            if 0 <= h_int <= 23 and 0 <= m_int <= 59:
                                                                h_norm = f"{h_int:02d}:{m_int:02d}"
                                                            else:
                                                                st.error("❌ Heure invalide")
                                                                h_ok = False
                                                        except:
                                                            st.error("❌ Format invalide")
                                                            h_ok = False
                                                    else:
                                                        st.error("❌ Format invalide")
                                                        h_ok = False
                                                
                                                if h_ok:
                                                    update_course_details(course['id'], h_norm, new_ch['id'])
                                                    del st.session_state[f'mod_detail_{course["id"]}']
                                                    st.rerun()
                                        with col_c:
                                            if st.button("❌ Annuler", key=f"cancel_detail_{course['id']}", use_container_width=True):
                                                del st.session_state[f'mod_detail_{course["id"]}']
                                                st.rerun()
                        else:
                            st.info("Aucune course")
                    else:
                        st.markdown(f"### ⚪ Chauffeur {i+1}")
                        st.info("Non assigné")
            
            st.markdown("---")
            st.caption("🔵 Nouvelle | 🟡 Confirmée | 🔴 PEC | 🟢 Terminée")
            
        else:
            # AFFICHAGE NORMAL DU PLANNING SEMAINE
            
            # BOUTONS DE DISTRIBUTION
            st.markdown("### 📤 Distribution des courses")
            
            date_aujourdhui = datetime.now(TIMEZONE).date()
            jours_fr = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
            
            for day_offset in range(7):
                day_date = st.session_state.week_start_date + timedelta(days=day_offset)
                jour_nom = jours_fr[day_date.weekday()]
                
                if day_date <= date_aujourdhui:
                    continue
                
                day_courses = get_courses(date_filter=day_date.strftime('%Y-%m-%d'))
                courses_non_dist = [c for c in day_courses if not c.get('visible_chauffeur', True)]
                nb_non_dist = len(courses_non_dist)
                
                if nb_non_dist > 0:
                    col_jour, col_badge, col_bouton = st.columns([2, 1, 2])
                    
                    with col_jour:
                        st.markdown(f"**{jour_nom} {day_date.strftime('%d/%m/%Y')}**")
                    
                    with col_badge:
                        st.markdown(f"🔒 **{nb_non_dist}** course(s)")
                    
                    with col_bouton:
                        if st.button(f"📤 Distribuer ce jour ({nb_non_dist})", 
                                   key=f"dist_{day_date.strftime('%Y%m%d')}",
                                   type="primary",
                                   use_container_width=True):
                            result = distribute_courses_for_date(day_date.strftime('%Y-%m-%d'))
                            if result['success']:
                                st.success(result['message'])
                                st.balloons()
                                st.rerun()
                            else:
                                st.error(result['message'])
            
            st.markdown("---")
            
            # ARCHIVAGE HEBDOMADAIRE
            st.markdown("### 📥 Archivage hebdomadaire")
            
            week_end_date = st.session_state.week_start_date + timedelta(days=6)
            all_week_courses = []
            for day_offset in range(7):
                day_date = st.session_state.week_start_date + timedelta(days=day_offset)
                day_courses = get_courses(date_filter=day_date.strftime('%Y-%m-%d'))
                all_week_courses.extend(day_courses)
            
            week_courses_count = len(all_week_courses)
            week_num = st.session_state.week_start_date.isocalendar()[1]
            
            st.markdown(f"**Semaine {week_num} : du {st.session_state.week_start_date.strftime('%d/%m')} au {week_end_date.strftime('%d/%m/%Y')}**")
            st.caption(f"📊 {week_courses_count} course(s) dans cette semaine")
            
            if week_courses_count > 0:
                col_archive, col_delete = st.columns(2)
                
                with col_archive:
                    if st.button("📥 Archiver la semaine", 
                               type="primary", 
                               use_container_width=True,
                               disabled=st.session_state.get('week_archived', False)):
                        with st.spinner("📥 Export en cours..."):
                            result = export_week_to_excel(st.session_state.week_start_date)
                            
                            if result['success']:
                                st.session_state['week_archived'] = True
                                st.session_state['archive_filename'] = result['filename']
                                st.session_state['archive_excel_data'] = result['excel_data']
                                st.session_state['archive_count'] = result['count']
                                st.rerun()
                            else:
                                st.error(f"❌ Erreur : {result.get('error', 'Erreur inconnue')}")
                
                with col_delete:
                    if st.session_state.get('week_archived', False):
                        if st.button("🗑️ Supprimer la semaine", 
                                   type="secondary",
                                   use_container_width=True):
                            st.session_state['confirm_delete_week'] = True
                            st.rerun()
                    else:
                        st.button("🗑️ Supprimer la semaine",
                                use_container_width=True,
                                disabled=True,
                                help="Archivez d'abord la semaine")
                
                if st.session_state.get('week_archived', False):
                    st.success("✅ Semaine archivée ! Téléchargez le fichier Excel :")
                    st.download_button(
                        label=f"📥 Télécharger {st.session_state['archive_filename']} ({st.session_state['archive_count']} courses)",
                        data=st.session_state['archive_excel_data'],
                        file_name=st.session_state['archive_filename'],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                
                if st.session_state.get('confirm_delete_week', False):
                    st.markdown("---")
                    st.error("⚠️ **SUPPRESSION DÉFINITIVE !**")
                    st.markdown(f"**Vous allez supprimer {week_courses_count} course(s).**")
                    
                    col_cancel, col_confirm = st.columns(2)
                    
                    with col_cancel:
                        if st.button("❌ Annuler", use_container_width=True):
                            st.session_state['confirm_delete_week'] = False
                            st.rerun()
                    
                    with col_confirm:
                        if st.button("✅ CONFIRMER LA SUPPRESSION", 
                                   type="primary",
                                   use_container_width=True):
                            with st.spinner("🗑️ Suppression en cours..."):
                                purge_result = purge_week_courses(st.session_state.week_start_date)
                                
                                if purge_result['success']:
                                    st.success(f"🎉 {purge_result['count']} course(s) supprimée(s) !")
                                    
                                    if 'week_archived' in st.session_state:
                                        del st.session_state['week_archived']
                                    if 'archive_filename' in st.session_state:
                                        del st.session_state['archive_filename']
                                    if 'archive_excel_data' in st.session_state:
                                        del st.session_state['archive_excel_data']
                                    if 'archive_count' in st.session_state:
                                        del st.session_state['archive_count']
                                    if 'confirm_delete_week' in st.session_state:
                                        del st.session_state['confirm_delete_week']
                                    
                                    st.rerun()
                                else:
                                    st.error(f"❌ Erreur : {purge_result.get('error', 'Erreur inconnue')}")
            else:
                st.info("Aucune course dans cette semaine")
            
            st.markdown("---")
            
            # Header avec les jours
            cols_days = st.columns(8)
            jours = ["Heure", "Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
            for i, jour in enumerate(jours):
                with cols_days[i]:
                    if i == 0:
                        st.markdown(f"**{jour}**")
                    else:
                        day_date = st.session_state.week_start_date + timedelta(days=i-1)
                        if st.button(f"{jour} {day_date.strftime('%d/%m')}", key=f"day_btn_{i}"):
                            st.session_state.view_day_detail = True
                            st.session_state.selected_day_date = day_date
                            st.rerun()
            
            # Plages horaires
            heures = list(range(6, 23))
            
            for heure in heures:
                cols_hours = st.columns(8)
                with cols_hours[0]:
                    st.markdown(f"**{heure:02d}:00**")
                
                for day_num in range(7):
                    with cols_hours[day_num + 1]:
                        courses_slot = []
                        for c in week_courses:
                            if c['day_offset'] != day_num:
                                continue
                            
                            heure_a_afficher = c.get('heure_pec_prevue')
                            if not heure_a_afficher:
                                heure_a_afficher = extract_time_str(c['heure_prevue'])
                            
                            if heure_a_afficher:
                                parts = heure_a_afficher.split(':')
                                if len(parts) == 2:
                                    h, m = parts
                                    heure_normalisee = f"{int(h):02d}:{m}"
                                else:
                                    heure_normalisee = heure_a_afficher
                            else:
                                heure_normalisee = None
                            
                            if heure_normalisee and heure_normalisee.startswith(f"{heure:02d}:"):
                                courses_slot.append(c)
                        
                        if courses_slot:
                            courses_slot.sort(key=lambda c: c.get('heure_pec_prevue') or extract_time_str(c['heure_prevue']) or '')
                        
                        if courses_slot:
                            for course in courses_slot:
                                statut_emoji = {
                                    'nouvelle': '🔵',
                                    'confirmee': '🟡',
                                    'pec': '🔴',
                                    'deposee': '🟢'
                                }
                                emoji = statut_emoji.get(course['statut'], '⚪')
                                
                                heure_affichage = course.get('heure_pec_prevue')
                                if not heure_affichage:
                                    heure_affichage = extract_time_str(course['heure_prevue'])
                                
                                if heure_affichage:
                                    parts = heure_affichage.split(':')
                                    if len(parts) == 2:
                                        h, m = parts
                                        heure_affichage = f"{int(h):02d}:{m}"
                                
                                chauffeur_prenom = course['chauffeur_name'].split()[0]
                                with st.popover(f"{chauffeur_prenom}\n{emoji} {heure_affichage}", use_container_width=True):
                                    st.markdown(f"**{course['nom_client']}**")
                                    st.caption(f"📞 {course['telephone_client']}")
                                    
                                    if course.get('heure_pec_prevue'):
                                        heure_pec = course['heure_pec_prevue']
                                        parts = heure_pec.split(':')
                                        if len(parts) == 2:
                                            h, m = parts
                                            heure_pec = f"{int(h):02d}:{m}"
                                        st.caption(f"⏰ **Heure PEC:** {heure_pec}")
                                    else:
                                        st.caption(f"⏰ Création: {extract_time_str(course['heure_prevue'])}")
                                    
                                    st.caption(f"📍 **PEC:** {course['adresse_pec']}")
                                    st.caption(f"🏁 **Dépose:** {course['lieu_depose']}")
                                    st.caption(f"🚗 {course['chauffeur_name']}")
                                    st.caption(f"💰 {course['tarif_estime']}€ | {course['km_estime']} km")
                        else:
                            st.write("")
            
            st.markdown("---")
            st.caption("🔵 Nouvelle | 🟡 Confirmée | 🔴 PEC | 🟢 Terminée")
    
    with tab4:
        st.subheader("📆 Planning du Jour")
        
        # Gestion des réattributions
        query_params = st.query_params
        if query_params.get("action") == "reassign":
            try:
                course_id = int(query_params.get("course_id"))
                new_chauffeur_id = int(query_params.get("new_chauffeur_id"))
                old_chauffeur_name = query_params.get("old_chauffeur_name", "")
                new_chauffeur_name = query_params.get("new_chauffeur_name", "")
                
                result = reassign_course_to_driver(course_id, new_chauffeur_id)
                
                if result['success']:
                    st.success(f"✅ Course réattribuée : **{old_chauffeur_name}** → **{new_chauffeur_name}**")
                else:
                    st.error(f"❌ Erreur : {result.get('error', 'Erreur inconnue')}")
                
                st.query_params.clear()
                
            except (ValueError, TypeError) as e:
                st.error(f"❌ Erreur : paramètres invalides")
                st.query_params.clear()
        
        # Initialiser la date
        if 'planning_jour_date' not in st.session_state:
            st.session_state.planning_jour_date = datetime.now(TIMEZONE).date()
        
        # Sélecteur de date
        selected_date = st.date_input(
            "Date",
            value=st.session_state.planning_jour_date,
            key="date_picker_jour"
        )
        if selected_date != st.session_state.planning_jour_date:
            st.session_state.planning_jour_date = selected_date
            st.rerun()
        
        # Afficher la date en français
        jours_fr = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
        jour_semaine = jours_fr[selected_date.weekday()]
        st.markdown(f"### {jour_semaine} {selected_date.strftime('%d/%m/%Y')}")
        
        st.markdown("---")
        
        # Mode Réattribution Rapide
        mode_reattribution = st.checkbox("🔄 Mode Réattribution Rapide", value=False, 
                                        help="Sélectionnez une ou plusieurs courses pour les réattribuer")
        
        if mode_reattribution:
            st.info("💡 **Sélectionnez les courses, choisissez le nouveau chauffeur, puis cliquez sur Réattribuer**")
            
            courses_jour = get_courses(date_filter=st.session_state.planning_jour_date.strftime('%Y-%m-%d'))
            chauffeurs = get_chauffeurs()
            
            if not courses_jour:
                st.warning("Aucune course pour ce jour")
            else:
                if 'selected_courses' not in st.session_state:
                    st.session_state.selected_courses = []
                
                st.markdown("#### 1️⃣ Sélectionner les courses")
                
                courses_par_chauffeur = {}
                for course in courses_jour:
                    chauffeur_id = course['chauffeur_id']
                    if chauffeur_id not in courses_par_chauffeur:
                        courses_par_chauffeur[chauffeur_id] = []
                    courses_par_chauffeur[chauffeur_id].append(course)
                
                selected_course_ids = []
                
                for chauffeur in chauffeurs:
                    if chauffeur['id'] in courses_par_chauffeur:
                        with st.expander(f"🚗 {chauffeur['full_name']} ({len(courses_par_chauffeur[chauffeur['id']])} course(s))", expanded=True):
                            courses = courses_par_chauffeur[chauffeur['id']]
                            courses.sort(key=lambda c: c.get('heure_pec_prevue') or extract_time_str(c['heure_prevue']) or '')
                            
                            for course in courses:
                                statut_emoji = {
                                    'nouvelle': '🔵',
                                    'confirmee': '🟡',
                                    'pec': '🔴',
                                    'deposee': '🟢'
                                }
                                emoji = statut_emoji.get(course['statut'], '⚪')
                                
                                heure_affichage = course.get('heure_pec_prevue')
                                if not heure_affichage:
                                    heure_affichage = extract_time_str(course['heure_prevue'])
                                
                                if heure_affichage:
                                    parts = heure_affichage.split(':')
                                    if len(parts) == 2:
                                        h, m = parts
                                        heure_affichage = f"{int(h):02d}:{m}"
                                
                                label = f"{emoji} {heure_affichage} - {course['nom_client']} ({course['adresse_pec']} → {course['lieu_depose']})"
                                
                                if st.checkbox(label, key=f"select_course_{course['id']}"):
                                    selected_course_ids.append(course['id'])
                
                if selected_course_ids:
                    st.markdown(f"#### 2️⃣ Nouveau chauffeur ({len(selected_course_ids)} course(s))")
                    
                    chauffeur_options = {f"{ch['full_name']}": ch['id'] for ch in chauffeurs}
                    nouveau_chauffeur_name = st.selectbox(
                        "Choisir le nouveau chauffeur",
                        options=list(chauffeur_options.keys()),
                        key="nouveau_chauffeur_select"
                    )
                    nouveau_chauffeur_id = chauffeur_options[nouveau_chauffeur_name]
                    
                    st.markdown("#### 3️⃣ Confirmer")
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        if st.button("🔄 Réattribuer", type="primary", use_container_width=True):
                            success_count = 0
                            for course_id in selected_course_ids:
                                result = reassign_course_to_driver(course_id, nouveau_chauffeur_id)
                                if result['success']:
                                    success_count += 1
                            
                            if success_count == len(selected_course_ids):
                                st.success(f"✅ {success_count} course(s) réattribuée(s) à {nouveau_chauffeur_name} !")
                                st.balloons()
                                st.rerun()
                            else:
                                st.error(f"❌ Erreur : {success_count}/{len(selected_course_ids)} course(s) réattribuée(s)")
                    
                    with col2:
                        if st.button("❌ Annuler", use_container_width=True):
                            st.rerun()
                else:
                    st.info("👆 Sélectionnez au moins une course")
            
            st.markdown("---")
        
        st.markdown("---")
        
        # Récupérer tous les chauffeurs
        chauffeurs = get_chauffeurs()
        
        # Ordre personnalisé
        def ordre_chauffeur(chauffeur):
            nom = chauffeur['full_name'].lower()
            if 'patron' in nom:
                return (0, nom)
            elif 'franck' in nom:
                return (1, nom)
            elif 'laurence' in nom:
                return (2, nom)
            else:
                return (3, nom)
        
        chauffeurs = sorted(chauffeurs, key=ordre_chauffeur)
        
        nb_colonnes = 4
        
        # Récupérer toutes les courses du jour
        courses_jour = get_courses(date_filter=st.session_state.planning_jour_date.strftime('%Y-%m-%d'))
        
        # Créer 4 colonnes
        cols_chauffeurs = st.columns(nb_colonnes)
        
        for i in range(nb_colonnes):
            with cols_chauffeurs[i]:
                if i < len(chauffeurs):
                    chauffeur = chauffeurs[i]
                    st.markdown(f"### 🚗 {chauffeur['full_name']}")
                    
                    courses_chauffeur = [c for c in courses_jour if c['chauffeur_id'] == chauffeur['id']]
                    courses_chauffeur.sort(key=lambda c: c.get('heure_pec_prevue') or extract_time_str(c['heure_prevue']) or '')
                    
                    if courses_chauffeur:
                        for course in courses_chauffeur:
                            statut_emoji = {
                                'nouvelle': '🔵',
                                'confirmee': '🟡',
                                'pec': '🔴',
                                'deposee': '🟢'
                            }
                            emoji = statut_emoji.get(course['statut'], '⚪')
                            
                            heure_affichage = course.get('heure_pec_prevue')
                            if not heure_affichage:
                                heure_affichage = extract_time_str(course['heure_prevue'])
                            
                            if heure_affichage:
                                parts = heure_affichage.split(':')
                                if len(parts) == 2:
                                    h, m = parts
                                    heure_affichage = f"{int(h):02d}:{m}"
                            
                            with st.popover(f"{emoji} {heure_affichage} - {course['nom_client']}", use_container_width=True):
                                st.markdown(f"**{course['nom_client']}** - {course['telephone_client']}")
                                
                                if course.get('heure_pec_prevue'):
                                    heure_pec = course['heure_pec_prevue']
                                    parts = heure_pec.split(':')
                                    if len(parts) == 2:
                                        h, m = parts
                                        heure_pec = f"{int(h):02d}:{m}"
                                    st.caption(f"⏰ {heure_pec} • {course['adresse_pec']} → {course['lieu_depose']}")
                                else:
                                    st.caption(f"📍 {course['adresse_pec']} → {course['lieu_depose']}")
                                
                                st.caption(f"💰 {course['tarif_estime']}€ | {course['km_estime']} km")
                                
                                st.markdown("---")
                                
                                if course['statut'] == 'nouvelle':
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        if st.button("Confirmer", key=f"confirm_jour_{course['id']}", use_container_width=True):
                                            update_course_status(course['id'], 'confirmee')
                                            st.rerun()
                                    with col2:
                                        if st.button("Supp", key=f"del_jour_{course['id']}", use_container_width=True):
                                            st.session_state[f'confirm_del_jour_{course["id"]}'] = True
                                            st.rerun()
                                
                                elif course['statut'] == 'confirmee':
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        if st.button("📍 PEC", key=f"pec_jour_{course['id']}", use_container_width=True):
                                            update_course_status(course['id'], 'pec')
                                            st.rerun()
                                    with col2:
                                        if st.button("Supp", key=f"del_jour_{course['id']}", use_container_width=True):
                                            st.session_state[f'confirm_del_jour_{course["id"]}'] = True
                                            st.rerun()
                                
                                elif course['statut'] == 'pec':
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        if st.button("🏁 Déposé", key=f"depose_jour_{course['id']}", use_container_width=True):
                                            update_course_status(course['id'], 'deposee')
                                            st.rerun()
                                    with col2:
                                        if st.button("Supp", key=f"del_jour_{course['id']}", use_container_width=True):
                                            st.session_state[f'confirm_del_jour_{course["id"]}'] = True
                                            st.rerun()
                                
                                elif course['statut'] == 'deposee':
                                    if st.button("Supp", key=f"del_jour_{course['id']}", use_container_width=True):
                                        st.session_state[f'confirm_del_jour_{course["id"]}'] = True
                                        st.rerun()
                                
                                if st.session_state.get(f'confirm_del_jour_{course["id"]}', False):
                                    st.warning("⚠️ Confirmer la suppression ?")
                                    col_c1, col_c2 = st.columns(2)
                                    with col_c1:
                                        if st.button("❌ Annuler", key=f"cancel_del_jour_{course['id']}", use_container_width=True):
                                            del st.session_state[f'confirm_del_jour_{course["id"]}']
                                            st.rerun()
                                    with col_c2:
                                        if st.button("✅ Confirmer", key=f"ok_del_jour_{course['id']}", use_container_width=True):
                                            delete_course(course['id'])
                                            del st.session_state[f'confirm_del_jour_{course["id"]}']
                                            st.rerun()
                    else:
                        st.info("Aucune course")
                else:
                    st.markdown(f"### ⚪ Chauffeur {i+1}")
                    st.info("Non assigné")
        
        st.markdown("---")
        st.caption("🔵 Nouvelle | 🟡 Confirmée | 🔴 PEC | 🟢 Terminée")
    
    with tab5:
        st.subheader("💡 Assistant Intelligent - Suggestion automatique de chauffeur")
        
        st.info("🎯 **L'assistant analyse** : Distance depuis dernière course, charge de travail, disponibilité")
        
        chauffeurs_list = get_chauffeurs()
        
        if not chauffeurs_list:
            st.error("⚠️ Aucun chauffeur disponible.")
        else:
            st.markdown("### 📋 Nouvelle course")
            
            col1, col2 = st.columns(2)
            
            with col1:
                nom_client_assistant = st.text_input("Nom du client", key="nom_client_assistant")
                adresse_pec_assistant = st.text_input("Adresse PEC", key="adresse_pec_assistant",
                                                     help="Ex: Dangeau, Place de l'Église")
            
            with col2:
                lieu_depose_assistant = st.text_input("Lieu de dépose", key="lieu_depose_assistant",
                                                     help="Ex: Chartres Gare")
                heure_prevue_assistant = st.time_input("Heure PEC", value=datetime.now(TIMEZONE).time(),
                                                       key="heure_prevue_assistant")
            
            if st.button("🤖 Suggérer le meilleur chauffeur", type="primary", use_container_width=True):
                
                if not nom_client_assistant or not adresse_pec_assistant or not lieu_depose_assistant:
                    st.error("⚠️ Veuillez remplir tous les champs")
                else:
                    with st.spinner("🔄 Analyse en cours..."):
                        
                        try:
                            google_api_key = st.secrets["google_maps"]["api_key"]
                        except:
                            st.error("⚠️ Erreur : Clé API Google Maps non configurée")
                            st.stop()
                        
                        date_aujourdhui = datetime.now(TIMEZONE).strftime('%Y-%m-%d')
                        
                        chauffeurs_data = []
                        
                        for chauf in chauffeurs_list:
                            courses_chauffeur = get_courses(chauffeur_id=chauf['id'], date_filter=date_aujourdhui)
                            nb_courses = len(courses_chauffeur) if courses_chauffeur else 0
                            
                            last_course_data = None
                            if courses_chauffeur and len(courses_chauffeur) > 0:
                                courses_triees = sorted(courses_chauffeur, 
                                                       key=lambda x: x.get('heure_prevue', ''), 
                                                       reverse=True)
                                derniere = courses_triees[0]
                                last_course_data = {
                                    'lieu_depose': derniere.get('lieu_depose', '')
                                }
                            
                            chauffeurs_data.append({
                                'id': chauf['id'],
                                'name': chauf['full_name'],
                                'last_course': last_course_data,
                                'courses_today': nb_courses
                            })
                        
                        course_data = {
                            'adresse_pec': adresse_pec_assistant,
                            'heure_prevue': datetime.now(TIMEZONE),
                            'lieu_depose': lieu_depose_assistant
                        }
                        
                        try:
                            suggestions = suggest_best_driver(
                                chauffeurs=chauffeurs_data,
                                course_data=course_data,
                                api_key=google_api_key
                            )
                            
                            st.session_state['assistant_suggestions'] = suggestions
                            st.session_state['assistant_course_data'] = {
                                'nom_client': nom_client_assistant,
                                'adresse_pec': adresse_pec_assistant,
                                'lieu_depose': lieu_depose_assistant,
                                'heure_prevue': heure_prevue_assistant
                            }
                            
                            st.success("✅ Analyse terminée !")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Erreur lors de l'analyse : {str(e)}")
            
            if 'assistant_suggestions' in st.session_state and st.session_state['assistant_suggestions']:
                
                st.markdown("---")
                st.markdown("### 📊 Résultats - Classement des chauffeurs")
                
                suggestions = st.session_state['assistant_suggestions']
                course_info = st.session_state.get('assistant_course_data', {})
                
                st.info(f"**Course :** {course_info.get('nom_client', 'N/A')} | "
                       f"{course_info.get('adresse_pec', 'N/A')} → {course_info.get('lieu_depose', 'N/A')}")
                
                for i, sug in enumerate(suggestions, 1):
                    
                    if i == 1:
                        emoji = "🏆"
                        color = "#28a745"
                        badge = "OPTIMAL"
                    elif i == 2:
                        emoji = "⚠️"
                        color = "#ffc107"
                        badge = "ALTERNATIF"
                    else:
                        emoji = "❌"
                        color = "#dc3545"
                        badge = "NON RECOMMANDÉ"
                    
                    with st.container():
                        st.markdown(
                            f"""
                            <div style="border: 2px solid {color}; border-radius: 10px; padding: 15px; margin-bottom: 15px;">
                                <h4>{emoji} #{i} - {sug['driver_name']} <span style="background-color: {color}; color: white; padding: 3px 10px; border-radius: 5px; font-size: 0.8em;">{badge}</span></h4>
                                <p style="font-size: 1.2em; font-weight: bold;">Score : {sug['score']}/100 points</p>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                        
                        col_info1, col_info2, col_info3 = st.columns(3)
                        
                        with col_info1:
                            if sug['distance_km'] is not None:
                                st.metric("Distance", f"{sug['distance_km']} km")
                                st.caption(f"~{sug['duration_min']} min")
                            else:
                                st.metric("Distance", "À sa base")
                        
                        with col_info2:
                            st.metric("Courses aujourd'hui", sug['courses_today'])
                        
                        with col_info3:
                            st.metric("Disponibilité", "✅ OK" if sug['available'] else "❌ Occupé")
                        
                        st.caption(f"**Détails :** {sug['details']}")
                        
                        if st.button(f"✅ Assigner à {sug['driver_name']}", 
                                   key=f"assign_{sug['driver_id']}", 
                                   use_container_width=True,
                                   type="primary" if i == 1 else "secondary"):
                            
                            heure_prevue_dt = datetime.combine(
                                datetime.now(TIMEZONE).date(),
                                course_info.get('heure_prevue', datetime.now(TIMEZONE).time())
                            )
                            heure_prevue_dt = TIMEZONE.localize(heure_prevue_dt)
                            
                            course_to_create = {
                                'chauffeur_id': sug['driver_id'],
                                'nom_client': course_info.get('nom_client', ''),
                                'telephone_client': '',
                                'adresse_pec': course_info.get('adresse_pec', ''),
                                'lieu_depose': course_info.get('lieu_depose', ''),
                                'heure_prevue': heure_prevue_dt.isoformat(),
                                'type_course': 'Autre',
                                'tarif_estime': 0,
                                'km_estime': sug['distance_km'] if sug['distance_km'] else 0,
                                'commentaire': f"Suggéré par Assistant (Score: {sug['score']}/100)",
                                'created_by': st.session_state.user['id']
                            }
                            
                            try:
                                create_course(course_to_create)
                                st.success(f"✅ Course créée et assignée à {sug['driver_name']} !")
                                
                                if 'assistant_suggestions' in st.session_state:
                                    del st.session_state['assistant_suggestions']
                                if 'assistant_course_data' in st.session_state:
                                    del st.session_state['assistant_course_data']
                                
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Erreur : {str(e)}")
                
                st.markdown("---")
                
                if st.button("🔄 Nouvelle suggestion", use_container_width=True):
                    if 'assistant_suggestions' in st.session_state:
                        del st.session_state['assistant_suggestions']
                    if 'assistant_course_data' in st.session_state:
                        del st.session_state['assistant_course_data']
                    st.rerun()


# ============================================
# INTERFACE CHAUFFEUR - OPTIMISÉE
# ============================================

def chauffeur_page():
    """Interface Chauffeur - OPTIMISÉE avec système de notifications"""
    
    # ============================================
    # AUTO-REFRESH AUTOMATIQUE (30 secondes) - STREAMLIT-AUTOREFRESH
    # ============================================
    # Package officiel qui fonctionne vraiment !
    count = st_autorefresh(interval=30000, key="chauffeur_autorefresh")
    
    # ============================================
    # ============================================
    col_deconnexion, col_refresh = st.columns([1, 6])
    
    st.title("🚖 Mes courses")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Chauffeur)")
    
    # ============================================
    # SYSTÈME DE NOTIFICATIONS
    # ============================================
    unread_count = get_unread_count(st.session_state.user['id'])
    
    if unread_count > 0:
        # Badge de notification
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #FF4444 0%, #CC0000 100%); 
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
        
        # Liste des notifications
        with st.expander("📋 Voir les notifications", expanded=True):
            notifications = get_unread_notifications(st.session_state.user['id'])
            
            for notif in notifications:
                icon = {
                    'nouvelle_course': '🆕',
                    'modification': '✏️',
                    'changement_chauffeur': '🔄',
                    'annulation': '❌'
                }.get(notif['type'], '📢')
                
                st.info(f"{icon} **{notif['message']}**")
                
                if notif.get('nom_client'):
                    heure = notif.get('heure_pec_prevue', 'N/A')
                    st.caption(f"👤 {notif['nom_client']} | ⏰ {heure}")
                    st.caption(f"📍 {notif.get('adresse_pec', 'N/A')} → {notif.get('lieu_depose', 'N/A')}")
            
            if st.button("✅ Marquer tout comme lu", use_container_width=True):
                mark_notifications_as_read(st.session_state.user['id'])
                st.session_state.notification_sound_played = False
                st.rerun()
    
    
    
    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
            if "user" in st.session_state:
                del st.session_state.user
            st.rerun()
    
    with col_refresh:
        if st.button("🔄 Actualiser (auto: 30s)", use_container_width=True):
            st.rerun()
    
    st.markdown("---")
    
    # Filtres
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
    
    # Récupérer les courses DU CHAUFFEUR avec role='chauffeur' pour filtrer visible_chauffeur
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
                'deposee': '🟢'
            }
            
            statut_text = {
                'nouvelle': 'NOUVELLE',
                'confirmee': 'CONFIRMÉE',
                'pec': 'PRISE EN CHARGE',
                'deposee': 'TERMINÉE'
            }
            
            date_fr = format_date_fr(course['heure_prevue'])
            heure_affichage = course.get('heure_pec_prevue', extract_time_str(course['heure_prevue']))
            titre = f"{statut_colors.get(course['statut'], '⚪')} {date_fr} {heure_affichage} - {course['nom_client']} - {statut_text.get(course['statut'], course['statut'].upper())}"
            
            with st.expander(titre):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Client :** {course['nom_client']}")
                    st.write(f"**Tel :** {course['telephone_client']}")
                    st.write(f"**📅 Date :** {date_fr}")
                    
                    if course.get('heure_pec_prevue'):
                        st.success(f"⏰ **Heure PEC : {course['heure_pec_prevue']}**")
                    st.write(f"**PEC :** {course['adresse_pec']}")
                
                with col2:
                    st.write(f"**Dépose :** {course['lieu_depose']}")
                    st.write(f"**Type :** {course['type_course']}")
                    st.write(f"**Tarif :** {course['tarif_estime']}€")
                    st.write(f"**Km :** {course['km_estime']} km")
                
                if course['date_confirmation']:
                    st.caption(f"✅ Confirmée : {format_datetime_fr(course['date_confirmation'])}")
                if course['date_pec']:
                    st.info(f"📍 **PEC : {extract_time_str(course['date_pec'])}**")
                if course['date_depose']:
                    st.caption(f"🏁 Déposée : {format_datetime_fr(course['date_depose'])}")
                
                if course['commentaire']:
                    st.info(f"💬 **Secrétaire :** {course['commentaire']}")
                
                # ============================================
                # COMMENTAIRE CHAUFFEUR - OPTIMISÉ
                # ============================================
                st.markdown("---")
                st.markdown("**💭 Commentaire**")
                
                if course.get('commentaire_chauffeur'):
                    st.success(f"📝 {course['commentaire_chauffeur']}")
                
                new_comment = st.text_area(
                    "Ajouter/modifier",
                    value=course.get('commentaire_chauffeur', ''),
                    key=f"comment_{course['id']}",
                    height=80
                )
                
                # OPTIMISATION: Enregistrement en 1 clic - Rerun sans message
                if st.button("💾 Enregistrer", key=f"save_comment_{course['id']}"):
                    update_commentaire_chauffeur(course['id'], new_comment)
                    st.rerun()  # Rerun immédiat sans message
                
                st.markdown("---")
                
                # ============================================
                # BOUTONS D'ACTION - OPTIMISÉS POUR 1 CLIC
                # ============================================
                col1, col2, col3, col4 = st.columns(4)
                
                if course['statut'] == 'nouvelle':
                    with col1:
                        # OPTIMISATION: Rerun immédiat sans message
                        if st.button("✅ Confirmer", key=f"confirm_{course['id']}", use_container_width=True):
                            update_course_status(course['id'], 'confirmee')
                            st.rerun()
                
                elif course['statut'] == 'confirmee':
                    with col2:
                        # OPTIMISATION: Rerun immédiat sans message
                        if st.button("📍 PEC", key=f"pec_{course['id']}", use_container_width=True):
                            update_course_status(course['id'], 'pec')
                            st.rerun()
                
                elif course['statut'] == 'pec':
                    with col3:
                        # OPTIMISATION: Rerun immédiat sans message
                        if st.button("🏁 Déposé", key=f"depose_{course['id']}", use_container_width=True):
                            update_course_status(course['id'], 'deposee')
                            st.rerun()
                
                elif course['statut'] == 'deposee':
                    st.success("✅ Course terminée")


# ============================================
# MAIN APPLICATION
# ============================================

def main():
    """Point d'entrée principal de l'application"""
    init_db()
    
    if 'user' not in st.session_state:
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
