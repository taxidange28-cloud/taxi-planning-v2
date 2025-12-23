import streamlit as st
import streamlit.components.v1 as components
import psycopg2
from psycopg2.extras import RealDictCursor
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import os
import pytz

# Import du module Assistant Intelligent
from assistant import suggest_best_driver, calculate_distance

# ============================================
# CORRECTIONS BUGS V2 :
# - Suppression du cache @st.cache_data pour éviter les validations multiples
# - Ajout de st.cache_data.clear() après chaque modification
# - Tri chronologique ASC (croissant) par heure_pec_prevue
# - Callbacks optimisés
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
    page_title="Transport DanGE - Planning V2",
    page_icon="🚖",
    layout="wide"
)

# Connexion à la base de données Supabase PostgreSQL
def get_db_connection():
    """Connexion à PostgreSQL Supabase avec secrets Streamlit"""
    try:
        # Essayer avec connection string si disponible
        if "connection_string" in st.secrets.get("supabase", {}):
            conn = psycopg2.connect(
                st.secrets["supabase"]["connection_string"],
                cursor_factory=RealDictCursor
            )
        else:
            # Sinon utiliser les paramètres individuels avec sslmode
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
    pass

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
    conn.close()
    
    if user:
        return {
            'id': user['id'],
            'username': user['username'],
            'role': user['role'],
            'full_name': user['full_name']
        }
    return None

# ============================================
# CORRECTION BUG : SUPPRESSION DU CACHE
# Avant : @st.cache_data(ttl=60)
# Maintenant : Pas de cache pour rafraîchissement immédiat
# ============================================
def get_chauffeurs():
    """Récupère tous les chauffeurs - SANS CACHE pour éviter les bugs de validation"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, full_name, username
        FROM users
        WHERE role = 'chauffeur'
        ORDER BY full_name
    ''')
    chauffeurs = cursor.fetchall()
    conn.close()
    return [{'id': c['id'], 'full_name': c['full_name'], 'username': c['username']} for c in chauffeurs]

# ============ FONCTIONS CLIENTS RÉGULIERS ============

def create_client_regulier(data):
    conn = get_db_connection()
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
    conn.close()
    return client_id

def get_clients_reguliers(search_term=None):
    conn = get_db_connection()
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
    conn.close()
    
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
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM clients_reguliers WHERE id = %s', (client_id,))
    client = cursor.fetchone()
    conn.close()
    
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
    conn.close()

def delete_client_regulier(client_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE clients_reguliers SET actif = 0 WHERE id = %s', (client_id,))
    conn.commit()
    conn.close()

# ============ FIN FONCTIONS CLIENTS RÉGULIERS ============


# Fonction pour créer une course
def create_course(data):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Déterminer si la course doit être visible pour le chauffeur
    from datetime import datetime
    import pytz
    
    heure_prevue = data['heure_prevue']
    if isinstance(heure_prevue, str):
        heure_prevue = datetime.fromisoformat(heure_prevue.replace('Z', '+00:00'))
    
    if heure_prevue.tzinfo is None:
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
    conn.close()
    return course_id

# Fonction helper pour convertir date au format français
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

# Fonction helper pour convertir date+heure au format français
def format_datetime_fr(datetime_input):
    """Convertit une datetime ISO (YYYY-MM-DD HH:MM:SS) ou datetime object en format français (DD/MM/YYYY HH:MM)"""
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

# Fonction helper pour extraire l'heure d'un datetime ou string
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
# CORRECTION BUG : SUPPRESSION DU CACHE + TRI CHRONOLOGIQUE
# Avant : @st.cache_data(ttl=30) + ORDER BY heure_prevue DESC
# Maintenant : Pas de cache + ORDER BY COALESCE(heure_pec_prevue, heure_prevue) ASC
# ============================================
def get_courses(chauffeur_id=None, date_filter=None, role=None):
    """
    Récupère les courses - SANS CACHE pour éviter les bugs de validation
    TRI CHRONOLOGIQUE CROISSANT par heure PEC prévue (ou heure création)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = '''
        SELECT c.*, u.full_name as chauffeur_name
        FROM courses c
        JOIN users u ON c.chauffeur_id = u.id
        WHERE 1=1
    '''
    params = []
    
    if chauffeur_id:
        query += ' AND c.chauffeur_id = %s'
        params.append(chauffeur_id)
    
    if date_filter:
        query += ' AND DATE(c.heure_prevue) = %s'
        params.append(date_filter)
    
    if role == 'chauffeur':
        query += ' AND c.visible_chauffeur = true'
    
    # CORRECTION : Tri chronologique ASC (croissant) par heure PEC
    query += ''' 
        ORDER BY 
            COALESCE(
                c.heure_pec_prevue::time, 
                (c.heure_prevue AT TIME ZONE 'Europe/Paris')::time
            ) ASC
    '''
    
    cursor.execute(query, params)
    courses = cursor.fetchall()
    conn.close()
    
    result = []
    for course in courses:
        try:
            commentaire_chauffeur = course['commentaire_chauffeur']
        except (KeyError, IndexError):
            commentaire_chauffeur = None
        
        try:
            heure_pec_prevue = course['heure_pec_prevue']
        except (KeyError, IndexError):
            heure_pec_prevue = None
        
        try:
            temps_trajet_minutes = course['temps_trajet_minutes']
        except (KeyError, IndexError):
            temps_trajet_minutes = None
        
        try:
            heure_depart_calculee = course['heure_depart_calculee']
        except (KeyError, IndexError):
            heure_depart_calculee = None
        
        try:
            client_regulier_id = course['client_regulier_id']
        except (KeyError, IndexError):
            client_regulier_id = None
        
        try:
            visible_chauffeur = course['visible_chauffeur']
        except (KeyError, IndexError):
            visible_chauffeur = True
        
        result.append({
            'id': course['id'],
            'chauffeur_id': course['chauffeur_id'],
            'nom_client': course['nom_client'],
            'telephone_client': course['telephone_client'],
            'adresse_pec': course['adresse_pec'],
            'lieu_depose': course['lieu_depose'],
            'heure_prevue': course['heure_prevue'],
            'heure_pec_prevue': heure_pec_prevue,
            'temps_trajet_minutes': temps_trajet_minutes,
            'heure_depart_calculee': heure_depart_calculee,
            'type_course': course['type_course'],
            'tarif_estime': course['tarif_estime'],
            'km_estime': course['km_estime'],
            'commentaire': course['commentaire'],
            'commentaire_chauffeur': commentaire_chauffeur,
            'statut': course['statut'],
            'date_creation': course['date_creation'],
            'date_confirmation': course['date_confirmation'],
            'date_pec': course['date_pec'],
            'date_depose': course['date_depose'],
            'created_by': course['created_by'],
            'client_regulier_id': client_regulier_id,
            'chauffeur_name': course['chauffeur_name'],
            'visible_chauffeur': visible_chauffeur
        })
    
    return result

# NOUVEAU : Fonction pour distribuer les courses d'un jour
def distribute_courses_for_date(date_str):
    """Rend visibles toutes les courses non distribuées pour une date donnée"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE courses
            SET visible_chauffeur = true
            WHERE DATE(heure_prevue AT TIME ZONE 'Europe/Paris') = %s
            AND visible_chauffeur = false
        ''', (date_str,))
        
        count = cursor.rowcount
        conn.commit()
        conn.close()
        
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

# NOUVEAU : Fonction pour exporter et purger une semaine
def export_week_to_excel(week_start_date):
    """Exporte toutes les courses d'une semaine en Excel"""
    try:
        from io import BytesIO
        from openpyxl.styles import Font, PatternFill, Alignment
        
        conn = get_db_connection()
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
        conn.close()
        
        if not rows or len(rows) == 0:
            return {
                'success': False,
                'excel_data': None,
                'count': 0,
                'filename': '',
                'error': f'Aucune course trouvée pour la semaine du {week_start_date.strftime("%d/%m/%Y")} au {week_end_date.strftime("%d/%m/%Y")}'
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
            'excel_data': None,
            'count': 0,
            'filename': '',
            'error': str(e)
        }

def purge_week_courses(week_start_date):
    """Supprime TOUTES les courses de la semaine de la base de données"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        week_end_date = week_start_date + timedelta(days=6)
        
        cursor.execute('''
            SELECT id FROM courses
            WHERE heure_prevue >= %s AND heure_prevue < %s + INTERVAL '1 day'
        ''', (week_start_date, week_end_date))
        
        course_ids = [row['id'] for row in cursor.fetchall()]
        
        if not course_ids:
            conn.close()
            return {'success': True, 'count': 0}
        
        cursor.execute('''
            DELETE FROM courses
            WHERE id = ANY(%s)
        ''', (course_ids,))
        
        count = cursor.rowcount
        conn.commit()
        conn.close()
        
        return {'success': True, 'count': count}
        
    except Exception as e:
        return {'success': False, 'count': 0, 'error': str(e)}

# ============================================
# CORRECTION BUG : Fonction update_course_status simplifiée
# Ajout de clear_cache() pour rafraîchissement immédiat
# ============================================
def update_course_status(course_id, new_status):
    """Met à jour le statut d'une course et vide le cache"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
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
    conn.close()

def update_commentaire_chauffeur(course_id, commentaire):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE courses
        SET commentaire_chauffeur = %s
        WHERE id = %s
    ''', (commentaire, course_id))
    
    conn.commit()
    conn.close()

def update_heure_pec_prevue(course_id, nouvelle_heure):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE courses
        SET heure_pec_prevue = %s
        WHERE id = %s
    ''', (nouvelle_heure, course_id))
    
    conn.commit()
    conn.close()
    return True

def delete_course(course_id):
    """Supprime une course et vide le cache"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        DELETE FROM courses
        WHERE id = %s
    ''', (course_id,))
    
    conn.commit()
    conn.close()
    return True

def update_course_details(course_id, nouvelle_heure_pec, nouveau_chauffeur_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE courses
        SET heure_pec_prevue = %s, chauffeur_id = %s
        WHERE id = %s
    ''', (nouvelle_heure_pec, nouveau_chauffeur_id, course_id))
    
    conn.commit()
    conn.close()
    return True

def create_user(username, password, role, full_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = hash_password(password)
    
    try:
        cursor.execute('''
            INSERT INTO users (username, password_hash, role, full_name)
            VALUES (%s, %s, %s, %s)
        ''', (username, hashed_password, role, full_name))
        conn.commit()
        conn.close()
        return True
    except psycopg2.IntegrityError:
        conn.close()
        return False

def delete_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
        admin_count = get_scalar_result(cursor)
        
        cursor.execute("SELECT role FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        
        if user and user['role'] == 'admin' and admin_count <= 1:
            conn.close()
            return False, "Impossible de supprimer le dernier administrateur"
        
        cursor.execute('DELETE FROM users WHERE id = %s', (user_id,))
        conn.commit()
        conn.close()
        return True, "Utilisateur supprimé avec succès"
    except Exception as e:
        conn.close()
        return False, f"Erreur: {str(e)}"

def get_all_users():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, username, role, full_name, created_at
        FROM users
        ORDER BY role, full_name
    ''')
    users = cursor.fetchall()
    conn.close()
    return users

# Interface de connexion
def login_page():
    st.title("Transport DanGE - Planning V2")
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

# Fonction callback pour les boutons du Planning du Jour
def set_delete_confirmation(course_id):
    """Active la confirmation de suppression pour une course"""
    st.session_state[f'confirm_del_jour_{course_id}'] = True

# Fonction de réattribution de course
def reassign_course_to_driver(course_id, new_chauffeur_id):
    """Réattribue une course à un nouveau chauffeur"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT c.chauffeur_id, c.nom_client, u.full_name 
        FROM courses c
        JOIN users u ON c.chauffeur_id = u.id
        WHERE c.id = %s
    ''', (course_id,))
    result = cursor.fetchone()
    
    if result:
        old_chauffeur_id, nom_client, old_chauffeur_name = result
        
        cursor.execute('SELECT full_name FROM users WHERE id = %s', (new_chauffeur_id,))
        new_chauffeur_name = get_scalar_result(cursor)
        
        cursor.execute('''
            UPDATE courses 
            SET chauffeur_id = %s
            WHERE id = %s
        ''', (new_chauffeur_id, course_id))
        
        conn.commit()
        conn.close()
        
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
        conn.close()
        return {'success': False, 'error': 'Course non trouvée'}

# Interface Admin
def admin_page():
    st.title("🔧 Administration - Transport DanGE V2")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Admin)")
    
    col_deconnexion, col_refresh = st.columns([1, 6])
    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
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
        
        conn.close()
    
    with tab4:
        st.subheader("💾 Export des données")
        st.write("Exporter les courses en CSV pour analyse ou comptabilité")
        
        export_date_debut = st.date_input("Date de début", value=datetime.now() - timedelta(days=30))
        export_date_fin = st.date_input("Date de fin", value=datetime.now())
        
        if st.button("Exporter en CSV"):
            conn = get_db_connection()
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
            conn.close()
            
            csv = df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Télécharger le CSV",
                data=csv,
                file_name=f"courses_export_{export_date_debut}_{export_date_fin}.csv",
                mime="text/csv"
            )


# Interface Secrétaire (PARTIE 1 - continuera avec str_replace suivant)
def secretaire_page():
    st.title("📝 Secrétariat - Planning V2")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Secrétaire)")
    
    col_deconnexion, col_refresh = st.columns([1, 6])
    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
            del st.session_state.user
            st.rerun()
    with col_refresh:
        if st.button("🔄 Actualiser"):
            st.rerun()
    
    st.markdown("---")
    
    tab1, tab2, tab3 = st.tabs(["➕ Nouvelle Course", "📊 Planning Global", "📅 Planning du Jour"])
    
    with tab1:
        st.subheader("Créer une nouvelle course")
        
        course_dupliquee = None
        if 'course_to_duplicate' in st.session_state:
            course_dupliquee = st.session_state.course_to_duplicate
            st.success(f"📋 Duplication de : {course_dupliquee['nom_client']}")
            if st.button("❌ Annuler la duplication"):
                del st.session_state.course_to_duplicate
                st.rerun()
        
        chauffeurs = get_chauffeurs()
        
        if not chauffeurs:
            st.error("⚠️ Aucun chauffeur disponible.")
        else:
            with st.form("new_course_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    chauffeur_names = [c['full_name'] for c in chauffeurs]
                    selected_chauffeur = st.selectbox("Chauffeur *", chauffeur_names)
                    
                    nom_client = st.text_input("Nom du client *")
                    telephone_client = st.text_input("Téléphone du client")
                    adresse_pec = st.text_input("Adresse de prise en charge *")
                    lieu_depose = st.text_input("Lieu de dépose *")
                
                with col2:
                    now_paris = datetime.now(TIMEZONE)
                    date_course = st.date_input("Date de la course *", value=now_paris.date())
                    heure_pec_prevue = st.text_input("Heure PEC prévue (HH:MM)", placeholder="Ex: 17:50")
                    
                    type_course = st.selectbox("Type de course *", ["CPAM", "Privé"])
                    tarif_estime = st.number_input("Tarif estimé (€)", min_value=0.0, step=5.0)
                    km_estime = st.number_input("Kilométrage estimé", min_value=0.0, step=1.0)
                    commentaire = st.text_area("Commentaire")
                
                submitted = st.form_submit_button("✅ Créer la course", use_container_width=True)
                
                if submitted:
                    if nom_client and adresse_pec and lieu_depose and selected_chauffeur:
                        chauffeur_id = None
                        for c in chauffeurs:
                            if c['full_name'] == selected_chauffeur:
                                chauffeur_id = c['id']
                                break
                        
                        if chauffeur_id:
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
                                'created_by': st.session_state.user['id']
                            }
                            
                            course_id = create_course(course_data)
                            if course_id:
                                st.success(f"✅ Course créée avec succès pour {selected_chauffeur}")
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de la création")
                    else:
                        st.error("Veuillez remplir tous les champs obligatoires (*)")
    
    with tab2:
        st.subheader("Planning Global")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            show_all_sec = st.checkbox("Afficher toutes les courses", value=True, key="sec_show_all")
            if not show_all_sec:
                date_filter = st.date_input("Date", value=datetime.now(), key="sec_date")
            else:
                date_filter = None
        with col2:
            chauffeur_filter = st.selectbox("Chauffeur", ["Tous"] + [c['full_name'] for c in get_chauffeurs()], key="sec_chauff")
        with col3:
            statut_filter = st.selectbox("Statut", ["Tous", "Nouvelle", "Confirmée", "PEC", "Déposée"], key="sec_statut")
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
        if not show_all_sec and date_filter:
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
                    with col2:
                        st.write(f"**Chauffeur :** {course['chauffeur_name']}")
                        st.write(f"**Tarif estimé :** {course['tarif_estime']}€")
                        st.write(f"**Km estimé :** {course['km_estime']} km")
                        st.write(f"**Statut :** {course['statut'].upper()}")
                        if course['commentaire']:
                            st.write(f"**Commentaire :** {course['commentaire']}")
                    
                    if course.get('commentaire_chauffeur'):
                        st.warning(f"💭 **Commentaire chauffeur** : {course['commentaire_chauffeur']}")
                    
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
                                st.success("✅ Course supprimée")
                                del st.session_state[f'confirmer_suppression_{course["id"]}']
                                st.rerun()
        else:
            st.info("Aucune course")
    
    with tab3:
        st.subheader("📆 Planning du Jour")
        
        if 'planning_jour_date' not in st.session_state:
            st.session_state.planning_jour_date = datetime.now(TIMEZONE).date()
        
        selected_date = st.date_input(
            "Date",
            value=st.session_state.planning_jour_date,
            key="date_picker_jour"
        )
        if selected_date != st.session_state.planning_jour_date:
            st.session_state.planning_jour_date = selected_date
            st.rerun()
        
        jours_fr = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
        jour_semaine = jours_fr[selected_date.weekday()]
        st.markdown(f"### {jour_semaine} {selected_date.strftime('%d/%m/%Y')}")
        
        st.markdown("---")
        
        chauffeurs = get_chauffeurs()
        
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
        
        courses_jour = get_courses(date_filter=st.session_state.planning_jour_date.strftime('%Y-%m-%d'))
        
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
                                    st.warning("⚠️ Confirmer ?")
                                    col_c1, col_c2 = st.columns(2)
                                    with col_c1:
                                        if st.button("❌ Annuler", key=f"cancel_del_jour_{course['id']}", use_container_width=True):
                                            del st.session_state[f'confirm_del_jour_{course["id"]}']
                                            st.rerun()
                                    with col_c2:
                                        if st.button("✅ OK", key=f"ok_del_jour_{course['id']}", use_container_width=True):
                                            delete_course(course['id'])
                                            st.success("✅ Supprimée")
                                            del st.session_state[f'confirm_del_jour_{course["id"]}']
                                            st.rerun()
                    else:
                        st.info("Aucune course")
                else:
                    st.markdown(f"### ⚪ Chauffeur {i+1}")
                    st.info("Non assigné")
        
        st.markdown("---")
        st.caption("🔵 Nouvelle | 🟡 Confirmée | 🔴 PEC | 🟢 Terminée")

# Interface Chauffeur 
def chauffeur_page():
    st.title("🚖 Mes courses")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Chauffeur)")
    
    col_deconnexion, col_refresh = st.columns([1, 6])
    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
            del st.session_state.user
            st.rerun()
    with col_refresh:
        if st.button("🔄 Actualiser"):
            st.rerun()
    
    st.markdown("---")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        show_all_chauff = st.checkbox("Afficher toutes mes courses", value=True)
        if not show_all_chauff:
            date_filter = st.date_input("Date", value=datetime.now())
        else:
            date_filter = None
    with col2:
        date_filter_str = None
        if not show_all_chauff and date_filter:
            date_filter_str = date_filter.strftime('%Y-%m-%d')
        courses = get_courses(chauffeur_id=st.session_state.user['id'], date_filter=date_filter_str, role='chauffeur')
        st.metric("Mes courses", len([c for c in courses if c['statut'] != 'deposee']))
    with col3:
        st.metric("Terminées", len([c for c in courses if c['statut'] == 'deposee']))
    
    if not courses:
        st.info("Aucune course pour cette sélection")
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
                    st.write(f"**Téléphone :** {course['telephone_client']}")
                    st.write(f"**📅 Date PEC :** {date_fr}")
                    
                    if course.get('heure_pec_prevue'):
                        st.success(f"⏰ **Heure PEC prévue : {course['heure_pec_prevue']}**")
                    st.write(f"**PEC :** {course['adresse_pec']}")
                
                with col2:
                    st.write(f"**Dépose :** {course['lieu_depose']}")
                    st.write(f"**Type :** {course['type_course']}")
                    st.write(f"**Tarif estimé :** {course['tarif_estime']}€")
                    st.write(f"**Km estimé :** {course['km_estime']} km")
                
                if course['date_confirmation']:
                    st.caption(f"✅ Confirmée le : {format_datetime_fr(course['date_confirmation'])}")
                if course['date_pec']:
                    st.info(f"📍 **Heure de PEC : {extract_time_str(course['date_pec'])}**")
                if course['date_depose']:
                    st.caption(f"🏁 Déposée le : {format_datetime_fr(course['date_depose'])}")
                
                if course['commentaire']:
                    st.info(f"💬 **Commentaire secrétaire :** {course['commentaire']}")
                
                st.markdown("---")
                st.markdown("**💭 Commentaire pour la secrétaire**")
                
                if course.get('commentaire_chauffeur'):
                    st.success(f"📝 Votre commentaire : {course['commentaire_chauffeur']}")
                
                new_comment = st.text_area(
                    "Ajouter ou modifier un commentaire",
                    value=course.get('commentaire_chauffeur', ''),
                    key=f"comment_{course['id']}",
                    placeholder="Ex: Client en retard, bagages supplémentaires...",
                    height=80
                )
                
                if st.button("💾 Enregistrer commentaire", key=f"save_comment_{course['id']}"):
                    update_commentaire_chauffeur(course['id'], new_comment)
                    st.success("✅ Commentaire enregistré")
                    st.rerun()
                
                st.markdown("---")
                
                col1, col2, col3, col4 = st.columns(4)
                
                if course['statut'] == 'nouvelle':
                    with col1:
                        if st.button("✅ Confirmer", key=f"confirm_{course['id']}", use_container_width=True):
                            update_course_status(course['id'], 'confirmee')
                            st.rerun()
                
                elif course['statut'] == 'confirmee':
                    with col2:
                        if st.button("📍 PEC effectuée", key=f"pec_{course['id']}", use_container_width=True):
                            update_course_status(course['id'], 'pec')
                            st.rerun()
                
                elif course['statut'] == 'pec':
                    with col3:
                        if st.button("🏁 Client déposé", key=f"depose_{course['id']}", use_container_width=True):
                            update_course_status(course['id'], 'deposee')
                            st.rerun()
                
                elif course['statut'] == 'deposee':
                    st.success("✅ Course terminée")


# Main
def main():
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
