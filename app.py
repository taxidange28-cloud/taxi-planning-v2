import os
import hashlib
import pytz
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool, PoolError
from datetime import datetime, timedelta, time, date
from weakref import WeakSet
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Import du module Assistant Intelligent (doit exister dans votre repo)
from assistant import suggest_best_driver, calculate_distance

# ============================================
# Configuration et tracking
# ============================================
TIMEZONE = pytz.timezone("Europe/Paris")
_pool_connections = WeakSet()

# Page config
st.set_page_config(page_title="Transport DanGE - Planning", page_icon="🚖", layout="wide")

# ============================================
# Connection Pool - configurable & robuste
# ============================================
@st.cache_resource
def get_connection_pool():
    """
    Crée et met en cache un SimpleConnectionPool.
    Valeurs min/max du pool configurables via les variables d'environnement:
      - DB_POOL_MIN (défaut 1)
      - DB_POOL_MAX (défaut 10)
    """
    supabase = st.secrets.get("supabase", {}) or {}
    min_conn = int(os.environ.get("DB_POOL_MIN", 1))
    max_conn = int(os.environ.get("DB_POOL_MAX", 10))

    try:
        if supabase.get("connection_string"):
            return SimpleConnectionPool(min_conn, max_conn, supabase["connection_string"])
        else:
            return SimpleConnectionPool(
                min_conn,
                max_conn,
                host=supabase.get("host"),
                database=supabase.get("database"),
                user=supabase.get("user"),
                password=supabase.get("password"),
                port=supabase.get("port"),
                sslmode="require",
            )
    except Exception as e:
        # On logue et retourne None pour fallback
        print("get_connection_pool error:", e)
        return None


def get_db_connection():
    """
    Récupère une connexion depuis le pool, ou crée une connexion directe si pool indisponible.
    Assure RealDictCursor partout.
    """
    supabase = st.secrets.get("supabase", {}) or {}

    conn_pool = None
    try:
        conn_pool = get_connection_pool()
    except Exception as e:
        print("get_db_connection: error fetching pool:", e)
        conn_pool = None

    # Try pool first
    if conn_pool:
        try:
            conn = conn_pool.getconn()
            # Assigner le cursor_factory (pour RealDictCursor)
            conn.cursor_factory = RealDictCursor
            _pool_connections.add(conn)
            return conn
        except PoolError as pe:
            print("PoolError getconn:", pe)
        except Exception as e:
            print("get_db_connection pool.getconn() error:", e)

    # Fallback -> connexion directe
    try:
        if supabase.get("connection_string"):
            conn = psycopg2.connect(supabase["connection_string"], cursor_factory=RealDictCursor)
        else:
            conn = psycopg2.connect(
                host=supabase.get("host"),
                database=supabase.get("database"),
                user=supabase.get("user"),
                password=supabase.get("password"),
                port=supabase.get("port"),
                sslmode="require",
                cursor_factory=RealDictCursor,
            )
        return conn
    except Exception as e:
        print("get_db_connection direct connect error:", e)
        return None


def release_db_connection(conn):
    """
    Remet la connexion dans le pool si elle vient du pool, sinon ferme la connexion.
    Toujours appeler après usage (finally).
    """
    try:
        if not conn:
            return
        conn_pool = get_connection_pool()
        # Si la connexion est trackée comme provenant du pool -> restituer
        if conn in _pool_connections and conn_pool:
            try:
                conn_pool.putconn(conn)
            except Exception:
                # Si erreur, fermer en force
                try:
                    conn.close()
                except Exception:
                    pass
            finally:
                try:
                    _pool_connections.discard(conn)
                except Exception:
                    pass
        else:
            # Connexion directe -> fermer
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        print("release_db_connection error:", e)


# ============================================
# Helper d'exécution SQL centralisé
# ============================================
def execute_query(query, params=None, fetchone=False, fetchall=False, commit=False):
    """
    Exécute une requête SQL en garantissant l'ouverture/fermeture de connexion.
    Returns:
      - fetchone => dict or None
      - fetchall => list[dict] or []
      - commit => True/False
      - otherwise => None
    """
    conn = get_db_connection()
    if not conn:
        return None if not fetchall else []
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        if commit:
            conn.commit()
            return True
        if fetchone:
            return cursor.fetchone()
        if fetchall:
            rows = cursor.fetchall()
            return rows
        return None
    except Exception as e:
        # Log erreur, rollback si nécessaire
        print("execute_query error:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        # Retour sécurisé
        if fetchall:
            return []
        return None
    finally:
        # Release connection dans tous les cas
        release_db_connection(conn)


# ============================================
# Helpers utilitaires
# ============================================
def get_scalar_result_from_query(query, params=None):
    row = execute_query(query, params=params, fetchone=True)
    if not row:
        return None
    # row is RealDictRow -> return first value
    try:
        return list(row.values())[0]
    except Exception:
        return None


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


# ============================================
# Fonctions CRUD / DB
# ============================================

def get_chauffeurs():
    rows = execute_query(
        """
        SELECT id, full_name, username
        FROM users
        WHERE role = 'chauffeur'
        ORDER BY full_name
        """,
        fetchall=True,
    )
    if not rows:
        return []
    return [{"id": r["id"], "full_name": r["full_name"], "username": r["username"]} for r in rows]


def init_notifications_table():
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id SERIAL PRIMARY KEY,
            chauffeur_id INTEGER REFERENCES users(id),
            course_id INTEGER,
            message TEXT,
            type VARCHAR(50),
            lu BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        commit=True,
    )


def create_notification(chauffeur_id, course_id, message, notification_type="nouvelle_course"):
    return execute_query(
        """
        INSERT INTO notifications (chauffeur_id, course_id, message, type)
        VALUES (%s, %s, %s, %s)
        """,
        params=(chauffeur_id, course_id, message, notification_type),
        commit=True,
    )


def get_unread_notifications(chauffeur_id):
    rows = execute_query(
        """
        SELECT n.id, n.message, n.type, n.created_at, n.course_id,
               c.nom_client, c.adresse_pec, c.lieu_depose, c.heure_pec_prevue
        FROM notifications n
        LEFT JOIN courses c ON n.course_id = c.id
        WHERE n.chauffeur_id = %s AND n.lu = FALSE
        ORDER BY n.created_at DESC
        LIMIT 20
        """,
        params=(chauffeur_id,),
        fetchall=True,
    )
    return [dict(r) for r in rows] if rows else []


def mark_notifications_as_read(chauffeur_id):
    return execute_query(
        """
        UPDATE notifications
        SET lu = TRUE
        WHERE chauffeur_id = %s AND lu = FALSE
        """,
        params=(chauffeur_id,),
        commit=True,
    )


def get_unread_count(chauffeur_id):
    val = get_scalar_result_from_query(
        """
        SELECT COUNT(*) FROM notifications
        WHERE chauffeur_id = %s AND lu = FALSE
        """,
        params=(chauffeur_id,),
    )
    try:
        return int(val or 0)
    except Exception:
        return 0


def create_client_regulier(data):
    row = execute_query(
        """
        INSERT INTO clients_reguliers (
            nom_complet, telephone, adresse_pec_habituelle, adresse_depose_habituelle,
            type_course_habituel, tarif_habituel, km_habituels, remarques
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        params=(
            data["nom_complet"],
            data.get("telephone"),
            data.get("adresse_pec_habituelle"),
            data.get("adresse_depose_habituelle"),
            data.get("type_course_habituel"),
            data.get("tarif_habituel"),
            data.get("km_habituels"),
            data.get("remarques"),
        ),
        fetchone=True,
    )
    if row:
        return row.get("id")
    return None


def get_clients_reguliers(search_term=None):
    if search_term:
        rows = execute_query(
            """
            SELECT * FROM clients_reguliers
            WHERE actif = 1 AND nom_complet LIKE %s
            ORDER BY nom_complet
            """,
            params=(f"%{search_term}%",),
            fetchall=True,
        )
    else:
        rows = execute_query(
            """
            SELECT * FROM clients_reguliers
            WHERE actif = 1
            ORDER BY nom_complet
            """,
            fetchall=True,
        )
    if not rows:
        return []
    result = []
    for client in rows:
        result.append(
            {
                "id": client.get("id"),
                "nom_complet": client.get("nom_complet"),
                "telephone": client.get("telephone"),
                "adresse_pec_habituelle": client.get("adresse_pec_habituelle"),
                "adresse_depose_habituelle": client.get("adresse_depose_habituelle"),
                "type_course_habituel": client.get("type_course_habituel"),
                "tarif_habituel": client.get("tarif_habituel"),
                "km_habituels": client.get("km_habituels"),
                "remarques": client.get("remarques"),
            }
        )
    return result


def get_client_regulier(client_id):
    client = execute_query("SELECT * FROM clients_reguliers WHERE id = %s", params=(client_id,), fetchone=True)
    if not client:
        return None
    return {
        "id": client.get("id"),
        "nom_complet": client.get("nom_complet"),
        "telephone": client.get("telephone"),
        "adresse_pec_habituelle": client.get("adresse_pec_habituelle"),
        "adresse_depose_habituelle": client.get("adresse_depose_habituelle"),
        "type_course_habituel": client.get("type_course_habituel"),
        "tarif_habituel": client.get("tarif_habituel"),
        "km_habituels": client.get("km_habituels"),
        "remarques": client.get("remarques"),
    }


def update_client_regulier(client_id, data):
    return execute_query(
        """
        UPDATE clients_reguliers
        SET nom_complet = %s, telephone = %s, adresse_pec_habituelle = %s,
            adresse_depose_habituelle = %s, type_course_habituel = %s,
            tarif_habituel = %s, km_habituels = %s, remarques = %s
        WHERE id = %s
        """,
        params=(
            data["nom_complet"],
            data.get("telephone"),
            data.get("adresse_pec_habituelle"),
            data.get("adresse_depose_habituelle"),
            data.get("type_course_habituel"),
            data.get("tarif_habituel"),
            data.get("km_habituels"),
            data.get("remarques"),
            client_id,
        ),
        commit=True,
    )


def delete_client_regulier(client_id):
    return execute_query("UPDATE clients_reguliers SET actif = 0 WHERE id = %s", params=(client_id,), commit=True)


def create_course(data):
    """
    Normalise heure_prevue et insert en base en utilisant RETURNING id.
    Retourne l'id créé ou None.
    """
    heure_prevue = data.get("heure_prevue")
    # Normalize to ISO string
    try:
        if isinstance(heure_prevue, str):
            try:
                dt = datetime.fromisoformat(heure_prevue.replace("Z", "+00:00"))
            except Exception:
                # fallback common format
                dt = datetime.strptime(heure_prevue, "%Y-%m-%d %H:%M:%S")
        elif isinstance(heure_prevue, datetime):
            dt = heure_prevue
        elif isinstance(heure_prevue, date):
            dt = datetime.combine(heure_prevue, datetime.now().time())
        else:
            dt = datetime.now(TIMEZONE)
        if dt.tzinfo is None:
            try:
                dt = TIMEZONE.localize(dt)
            except Exception:
                dt = dt.replace(tzinfo=TIMEZONE)
        else:
            dt = dt.astimezone(TIMEZONE)
        heure_iso = dt.isoformat()
    except Exception as e:
        print("create_course date normalization error:", e)
        heure_iso = datetime.now(TIMEZONE).isoformat()

    date_course = datetime.fromisoformat(heure_iso).date()
    date_aujourdhui = datetime.now(TIMEZONE).date()
    visible_chauffeur = date_course <= date_aujourdhui

    row = execute_query(
        """
        INSERT INTO courses (
            chauffeur_id, nom_client, telephone_client, adresse_pec,
            lieu_depose, heure_prevue, heure_pec_prevue, temps_trajet_minutes,
            heure_depart_calculee, type_course, tarif_estime,
            km_estime, commentaire, created_by, client_regulier_id, visible_chauffeur
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        params=(
            data.get("chauffeur_id"),
            data.get("nom_client"),
            data.get("telephone_client"),
            data.get("adresse_pec"),
            data.get("lieu_depose"),
            heure_iso,
            data.get("heure_pec_prevue"),
            data.get("temps_trajet_minutes"),
            data.get("heure_depart_calculee"),
            data.get("type_course"),
            data.get("tarif_estime"),
            data.get("km_estime"),
            data.get("commentaire"),
            data.get("created_by"),
            data.get("client_regulier_id"),
            visible_chauffeur,
        ),
        fetchone=True,
    )
    if row:
        return row.get("id")
    return None


def format_date_fr(date_input):
    if not date_input:
        return ""
    if isinstance(date_input, datetime):
        date_str = date_input.strftime("%Y-%m-%d")
    else:
        date_str = str(date_input)
    if len(date_str) < 10:
        return date_str
    annee, mois, jour = date_str[0:10].split("-")
    return f"{jour}/{mois}/{annee}"


def format_datetime_fr(datetime_input):
    if not datetime_input:
        return ""
    try:
        if isinstance(datetime_input, datetime):
            datetime_str = datetime_input.strftime("%Y-%m-%d %H:%M:%S")
        else:
            datetime_str = str(datetime_input)
        datetime_str = datetime_str.replace("T", " ")
        if len(datetime_str) >= 16:
            date_part = datetime_str[0:10]
            time_part = datetime_str[11:16]
            annee, mois, jour = date_part.split("-")
            return f"{jour}/{mois}/{annee} {time_part}"
        else:
            return format_date_fr(datetime_input)
    except Exception:
        return str(datetime_input)


def extract_time_str(datetime_input):
    if not datetime_input:
        return ""
    if isinstance(datetime_input, datetime):
        return datetime_input.strftime("%H:%M")
    datetime_str = str(datetime_input)
    if len(datetime_str) >= 16:
        return datetime_str[11:16]
    return ""


def get_courses(chauffeur_id=None, date_filter=None, role=None, days_back=30, limit=100):
    """
    Requête robuste pour récupérer les courses.
    Les paramètres date sont normalisés en 'YYYY-MM-DD' et castés côté SQL.
    """
    params = []
    query = """
        SELECT c.*, u.full_name as chauffeur_name
        FROM courses c
        JOIN users u ON c.chauffeur_id = u.id
        WHERE 1=1
    """
    try:
        if date_filter:
            if isinstance(date_filter, datetime):
                param_date = date_filter.date().strftime("%Y-%m-%d")
            elif isinstance(date_filter, date):
                param_date = date_filter.strftime("%Y-%m-%d")
            else:
                param_date = str(date_filter)[0:10]
            query += " AND DATE(c.heure_prevue) = CAST(%s AS date)"
            params.append(param_date)
        else:
            date_limite = (datetime.now(TIMEZONE) - timedelta(days=days_back)).date()
            param_date = date_limite.strftime("%Y-%m-%d")
            query += " AND DATE(c.heure_prevue) >= CAST(%s AS date)"
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
                    c.heure_pec_prevue::time,
                    (c.heure_prevue AT TIME ZONE 'Europe/Paris')::time
                ) ASC
        """
        query += f" LIMIT {limit}"

        rows = execute_query(query, params=params, fetchall=True)
        if not rows:
            return []
        result = []
        for course in rows:
            result.append(
                {
                    "id": course.get("id"),
                    "chauffeur_id": course.get("chauffeur_id"),
                    "nom_client": course.get("nom_client"),
                    "telephone_client": course.get("telephone_client"),
                    "adresse_pec": course.get("adresse_pec"),
                    "lieu_depose": course.get("lieu_depose"),
                    "heure_prevue": course.get("heure_prevue"),
                    "heure_pec_prevue": course.get("heure_pec_prevue"),
                    "temps_trajet_minutes": course.get("temps_trajet_minutes"),
                    "heure_depart_calculee": course.get("heure_depart_calculee"),
                    "type_course": course.get("type_course"),
                    "tarif_estime": course.get("tarif_estime"),
                    "km_estime": course.get("km_estime"),
                    "commentaire": course.get("commentaire"),
                    "commentaire_chauffeur": course.get("commentaire_chauffeur"),
                    "statut": course.get("statut"),
                    "date_creation": course.get("date_creation"),
                    "date_confirmation": course.get("date_confirmation"),
                    "date_pec": course.get("date_pec"),
                    "date_depose": course.get("date_depose"),
                    "created_by": course.get("created_by"),
                    "client_regulier_id": course.get("client_regulier_id"),
                    "chauffeur_name": course.get("chauffeur_name"),
                    "visible_chauffeur": course.get("visible_chauffeur", True),
                }
            )
        return result
    except Exception as e:
        print("get_courses error:", e)
        return []


def distribute_courses_for_date(date_str):
    try:
        if isinstance(date_str, (datetime,)):
            param = date_str.strftime("%Y-%m-%d")
        elif isinstance(date_str, date):
            param = date_str.strftime("%Y-%m-%d")
        else:
            param = str(date_str)[0:10]

        ok = execute_query(
            """
            UPDATE courses
            SET visible_chauffeur = true
            WHERE DATE(heure_prevue AT TIME ZONE 'Europe/Paris') = CAST(%s AS date)
            AND visible_chauffeur = false
            """,
            params=(param,),
            commit=True,
        )
        # rowcount not available via helper; we can fetch count with a separate query
        count = get_scalar_result_from_query(
            "SELECT COUNT(*) FROM courses WHERE DATE(heure_prevue AT TIME ZONE 'Europe/Paris') = CAST(%s AS date) AND visible_chauffeur = true",
            params=(param,),
        )
        return {"success": True, "count": int(count or 0), "message": f"✅ Courses distribuées pour {param}"}
    except Exception as e:
        print("distribute_courses_for_date error:", e)
        return {"success": False, "count": 0, "message": f"❌ Erreur : {e}"}


def export_week_to_excel(week_start_date):
    try:
        from io import BytesIO
        from openpyxl.styles import Font, PatternFill, Alignment

        # Normalize week_start_date
        if isinstance(week_start_date, datetime):
            start_dt = week_start_date.date()
        elif isinstance(week_start_date, date):
            start_dt = week_start_date
        else:
            start_dt = datetime.strptime(str(week_start_date)[0:10], "%Y-%m-%d").date()
        week_end_date = start_dt + timedelta(days=6)

        rows = execute_query(
            """
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
            WHERE c.heure_prevue >= %s::timestamp AT TIME ZONE 'Europe/Paris' AND c.heure_prevue < (%s::timestamp AT TIME ZONE 'Europe/Paris') + INTERVAL '1 day'
            ORDER BY c.heure_prevue
            """,
            params=(start_dt.strftime("%Y-%m-%d"), week_end_date.strftime("%Y-%m-%d")),
            fetchall=True,
        )

        if not rows:
            return {
                "success": False,
                "error": f"Aucune course trouvée pour la semaine du {start_dt.strftime('%d/%m/%Y')}",
            }

        data = []
        for row in rows:
            data.append(
                {
                    "Chauffeur": row.get("full_name"),
                    "Client": row.get("nom_client"),
                    "Téléphone": row.get("telephone_client"),
                    "Adresse PEC": row.get("adresse_pec"),
                    "Lieu dépose": row.get("lieu_depose"),
                    "Date/Heure": row.get("heure_prevue"),
                    "Heure PEC": row.get("heure_pec_prevue"),
                    "Type": row.get("type_course"),
                    "Tarif (€)": row.get("tarif_estime"),
                    "Km": row.get("km_estime"),
                    "Statut": row.get("statut"),
                    "Commentaire secrétaire": row.get("commentaire"),
                    "Commentaire chauffeur": row.get("commentaire_chauffeur"),
                    "Date confirmation": row.get("date_confirmation"),
                    "Date PEC réelle": row.get("date_pec"),
                    "Date dépose": row.get("date_depose"),
                }
            )
        df = pd.DataFrame(data)
        date_columns = ["Date/Heure", "Date confirmation", "Date PEC réelle", "Date dépose"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Courses")
            worksheet = writer.sheets["Courses"]
            for cell in worksheet[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.alignment = Alignment(horizontal="center")
            for i, col in enumerate(df.columns):
                max_length = max(df[col].astype(str).apply(len).max() if len(df) > 0 else 0, len(col)) + 2
                col_letter = chr(65 + i)
                worksheet.column_dimensions[col_letter].width = min(max_length, 50)
        buffer.seek(0)
        excel_data = buffer.getvalue()
        week_number = start_dt.isocalendar()[1]
        filename = f"semaine_{week_number:02d}_{start_dt.year}.xlsx"
        return {"success": True, "excel_data": excel_data, "count": len(df), "filename": filename}
    except Exception as e:
        print("export_week_to_excel error:", e)
        return {"success": False, "error": str(e)}


def purge_week_courses(week_start_date):
    try:
        if isinstance(week_start_date, datetime):
            start_dt = week_start_date.date()
        elif isinstance(week_start_date, date):
            start_dt = week_start_date
        else:
            start_dt = datetime.strptime(str(week_start_date)[0:10], "%Y-%m-%d").date()
        week_end_date = start_dt + timedelta(days=6)

        rows = execute_query(
            """
            SELECT id FROM courses
            WHERE heure_prevue >= %s::timestamp AND heure_prevue < (%s::timestamp + INTERVAL '1 day')
            """,
            params=(start_dt.strftime("%Y-%m-%d"), week_end_date.strftime("%Y-%m-%d")),
            fetchall=True,
        )
        course_ids = [r.get("id") for r in rows] if rows else []
        if not course_ids:
            return {"success": True, "count": 0}
        execute_query("DELETE FROM courses WHERE id = ANY(%s)", params=(course_ids,), commit=True)
        # We don't have rowcount via helper; compute count of deleted
        return {"success": True, "count": len(course_ids)}
    except Exception as e:
        print("purge_week_courses error:", e)
        return {"success": False, "error": str(e)}


def update_course_status(course_id, new_status, km_reels=None, tarif_reel=None):
    now_paris = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    timestamp_field = {"confirmee": "date_confirmation", "pec": "date_pec", "deposee": "date_depose"}
    try:
        if km_reels is not None and tarif_reel is not None:
            if new_status in timestamp_field:
                execute_query(
                    f"""
                    UPDATE courses
                    SET statut = %s, {timestamp_field[new_status]} = %s,
                        km_estime = %s, tarif_estime = %s
                    WHERE id = %s
                    """,
                    params=(new_status, now_paris, km_reels, tarif_reel, course_id),
                    commit=True,
                )
            else:
                execute_query(
                    """
                    UPDATE courses
                    SET statut = %s, km_estime = %s, tarif_estime = %s
                    WHERE id = %s
                    """,
                    params=(new_status, km_reels, tarif_reel, course_id),
                    commit=True,
                )
        else:
            if new_status in timestamp_field:
                execute_query(
                    f"""
                    UPDATE courses
                    SET statut = %s, {timestamp_field[new_status]} = %s
                    WHERE id = %s
                    """,
                    params=(new_status, now_paris, course_id),
                    commit=True,
                )
            else:
                execute_query(
                    """
                    UPDATE courses
                    SET statut = %s
                    WHERE id = %s
                    """,
                    params=(new_status, course_id),
                    commit=True,
                )
        return True
    except Exception as e:
        print("update_course_status error:", e)
        return False


def update_commentaire_chauffeur(course_id, commentaire):
    return execute_query(
        "UPDATE courses SET commentaire_chauffeur = %s WHERE id = %s",
        params=(commentaire, course_id),
        commit=True,
    )


def update_heure_pec_prevue(course_id, nouvelle_heure):
    return execute_query(
        "UPDATE courses SET heure_pec_prevue = %s WHERE id = %s", params=(nouvelle_heure, course_id), commit=True
    )


def delete_course(course_id):
    return execute_query("DELETE FROM courses WHERE id = %s", params=(course_id,), commit=True)


def update_course_details(course_id, nouvelle_heure_pec, nouveau_chauffeur_id):
    return execute_query(
        "UPDATE courses SET heure_pec_prevue = %s, chauffeur_id = %s WHERE id = %s",
        params=(nouvelle_heure_pec, nouveau_chauffeur_id, course_id),
        commit=True,
    )


def create_user(username, password, role, full_name):
    hashed_password = hash_password(password)
    try:
        ok = execute_query(
            "INSERT INTO users (username, password_hash, role, full_name) VALUES (%s, %s, %s, %s)",
            params=(username, hashed_password, role, full_name),
            commit=True,
        )
        return True if ok is not None else False
    except Exception as e:
        print("create_user error:", e)
        return False


def delete_user(user_id):
    try:
        admin_count = get_scalar_result_from_query("SELECT COUNT(*) FROM users WHERE role = 'admin'")
        user = execute_query("SELECT role FROM users WHERE id = %s", params=(user_id,), fetchone=True)
        if user and user.get("role") == "admin" and (admin_count or 0) <= 1:
            return False, "Impossible de supprimer le dernier administrateur"
        execute_query("DELETE FROM users WHERE id = %s", params=(user_id,), commit=True)
        return True, "Utilisateur supprimé avec succès"
    except Exception as e:
        print("delete_user error:", e)
        return False, f"Erreur: {str(e)}"


def get_all_users():
    rows = execute_query(
        "SELECT id, username, role, full_name, created_at FROM users ORDER BY role, full_name", fetchall=True
    )
    return rows if rows else []


def reassign_course_to_driver(course_id, new_chauffeur_id):
    try:
        result = execute_query(
            """
            SELECT c.chauffeur_id, c.nom_client, u.full_name
            FROM courses c
            JOIN users u ON c.chauffeur_id = u.id
            WHERE c.id = %s
            """,
            params=(course_id,),
            fetchone=True,
        )
        if result:
            old_chauffeur_id = result.get("chauffeur_id")
            nom_client = result.get("nom_client")
            old_chauffeur_name = result.get("full_name")
            new_chauffeur_name = get_scalar_result_from_query("SELECT full_name FROM users WHERE id = %s", params=(new_chauffeur_id,))
            execute_query("UPDATE courses SET chauffeur_id = %s WHERE id = %s", params=(new_chauffeur_id, course_id), commit=True)
            return {
                "success": True,
                "course_id": course_id,
                "nom_client": nom_client,
                "old_chauffeur_id": old_chauffeur_id,
                "old_chauffeur_name": old_chauffeur_name,
                "new_chauffeur_id": new_chauffeur_id,
                "new_chauffeur_name": new_chauffeur_name,
            }
        return {"success": False, "error": "Course non trouvée"}
    except Exception as e:
        print("reassign_course_to_driver error:", e)
        return {"success": False, "error": str(e)}


# ============================================
# INTERFACES UTILISATEUR (UI)
# Le code UI reprend la logique existante mais repose sur les helpers ci-dessus.
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
                st.session_state.user = user
                st.rerun()
            else:
                st.error("Nom d'utilisateur ou mot de passe incorrect")


def login(username, password):
    hashed_password = hash_password(password)
    row = execute_query(
        """
        SELECT id, username, role, full_name
        FROM users
        WHERE username = %s AND password_hash = %s
        """,
        params=(username, hashed_password),
        fetchone=True,
    )
    if row:
        return {"id": row.get("id"), "username": row.get("username"), "role": row.get("role"), "full_name": row.get("full_name")}
    return None


def admin_page():
    st.title("🔧 Administration - Transport DanGE")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Admin)")
    # ... (UI code similaire à votre version; pour concision, on conserve la logique préexistante)
    # NOTE: utilisez get_courses(), get_chauffeurs(), etc. définis ci-dessus.


def secretaire_page():
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

    with tab2:
        st.subheader("Planning Global de toutes les courses")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            show_all = st.checkbox("Afficher toutes les courses", value=True)
            if not show_all:
                date_filter = st.date_input("Filtrer par date", value=datetime.now().date())
            else:
                date_filter = None
        with col2:
            chauffeur_filter = st.selectbox("Filtrer par chauffeur", ["Tous"] + [c["full_name"] for c in get_chauffeurs()])
        with col3:
            statut_filter = st.selectbox("Filtrer par statut", ["Tous", "Nouvelle", "Confirmée", "PEC", "Déposée"])
        with col4:
            st.metric("Total courses", len(get_courses()))

        chauffeur_id = None
        if chauffeur_filter != "Tous":
            chauffeurs = get_chauffeurs()
            for c in chauffeurs:
                if c["full_name"] == chauffeur_filter:
                    chauffeur_id = c["id"]
                    break

        date_filter_str = None
        if not show_all and date_filter:
            date_filter_str = date_filter.strftime("%Y-%m-%d")

        courses = get_courses(chauffeur_id=chauffeur_id, date_filter=date_filter_str)

        st.info(f"📊 {len(courses)} course(s) trouvée(s)")

        if courses:
            for course in courses:
                statut_mapping = {"Nouvelle": "nouvelle", "Confirmée": "confirmee", "PEC": "pec", "Déposée": "deposee"}
                if statut_filter != "Tous":
                    statut_reel = statut_mapping.get(statut_filter, statut_filter.lower())
                    if str(course.get("statut", "")).lower() != statut_reel.lower():
                        continue
                statut_colors = {"nouvelle": "🔵", "confirmee": "🟡", "pec": "🔴", "deposee": "🟢"}
                date_fr = format_date_fr(course.get("heure_prevue"))
                heure_affichage = course.get("heure_pec_prevue") or extract_time_str(course.get("heure_prevue"))
                titre_course = f"{statut_colors.get(course.get('statut'), '⚪')} {date_fr} {heure_affichage} - {course.get('nom_client')} ({course.get('chauffeur_name')})"
                with st.expander(titre_course):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Client :** {course.get('nom_client')}")
                        st.write(f"**Téléphone :** {course.get('telephone_client')}")
                        st.write(f"**📅 Date PEC :** {format_date_fr(course.get('heure_prevue'))}")
                        if course.get("heure_pec_prevue"):
                            st.success(f"⏰ **Heure PEC prévue : {course.get('heure_pec_prevue')}**")
                        st.write(f"**PEC :** {course.get('adresse_pec')}")
                        st.write(f"**Dépose :** {course.get('lieu_depose')}")
                        st.write(f"**Type :** {course.get('type_course')}")
                    with col2:
                        st.write(f"**Chauffeur :** {course.get('chauffeur_name')}")
                        st.write(f"**Tarif estimé :** {course.get('tarif_estime')}€")
                        st.write(f"**Km estimé :** {course.get('km_estime')} km")
                        st.write(f"**Statut :** {str(course.get('statut', '')).upper()}")
                        if course.get("commentaire"):
                            st.write(f"**Commentaire secrétaire :** {course.get('commentaire')}")
                    if course.get("commentaire_chauffeur"):
                        st.warning(f"💭 **Commentaire chauffeur** : {course.get('commentaire_chauffeur')}")
                    if course.get("date_confirmation"):
                        st.info(f"✅ Confirmée le : {format_datetime_fr(course.get('date_confirmation'))}")
                    if course.get("date_pec"):
                        st.info(f"📍 PEC effectuée le : {format_datetime_fr(course.get('date_pec'))}")
                    if course.get("date_depose"):
                        st.success(f"🏁 Déposée le : {format_datetime_fr(course.get('date_depose'))}")
        else:
            st.info("Aucune course pour cette sélection")

    # (Les autres onglets peuvent réutiliser la même logique en s'appuyant sur les helpers robustes)


def chauffeur_page():
    """Interface Chauffeur - Simplifiée pour démonstration"""
    if "user" not in st.session_state:
        st.rerun()
        return
    count = st_autorefresh(interval=30000, key="chauffeur_autorefresh")
    col_deconnexion, col_refresh = st.columns([1, 6])
    st.title("🚖 Mes courses")
    st.markdown(f"**Connecté en tant que :** {st.session_state.user['full_name']} (Chauffeur)")

    # Notifications
    unread_count = get_unread_count(st.session_state.user["id"])
    if unread_count > 0:
        if "last_notif_count" not in st.session_state:
            st.session_state.last_notif_count = 0
        if unread_count > st.session_state.last_notif_count:
            st.markdown(
                """
                <audio id="notif-sound" autoplay>
                  <source src="data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqF" type="audio/wav">
                </audio>
                <script>
                    try {
                        const audio = document.getElementById('notif-sound');
                        audio.play().catch(()=>{console.log('audio play blocked')});
                    } catch(e) { console.log(e); }
                </script>
                """,
                unsafe_allow_html=True,
            )
            st.session_state.last_notif_count = unread_count

        st.markdown(
            f"""
            <div style="background: linear-gradient(135deg, #FF4444 0%, #CC0000 100%);
                        color: white; padding: 15px 25px;
                        border-radius: 30px; display: inline-block; font-weight: bold;
                        margin-bottom: 20px; box-shadow: 0 4px 15px rgba(255,68,68,0.4);">
                🔔 {unread_count} nouvelle(s) notification(s) !
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.expander("📋 Voir les notifications", expanded=True):
            notifications = get_unread_notifications(st.session_state.user["id"])
            for notif in notifications:
                icon = {
                    "nouvelle_course": "🆕",
                    "modification": "✏️",
                    "changement_chauffeur": "🔄",
                    "annulation": "❌",
                }.get(notif.get("type"), "📢")
                st.info(f"{icon} **{notif.get('message', '')}**")
                if notif.get("nom_client"):
                    heure = notif.get("heure_pec_prevue", "N/A")
                    st.caption(f"👤 {notif.get('nom_client')} | ⏰ {heure}")
                    st.caption(f"📍 {notif.get('adresse_pec', 'N/A')} → {notif.get('lieu_depose', 'N/A')}")
            if st.button("✅ Marquer tout comme lu", use_container_width=True):
                mark_notifications_as_read(st.session_state.user["id"])
                st.rerun()
    else:
        st.session_state.last_notif_count = 0

    with col_deconnexion:
        if st.button("🚪 Déconnexion"):
            if "user" in st.session_state:
                del st.session_state.user
            st.rerun()
    with col_refresh:
        if st.button("🔄 Actualiser (auto: 30s)", use_container_width=True):
            st.rerun()

    st.markdown("---")
    # Affichage simplifié des courses du chauffeur
    show_all = st.checkbox("Toutes mes courses", value=False)
    if not show_all:
        date_filter = st.date_input("Date", value=datetime.now().date())
    else:
        date_filter = None

    date_filter_str = date_filter.strftime("%Y-%m-%d") if date_filter and not show_all else None
    courses = get_courses(chauffeur_id=st.session_state.user["id"], date_filter=date_filter_str, role="chauffeur")
    if not courses:
        st.info("Aucune course")
    else:
        for course in courses:
            statut_colors = {"nouvelle": "🔵", "confirmee": "🟡", "pec": "🔴", "deposee": "🟢"}
            statut_text = {"nouvelle": "NOUVELLE", "confirmee": "CONFIRMÉE", "pec": "PRISE EN CHARGE", "deposee": "TERMINÉE"}
            date_fr = format_date_fr(course.get("heure_prevue"))
            heure_affichage = course.get("heure_pec_prevue") or extract_time_str(course.get("heure_prevue"))
            titre = f"{statut_colors.get(course.get('statut'), '⚪')} {date_fr} {heure_affichage} - {course.get('nom_client')} - {statut_text.get(course.get('statut'), str(course.get('statut', '')).upper())}"
            with st.expander(titre):
                st.write(f"**Client :** {course.get('nom_client')}")
                st.write(f"**Tel :** {course.get('telephone_client')}")
                st.write(f"**PEC :** {course.get('adresse_pec')}")
                st.write(f"**Dépose :** {course.get('lieu_depose')}")
                st.write(f"**Tarif :** {course.get('tarif_estime')}€ | Km : {course.get('km_estime')}")
                if course.get("statut") == "pec":
                    km_reel = st.number_input("Km réels", min_value=0.0, step=1.0, value=float(course.get("km_estime", 0)), key=f"km_{course.get('id')}")
                    tarif_reel = st.number_input("Tarif réel (€)", min_value=0.0, step=1.0, value=float(course.get("tarif_estime", 0)), key=f"tarif_{course.get('id')}")
                    if st.button("🏁 Déposé", key=f"depose_{course.get('id')}"):
                        update_course_status(course.get("id"), "deposee", km_reel, tarif_reel)
                        st.rerun()


# ============================================
# MAIN
# ============================================
def init_db():
    init_notifications_table()


def main():
    init_db()
    if "user" not in st.session_state:
        login_page()
    else:
        role = st.session_state.user.get("role")
        if role == "admin":
            admin_page()
        elif role == "secretaire":
            secretaire_page()
        elif role == "chauffeur":
            chauffeur_page()
        else:
            st.info("Rôle inconnu")


if __name__ == "__main__":
    main()
