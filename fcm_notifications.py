"""
Module de gestion des notifications Firebase Cloud Messaging (FCM)
pour l'application Transport DanGE

VERSION STREAMLIT CLOUD
"""

from pyfcm import FCMNotification
import os
import streamlit as st
import json
import tempfile


# ============================================
# INITIALISATION FCM CLIENT - STREAMLIT CLOUD
# ============================================
_fcm_client = None

def get_fcm_client():
    """
    Récupère ou crée le client FCM (singleton)
    VERSION STREAMLIT CLOUD - Lit depuis st.secrets
    
    Returns:
        FCMNotification: Client FCM initialisé
    """
    global _fcm_client
    
    if _fcm_client is None:
        try:
            # ============================================
            # STREAMLIT CLOUD : Lire depuis secrets
            # ============================================
            if 'firebase' in st.secrets and 'service_account' in st.secrets['firebase']:
                print("📱 Lecture credentials Firebase depuis Streamlit Secrets...")
                
                # Parser le JSON depuis les secrets
                firebase_config = json.loads(st.secrets['firebase']['service_account'])
                project_id = firebase_config.get('project_id')
                
                # Créer un fichier temporaire (nécessaire pour PyFCM)
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                    json.dump(firebase_config, f)
                    temp_service_account_path = f.name
                
                print(f"✅ Fichier temporaire créé : {temp_service_account_path}")
                
                # Initialiser le client FCM
                _fcm_client = FCMNotification(
                    service_account_file=temp_service_account_path,
                    project_id=project_id
                )
                
                print(f"✅ FCM Client initialisé avec projet: {project_id}")
                
            # ============================================
            # SERVEUR LOCAL : Lire depuis fichier (fallback)
            # ============================================
            else:
                print("📁 Secrets Streamlit non trouvés, recherche fichier local...")
                
                possible_paths = [
                    "./secrets/firebase-adminsdk.json",
                    os.path.expanduser("~/secrets/firebase-adminsdk.json"),
                    "./firebase-adminsdk.json"
                ]
                
                service_account_file = None
                for path in possible_paths:
                    if os.path.exists(path):
                        service_account_file = path
                        break
                
                if not service_account_file:
                    print("⚠️ Fichier Firebase service account non trouvé")
                    print("💡 Pour Streamlit Cloud : Ajoutez le JSON dans Settings → Secrets")
                    return None
                
                # Lire le project_id depuis le fichier
                with open(service_account_file, 'r') as f:
                    firebase_config = json.load(f)
                    project_id = firebase_config.get('project_id')
                
                # Initialiser le client FCM
                _fcm_client = FCMNotification(
                    service_account_file=service_account_file,
                    project_id=project_id
                )
                
                print(f"✅ FCM Client initialisé avec projet: {project_id}")
            
        except Exception as e:
            print(f"❌ Erreur initialisation FCM: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    return _fcm_client


# ============================================
# FONCTIONS D'ENVOI DE NOTIFICATIONS
# ============================================

def send_nouvelle_course_notification(fcm_token, course_data):
    """
    Envoie une notification de nouvelle course à un chauffeur
    
    Args:
        fcm_token (str): Token FCM du chauffeur
        course_data (dict): Données de la course
            - nom_client (str)
            - heure_pec (str)
            - adresse_pec (str)
            - lieu_depose (str)
            - tarif (float)
            - km (float)
            - course_id (int)
    
    Returns:
        dict: Résultat de l'envoi
    """
    fcm = get_fcm_client()
    
    if not fcm or not fcm_token:
        return {"success": False, "error": "FCM non disponible ou token manquant"}
    
    try:
        title = "🆕 Nouvelle course !"
        body = f"{course_data['nom_client']} - {course_data['heure_pec']}"
        
        # Configuration Android avec SON et VIBRATION
        android_config = {
            "notification": {
                "sound": "default",
                "channel_id": "courses_urgentes",
                "priority": "high",
                "default_vibrate_timings": True
            }
        }
        
        # Configuration iOS avec SON
        apns_config = {
            "payload": {
                "aps": {
                    "sound": "default",
                    "badge": 1
                }
            }
        }
        
        # Données supplémentaires
        data_payload = {
            "type": "nouvelle_course",
            "course_id": str(course_data['course_id']),
            "nom_client": course_data['nom_client'],
            "adresse_pec": course_data['adresse_pec'],
            "lieu_depose": course_data['lieu_depose'],
            "tarif": str(course_data['tarif']),
            "km": str(course_data['km'])
        }
        
        # Envoyer la notification
        result = fcm.notify(
            fcm_token=fcm_token,
            notification_title=title,
            notification_body=body,
            android_config=android_config,
            apns_config=apns_config,
            data_payload=data_payload
        )
        
        print(f"✅ Notification FCM envoyée : {result}")
        
        return {
            "success": True,
            "result": result
        }
        
    except Exception as e:
        print(f"❌ Erreur envoi notification FCM: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def send_modification_course_notification(fcm_token, course_data):
    """
    Envoie une notification de modification de course
    
    Args:
        fcm_token (str): Token FCM du chauffeur
        course_data (dict): Données de la course modifiée
    
    Returns:
        dict: Résultat de l'envoi
    """
    fcm = get_fcm_client()
    
    if not fcm or not fcm_token:
        return {"success": False, "error": "FCM non disponible"}
    
    try:
        title = "✏️ Course modifiée"
        body = f"{course_data['nom_client']} - Nouvelle heure: {course_data['heure_pec']}"
        
        data_payload = {
            "type": "modification_course",
            "course_id": str(course_data['course_id'])
        }
        
        result = fcm.notify(
            fcm_token=fcm_token,
            notification_title=title,
            notification_body=body,
            data_payload=data_payload,
            android_config={
                "notification": {
                    "sound": "default",
                    "priority": "default"
                }
            }
        )
        
        return {"success": True, "result": result}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_annulation_course_notification(fcm_token, course_data):
    """
    Envoie une notification d'annulation de course
    
    Args:
        fcm_token (str): Token FCM du chauffeur
        course_data (dict): Données de la course annulée
    
    Returns:
        dict: Résultat de l'envoi
    """
    fcm = get_fcm_client()
    
    if not fcm or not fcm_token:
        return {"success": False, "error": "FCM non disponible"}
    
    try:
        title = "❌ Course annulée"
        body = f"{course_data['nom_client']} - Course annulée"
        
        data_payload = {
            "type": "annulation_course",
            "course_id": str(course_data['course_id'])
        }
        
        result = fcm.notify(
            fcm_token=fcm_token,
            notification_title=title,
            notification_body=body,
            data_payload=data_payload,
            android_config={
                "notification": {
                    "sound": "default",
                    "priority": "high"
                }
            }
        )
        
        return {"success": True, "result": result}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================
# FONCTION UTILITAIRE
# ============================================

def update_chauffeur_fcm_token(chauffeur_id, new_fcm_token):
    """
    Met à jour le token FCM d'un chauffeur dans la DB
    
    Args:
        chauffeur_id (int): ID du chauffeur
        new_fcm_token (str): Nouveau token FCM
    
    Returns:
        bool: True si succès
    """
    try:
        # Import dynamique pour éviter circular import
        import sys
        if 'app' in sys.modules:
            from app import get_db_connection, release_db_connection
        else:
            print("⚠️ Module app non chargé, impossible de mettre à jour le token")
            return False
        
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users
            SET fcm_token = %s
            WHERE id = %s
        ''', (new_fcm_token, chauffeur_id))
        
        conn.commit()
        release_db_connection(conn)
        
        print(f"✅ Token FCM mis à jour pour chauffeur {chauffeur_id}")
        return True
        
    except Exception as e:
        print(f"❌ Erreur mise à jour token FCM: {e}")
        return False
