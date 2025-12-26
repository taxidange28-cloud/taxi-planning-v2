"""
ASSISTANT INTELLIGENT - MODULE 2
Transport DanGE Planning

Fonctions pour suggérer automatiquement le meilleur chauffeur
basé sur distance, charge de travail, et disponibilité.

Utilise Google Maps Distance Matrix API pour calculs de distance réels.
"""

import requests
from datetime import datetime, timedelta
import pytz

# Configuration
TIMEZONE = pytz.timezone('Europe/Paris')


def calculate_distance(origin, destination, api_key):
    """
    Calcule la distance et le temps de trajet entre 2 adresses.
    
    Args:
        origin (str): Adresse de départ (ex: "Dangeau, France")
        destination (str): Adresse d'arrivée (ex: "Chartres, France")
        api_key (str): Clé API Google Maps
        
    Returns:
        dict: {
            'distance_km': float,      # Distance en kilomètres
            'distance_meters': int,    # Distance en mètres
            'duration_min': int,       # Durée en minutes
            'duration_seconds': int,   # Durée en secondes
            'success': bool,           # True si succès
            'error': str or None       # Message d'erreur si échec
        }
    """
    
    # URL de l'API Google Maps Distance Matrix
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    
    # Paramètres de la requête
    params = {
        'origins': origin,
        'destinations': destination,
        'key': api_key,
        'language': 'fr',
        'units': 'metric'
    }
    
    try:
        # Appel API
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Lève exception si erreur HTTP
        
        data = response.json()
        
        # Vérifier le statut de la réponse
        if data.get('status') != 'OK':
            return {
                'success': False,
                'error': f"API Error: {data.get('status')} - {data.get('error_message', 'Unknown error')}"
            }
        
        # Extraire les données du premier résultat
        element = data['rows'][0]['elements'][0]
        
        if element.get('status') != 'OK':
            return {
                'success': False,
                'error': f"Route Error: {element.get('status')}"
            }
        
        # Extraire distance et durée
        distance_meters = element['distance']['value']
        duration_seconds = element['duration']['value']
        
        return {
            'distance_km': round(distance_meters / 1000, 2),
            'distance_meters': distance_meters,
            'duration_min': round(duration_seconds / 60),
            'duration_seconds': duration_seconds,
            'success': True,
            'error': None
        }
        
    except requests.exceptions.Timeout:
        return {
            'success': False,
            'error': 'Timeout: API took too long to respond'
        }
    except requests.exceptions.RequestException as e:
        return {
            'success': False,
            'error': f'Request Error: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Unexpected Error: {str(e)}'
        }


# ============ FONCTIONS À AJOUTER DANS LES PROCHAINES ÉTAPES ============

def calculate_driver_score(driver_data, course_data, api_key):
    """
    Calcule le score d'un chauffeur pour une course donnée.
    
    Args:
        driver_data (dict): {
            'id': int,
            'name': str,
            'last_course': dict or None,  # Dernière course du chauffeur
            'courses_today': int,          # Nombre de courses aujourd'hui
        }
        course_data (dict): {
            'adresse_pec': str,
            'heure_prevue': datetime,
            'lieu_depose': str
        }
        api_key (str): Clé API Google Maps
        
    Returns:
        dict: {
            'driver_id': int,
            'driver_name': str,
            'score': int (0-100),
            'distance_km': float,
            'duration_min': int,
            'courses_today': int,
            'details': str,  # Explication du score
            'available': bool
        }
    """
    
    score = 0
    details = []
    distance_km = None
    duration_min = None
    
    # ============ CRITÈRE 1 : DISTANCE (40 points max) ============
    
    if driver_data.get('last_course'):
        # Le chauffeur a une dernière course
        last_depose = driver_data['last_course'].get('lieu_depose', '')
        
        if last_depose:
            # Calculer distance entre dernière dépose et nouvelle PEC
            dist_result = calculate_distance(
                origin=last_depose,
                destination=course_data['adresse_pec'],
                api_key=api_key
            )
            
            if dist_result['success']:
                distance_km = dist_result['distance_km']
                duration_min = dist_result['duration_min']
                
                # Score inversement proportionnel à la distance
                # 0-10 km = 40 points
                # 10-20 km = 30 points
                # 20-30 km = 20 points
                # 30-50 km = 10 points
                # >50 km = 0 points
                
                if distance_km <= 10:
                    distance_score = 40
                elif distance_km <= 20:
                    distance_score = 30
                elif distance_km <= 30:
                    distance_score = 20
                elif distance_km <= 50:
                    distance_score = 10
                else:
                    distance_score = 0
                
                score += distance_score
                details.append(f"Distance: {distance_km} km ({distance_score} pts)")
            else:
                # Erreur de calcul, score neutre
                details.append(f"Distance: non calculée (20 pts par défaut)")
                score += 20
        else:
            # Pas d'adresse de dépose, score neutre
            details.append("Distance: pas de dernière dépose (20 pts)")
            score += 20
    else:
        # Pas de dernière course = chauffeur disponible à sa base
        # On considère que c'est bien (score moyen)
        details.append("Pas de course précédente (25 pts)")
        score += 25
    
    # ============ CRITÈRE 2 : CHARGE DE TRAVAIL (30 points max) ============
    
    courses_today = driver_data.get('courses_today', 0)
    
    # Score inversement proportionnel au nombre de courses
    # 0-2 courses = 30 points
    # 3-4 courses = 20 points
    # 5-6 courses = 10 points
    # 7+ courses = 0 points
    
    if courses_today <= 2:
        workload_score = 30
    elif courses_today <= 4:
        workload_score = 20
    elif courses_today <= 6:
        workload_score = 10
    else:
        workload_score = 0
    
    score += workload_score
    details.append(f"Charge: {courses_today} courses ({workload_score} pts)")
    
    # ============ CRITÈRE 3 : DISPONIBILITÉ HORAIRE (30 points max) ============
    
    # Pour l'instant, on suppose toujours disponible
    # TODO: Vérifier les conflits horaires dans une version future
    availability_score = 30
    score += availability_score
    details.append(f"Disponibilité: OK ({availability_score} pts)")
    
    # ============ RÉSULTAT FINAL ============
    
    return {
        'driver_id': driver_data['id'],
        'driver_name': driver_data['name'],
        'score': score,
        'distance_km': distance_km,
        'duration_min': duration_min,
        'courses_today': courses_today,
        'details': " | ".join(details),
        'available': True  # Pour l'instant toujours True
    }


def suggest_best_driver(chauffeurs, course_data, api_key):
    """
    Suggère le meilleur chauffeur pour une course.
    
    Args:
        chauffeurs (list): Liste de dicts avec infos chauffeurs
            Chaque dict doit contenir :
            {
                'id': int,
                'name': str,
                'last_course': dict or None,
                'courses_today': int
            }
        course_data (dict): {
            'adresse_pec': str,
            'heure_prevue': datetime,
            'lieu_depose': str
        }
        api_key (str): Clé API Google Maps
        
    Returns:
        list: Liste de scores triés par ordre décroissant
            Chaque élément contient le résultat de calculate_driver_score()
    """
    
    scores = []
    
    # Calculer le score pour chaque chauffeur
    for chauffeur in chauffeurs:
        score_result = calculate_driver_score(
            driver_data=chauffeur,
            course_data=course_data,
            api_key=api_key
        )
        scores.append(score_result)
    
    # Trier par score décroissant (meilleur d'abord)
    scores.sort(key=lambda x: x['score'], reverse=True)
    
    return scores


# ============ FONCTION DE TEST ============

def test_api():
    """
    Fonction de test pour vérifier que l'API fonctionne.
    
    Usage:
        python assistant.py
    """
    
    print("=" * 70)
    print("TEST GOOGLE MAPS API - ASSISTANT INTELLIGENT")
    print("=" * 70)
    print()
    
    # Clé API Google Maps (configurée automatiquement)
    API_KEY = "AIzaSyDqJAjyskUxRDSdyl-4UP7m_hqiZ-a5qAg"
    
    # ========== TEST 1 : Fonction calculate_distance() ==========
    
    print("📏 TEST 1 : Calcul de distance")
    print("-" * 70)
    
    result = calculate_distance(
        origin="Dangeau, France",
        destination="Chartres, France",
        api_key=API_KEY
    )
    
    if result['success']:
        print(f"✅ Dangeau → Chartres")
        print(f"   Distance : {result['distance_km']} km")
        print(f"   Durée    : {result['duration_min']} minutes")
    else:
        print(f"❌ Échec : {result['error']}")
    
    print()
    
    # ========== TEST 2 : Fonction suggest_best_driver() ==========
    
    print("🎯 TEST 2 : Suggestion chauffeur intelligent")
    print("-" * 70)
    print()
    
    # Scénario réaliste : Nouvelle course à 14h30
    print("📋 SCÉNARIO :")
    print("   Client : M. Durand")
    print("   PEC : Dangeau, Place de l'Église")
    print("   Dépose : Chartres Gare")
    print("   Heure : 14h30")
    print()
    
    # Données fictives des chauffeurs (comme dans ta vraie app)
    chauffeurs = [
        {
            'id': 1,
            'name': 'Franck',
            'last_course': {
                'lieu_depose': 'Illiers-Combray, France'  # Vient de déposer à Illiers
            },
            'courses_today': 6  # Déjà 6 courses aujourd'hui
        },
        {
            'id': 2,
            'name': 'Laurence',
            'last_course': {
                'lieu_depose': 'Brou, France'  # Vient de déposer à Brou
            },
            'courses_today': 4  # 4 courses aujourd'hui
        },
        {
            'id': 3,
            'name': 'Dunois',
            'last_course': None,  # Pas de course précédente (à sa base)
            'courses_today': 2  # Seulement 2 courses
        }
    ]
    
    # Nouvelle course
    course_data = {
        'adresse_pec': 'Dangeau, Place de l\'Église',
        'heure_prevue': datetime.now(TIMEZONE),
        'lieu_depose': 'Chartres Gare'
    }
    
    # Appel de la fonction de suggestion
    print("🔄 Calcul des scores...")
    print()
    
    suggestions = suggest_best_driver(
        chauffeurs=chauffeurs,
        course_data=course_data,
        api_key=API_KEY
    )
    
    # Affichage des résultats
    print("📊 RÉSULTATS (classement par score) :")
    print()
    
    for i, sug in enumerate(suggestions, 1):
        emoji = "✅" if i == 1 else "⚠️" if i == 2 else "❌"
        
        print(f"{emoji} #{i} - {sug['driver_name']} : {sug['score']}/100 points")
        print(f"      {sug['details']}")
        
        if sug['distance_km']:
            print(f"      Distance depuis dernière course : {sug['distance_km']} km ({sug['duration_min']} min)")
        
        print()
    
    # Recommandation finale
    best = suggestions[0]
    print("=" * 70)
    print(f"💡 RECOMMANDATION : Assigner à {best['driver_name']}")
    print(f"   Score : {best['score']}/100")
    print(f"   Raison : {best['details']}")
    print("=" * 70)
    print()
    print("✅ FIN DES TESTS - Toutes les fonctions opérationnelles !")
    print("=" * 70)


# Si le fichier est exécuté directement
if __name__ == "__main__":
    test_api()
