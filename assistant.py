# -*- coding: utf-8 -*-
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
import time
import functools
import logging

logger = logging.getLogger("assistant")
TIMEZONE = pytz.timezone('Europe/Paris')

# Simple cache en mémoire pour éviter d'appeler plusieurs fois la même paire (TTL)
_distance_cache = {}
_CACHE_TTL_SECONDS = 60 * 60  # 1 heure


def _cache_get(key):
    entry = _distance_cache.get(key)
    if not entry:
        return None
    value, ts = entry
    if time.time() - ts > _CACHE_TTL_SECONDS:
        del _distance_cache[key]
        return None
    return value


def _cache_set(key, value):
    _distance_cache[key] = (value, time.time())


def calculate_distance(origin, destination, api_key, max_retries=2):
    """
    Calcule la distance et le temps de trajet entre 2 adresses.
    Utilise Google Distance Matrix API. Retourne dict avec success flag.
    """
    # Vérifier cache
    cache_key = f"{origin}__{destination}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        'origins': origin,
        'destinations': destination,
        'key': api_key,
        'language': 'fr',
        'units': 'metric'
    }

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=8)
            response.raise_for_status()
            data = response.json()

            if data.get('status') != 'OK':
                logger.warning("Google API status != OK: %s", data.get('status'))
                return {'success': False, 'error': f"API Error: {data.get('status')} - {data.get('error_message', 'Unknown')}"}

            element = data['rows'][0]['elements'][0]
            if element.get('status') != 'OK':
                return {'success': False, 'error': f"Route Error: {element.get('status')}"}

            distance_meters = element['distance']['value']
            duration_seconds = element['duration']['value']
            result = {
                'distance_km': round(distance_meters / 1000, 2),
                'distance_meters': distance_meters,
                'duration_min': round(duration_seconds / 60),
                'duration_seconds': duration_seconds,
                'success': True,
                'error': None
            }
            _cache_set(cache_key, result)
            return result

        except requests.exceptions.Timeout:
            logger.warning("Timeout calculate_distance attempt %d for %s -> %s", attempt, origin, destination)
            if attempt < max_retries:
                time.sleep(1 + attempt)
                continue
            return {'success': False, 'error': 'Timeout: API took too long to respond'}
        except requests.exceptions.RequestException as e:
            logger.exception("RequestException in calculate_distance")
            if attempt < max_retries:
                time.sleep(1 + attempt)
                continue
            return {'success': False, 'error': f'Request Error: {str(e)}'}
        except Exception as e:
            logger.exception("Unexpected error in calculate_distance")
            return {'success': False, 'error': f'Unexpected Error: {str(e)}'}

    return {'success': False, 'error': 'Échec inconnu'}


def calculate_driver_score(driver_data, course_data, api_key):
    score = 0
    details = []
    distance_km = None
    duration_min = None

    # DISTANCE (40)
    if driver_data.get('last_course'):
        last_depose = driver_data['last_course'].get('lieu_depose', '')
        if last_depose:
            dist_result = calculate_distance(origin=last_depose, destination=course_data['adresse_pec'], api_key=api_key)
            if dist_result.get('success'):
                distance_km = dist_result['distance_km']
                duration_min = dist_result['duration_min']
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
                details.append("Distance: non calculée (20 pts)")
                score += 20
        else:
            details.append("Distance: pas de dernière dépose (20 pts)")
            score += 20
    else:
        details.append("Pas de course précédente (25 pts)")
        score += 25

    # CHARGE (30)
    courses_today = driver_data.get('courses_today', 0)
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

    # DISPONIBILITÉ (30) - placeholder (améliorer ultérieurement)
    availability_score = 30
    score += availability_score
    details.append(f"Disponibilité: OK ({availability_score} pts)")

    return {
        'driver_id': driver_data['id'],
        'driver_name': driver_data['name'],
        'score': score,
        'distance_km': distance_km,
        'duration_min': duration_min,
        'courses_today': courses_today,
        'details': " | ".join(details),
        'available': True
    }


def suggest_best_driver(chauffeurs, course_data, api_key):
    scores = []
    for chauffeur in chauffeurs:
        score_result = calculate_driver_score(driver_data=chauffeur, course_data=course_data, api_key=api_key)
        scores.append(score_result)
    scores.sort(key=lambda x: x['score'], reverse=True)
    return scores


def test_api():
    """
    Fonction de test : NE PAS mettre de clé API dans le code source !
    Utilisez la variable d'environnement ou st.secrets dans Streamlit.
    """
    print("Test assistant - pas de clé incluse dans le fichier.")
    # Exemple : lire la clé depuis une variable d'environnement (non fournie ici)
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("Aucune clé API configurée dans la variable d'environnement GOOGLE_MAPS_API_KEY.")
        return
    # ... exécuter quelques appels de test si la clé est présente.


if __name__ == "__main__":
    test_api()
