# 🚖 Transport DanGE - Planning V2

**Version 2.0** - Application de gestion de planning pour taxis avec corrections des bugs de validation

## 🎯 Nouvelles fonctionnalités V2

### ✅ Corrections majeures

1. **Bug validation multiple CORRIGÉ**
   - Suppression du cache `@st.cache_data`
   - Une seule validation nécessaire pour confirmer/PEC/déposer
   - Une seule validation nécessaire pour supprimer

2. **Tri chronologique intelligent**
   - Courses triées par heure PEC prévue (ordre croissant)
   - Si pas d'heure PEC : tri par heure de création
   - Application dans toutes les interfaces (chauffeur, secrétaire, admin)

3. **Thème sombre optimisé**
   - Interface fluide et moderne
   - Meilleure lisibilité sur mobile

## 📋 Fonctionnalités principales

- ✅ Gestion multi-utilisateurs (admin, secrétaire, chauffeur)
- ✅ Création et suivi de courses en temps réel
- ✅ Planning du jour avec vue par chauffeur
- ✅ Système de distribution de courses aux chauffeurs
- ✅ Commentaires bidirectionnels secrétaire ↔ chauffeur
- ✅ Statuts de course : Nouvelle → Confirmée → PEC → Déposée
- ✅ Base de données PostgreSQL (Supabase)
- ✅ Assistant intelligent de suggestion de chauffeur (Google Maps API)

## 🚀 Déploiement sur Streamlit Cloud

### Étape 1 : Créer le repository GitHub

1. Aller sur [GitHub](https://github.com)
2. Cliquer sur **"New repository"**
3. Nom : `taxi-planning-v2`
4. Visibilité : **Private** (recommandé)
5. Ne pas initialiser avec README
6. Créer le repository

### Étape 2 : Pousser le code

```bash
cd /chemin/vers/taxi-planning-v2

git init
git add .
git commit -m "Initial commit - Planning V2"
git remote add origin https://github.com/VOTRE_USERNAME/taxi-planning-v2.git
git branch -M main
git push -u origin main
```

### Étape 3 : Déployer sur Streamlit Cloud

1. Aller sur [share.streamlit.io](https://share.streamlit.io)
2. Se connecter avec votre compte GitHub
3. Cliquer sur **"New app"**
4. Sélectionner :
   - Repository : `taxi-planning-v2`
   - Branch : `main`
   - Main file path : `app.py`
5. Cliquer sur **"Advanced settings"**
6. Ajouter les **secrets** (voir section suivante)
7. Cliquer sur **"Deploy!"**

### Étape 4 : Configurer les secrets

Dans **Advanced settings → Secrets**, copier-coller :

```toml
[supabase]
host = "aws-1-eu-west-1.pooler.supabase.com"
database = "postgres"
user = "postgres.vrmcphtxqwsuwefmzuca"
password = "TransportDanGE2024!"
port = "5432"

[google_maps]
api_key = "AIzaSyDqJAjyskUxRDSdyl-4UP7m_hqiZ-a5qAg"
```

## 📱 Utilisation

### Interface Chauffeur
- Voir les courses du jour (triées chronologiquement)
- Confirmer les courses
- Signaler la prise en charge (PEC)
- Signaler la dépose
- Ajouter des commentaires pour la secrétaire

### Interface Secrétaire
- Créer de nouvelles courses
- Assigner les courses aux chauffeurs
- Distribuer les courses du lendemain
- Modifier les détails des courses
- Voir les commentaires des chauffeurs

### Interface Admin
- Toutes les fonctionnalités secrétaire
- Gestion des comptes utilisateurs
- Statistiques globales
- Export des données en CSV

## 🔧 Technologies utilisées

- **Frontend** : Streamlit
- **Base de données** : PostgreSQL (Supabase)
- **Géolocalisation** : Google Maps Distance Matrix API
- **Déploiement** : Streamlit Cloud
- **Langage** : Python 3.11

## 📝 Structure du projet

```
taxi-planning-v2/
├── app.py              # Application principale
├── assistant.py        # Module assistant intelligent
├── requirements.txt    # Dépendances Python
├── .gitignore         # Fichiers à ignorer par Git
├── .streamlit/
│   └── config.toml    # Configuration Streamlit (thème)
└── README.md          # Ce fichier
```

## 🆘 Support

En cas de problème :
1. Vérifier que les secrets sont bien configurés
2. Vérifier la connexion à Supabase
3. Vérifier que la clé API Google Maps est valide
4. Consulter les logs dans Streamlit Cloud

## 📄 Licence

© 2025 Transport DanGE - Tous droits réservés

---

**Version** : 2.0  
**Dernière mise à jour** : Décembre 2025  
**Développé pour** : Transport DanGE (Eure-et-Loir)
