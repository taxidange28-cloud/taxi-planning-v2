"""
Transport DanGE - Application de Planning
Point d'entrée principal - Architecture refactorisée

Version : 2.0 Refactorisée
Date : Décembre 2025
"""

import streamlit as st
from config import settings

# Configuration de la page
st.set_page_config(
    page_title=settings.APP_TITLE,
    page_icon=settings.APP_ICON,
    layout=settings.APP_LAYOUT
)

# Initialisation session state
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

# Routing
if not st.session_state.logged_in:
    # Page de connexion
    from pages import login
    login.show()
    
else:
    # Pages selon rôle
    role = st.session_state.get('role')
    
    if role == 'admin':
        from pages import admin
        admin.show()
        
    elif role == 'secretaire':
        from pages import secretaire
        secretaire.show()
        
    elif role == 'chauffeur':
        from pages import chauffeur
        chauffeur.show()
        
    else:
        st.error("❌ Rôle non reconnu")
        if st.button("🚪 Déconnexion"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
