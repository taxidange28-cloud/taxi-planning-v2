// Service Worker pour Firebase Cloud Messaging
// Ce fichier gère les notifications en arrière-plan

importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-messaging-compat.js');

// Configuration Firebase
const firebaseConfig = {
  apiKey: "AIzaSyCe08U4nEDIK9COhMUAWmz8YuxoCluZKfY",
  authDomain: "transport-dange.firebaseapp.com",
  projectId: "transport-dange",
  storageBucket: "transport-dange.firebasestorage.app",
  messagingSenderId: "86580303208",
  appId: "1:86580303208:web:fc2e8da737045a29dbf2dd"
};

// Initialiser Firebase dans le Service Worker
firebase.initializeApp(firebaseConfig);

// Récupérer l'instance de messaging
const messaging = firebase.messaging();

// Gérer les notifications en arrière-plan
messaging.onBackgroundMessage((payload) => {
  console.log('📬 Notification reçue en arrière-plan:', payload);
  
  const notificationTitle = payload.notification.title || '🆕 Nouvelle course';
  const notificationOptions = {
    body: payload.notification.body,
    icon: '/favicon.ico',
    badge: '/favicon.ico',
    tag: 'nouvelle-course',
    requireInteraction: true,
    vibrate: [200, 100, 200],
    data: payload.data
  };

  return self.registration.showNotification(notificationTitle, notificationOptions);
});

// Gérer le clic sur la notification
self.addEventListener('notificationclick', (event) => {
  console.log('🖱️ Notification cliquée:', event);
  
  event.notification.close();
  
  // Ouvrir ou focus sur l'app
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true })
      .then((clientList) => {
        // Si l'app est déjà ouverte, la mettre au premier plan
        for (let i = 0; i < clientList.length; i++) {
          const client = clientList[i];
          if (client.url.includes('streamlit.app') && 'focus' in client) {
            return client.focus();
          }
        }
        // Sinon, ouvrir une nouvelle fenêtre
        if (clients.openWindow) {
          return clients.openWindow('https://taxi-planning-v2-8fwzy8lvarakaqlnvbiwhx.streamlit.app/');
        }
      })
  );
});
