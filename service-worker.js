self.addEventListener('push', function(event) {
    const options = {
        body: event.data.text(),
        icon: 'https://api.dicebear.com/6.x/avataaars/svg?seed=default',
        badge: 'https://api.dicebear.com/6.x/avataaars/svg?seed=default'
    };
    
    event.waitUntil(
        self.registration.showNotification('New Message', options)
    );
});
