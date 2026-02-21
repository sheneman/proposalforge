function confirmFullSync(event) {
    if (!confirm('Start a full sync? This will re-fetch all opportunities from Grants.gov and may take several minutes.')) {
        event.preventDefault();
        event.stopPropagation();
        return false;
    }
    // Scroll to show the progress area
    document.getElementById('sync-live').scrollIntoView({ behavior: 'smooth', block: 'start' });
    return true;
}
