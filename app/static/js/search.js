// Search page functionality

function filterAgencies(query) {
    const items = document.querySelectorAll('.agency-item');
    const lowerQuery = query.toLowerCase();
    items.forEach(item => {
        const label = item.querySelector('label').textContent.toLowerCase();
        item.style.display = label.includes(lowerQuery) ? '' : 'none';
    });
}

function updateSort(value) {
    const [sortBy, sortOrder] = value.split(':');
    const form = document.getElementById('search-filters');
    form.querySelector('[name="sort_by"]').value = sortBy;
    form.querySelector('[name="sort_order"]').value = sortOrder;
    form.querySelector('[name="page"]').value = '1';
    htmx.trigger(form, 'change');
}

function goToPage(page) {
    const form = document.getElementById('search-filters');
    form.querySelector('[name="page"]').value = page;
    htmx.trigger(form, 'change');
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// Collect checkbox values into comma-separated strings for the form submission
document.addEventListener('htmx:configRequest', function(event) {
    const form = document.getElementById('search-filters');
    if (!form) return;

    // Collect status checkboxes
    const statuses = Array.from(form.querySelectorAll('[name="status"]:checked')).map(cb => cb.value);
    if (statuses.length > 0) {
        event.detail.parameters['status'] = statuses.join(',');
    } else {
        delete event.detail.parameters['status'];
    }

    // Collect agency checkboxes
    const agencies = Array.from(form.querySelectorAll('[name="agency"]:checked')).map(cb => cb.value);
    if (agencies.length > 0) {
        event.detail.parameters['agency'] = agencies.join(',');
    } else {
        delete event.detail.parameters['agency'];
    }

    // Collect category checkboxes
    const categories = Array.from(form.querySelectorAll('[name="category"]:checked')).map(cb => cb.value);
    if (categories.length > 0) {
        event.detail.parameters['category'] = categories.join(',');
    } else {
        delete event.detail.parameters['category'];
    }

    // Remove empty q parameter
    if (!event.detail.parameters['q']) {
        delete event.detail.parameters['q'];
    }
});

// Reset page to 1 when filters change (not pagination)
document.addEventListener('change', function(event) {
    if (event.target.closest('#search-filters') && event.target.name !== 'page') {
        const pageInput = document.getElementById('page-input');
        if (pageInput) pageInput.value = '1';
    }
});
