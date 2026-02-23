// Researcher search page interactions

function filterDepartments(query) {
    const items = document.querySelectorAll('.dept-item');
    const lower = query.toLowerCase();
    items.forEach(item => {
        const label = item.querySelector('label');
        if (label) {
            item.style.display = label.textContent.toLowerCase().includes(lower) ? '' : 'none';
        }
    });
}

function updateResearcherSort(value) {
    const [sortBy, sortOrder] = value.split(':');
    const form = document.getElementById('researcher-filters');
    if (!form) return;

    const sortByInput = form.querySelector('input[name="sort_by"]');
    const sortOrderInput = form.querySelector('input[name="sort_order"]');
    if (sortByInput) sortByInput.value = sortBy;
    if (sortOrderInput) sortOrderInput.value = sortOrder;

    // Reset page to 1
    const pageInput = form.querySelector('#page-input');
    if (pageInput) pageInput.value = '1';

    htmx.trigger(form, 'change');
}

function goToResearcherPage(page) {
    const form = document.getElementById('researcher-filters');
    if (!form) return;

    const pageInput = form.querySelector('#page-input');
    if (pageInput) pageInput.value = page;

    htmx.trigger(form, 'change');
}
