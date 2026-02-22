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

function toggleApiKey(btn) {
    const input = document.getElementById('llm-api-key');
    if (input.type === 'password') {
        input.type = 'text';
        btn.innerHTML = '<i class="bi bi-eye-slash"></i>';
    } else {
        input.type = 'password';
        btn.innerHTML = '<i class="bi bi-eye"></i>';
    }
}

async function testLlmConnection() {
    const result = document.getElementById('llm-test-result');
    result.innerHTML = '<span class="text-muted"><i class="bi bi-hourglass-split"></i> Testing...</span>';

    // Send current form values so test works even without saving
    const payload = {
        base_url: (document.getElementById('llm-base-url') || {}).value || '',
        model: (document.getElementById('llm-model') || {}).value || '',
        api_key: (document.getElementById('llm-api-key') || {}).value || '',
    };

    try {
        const resp = await fetch('/admin/llm/test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (data.success) {
            result.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> ' + data.message + '</span>';
        } else {
            result.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> ' + data.message + '</span>';
        }
    } catch (e) {
        result.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> Connection test failed</span>';
    }
}
