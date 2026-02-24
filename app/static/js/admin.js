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
    // Find the sibling input dynamically (works for all settings forms)
    const inputGroup = btn.closest('.input-group');
    const input = inputGroup ? inputGroup.querySelector('input[type="password"], input[type="text"].api-key-input, input[name="api_key"]') : null;
    // Fallback to legacy id
    const target = input || document.getElementById('llm-api-key');
    if (!target) return;

    if (target.type === 'password') {
        target.type = 'text';
        btn.innerHTML = '<i class="bi bi-eye-slash"></i>';
    } else {
        target.type = 'password';
        btn.innerHTML = '<i class="bi bi-eye"></i>';
    }
}

async function testLlmConnection() {
    const result = document.getElementById('llm-test-result');
    result.innerHTML = '<span class="text-muted"><i class="bi bi-hourglass-split"></i> Testing...</span>';

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

async function testEmbeddingConnection() {
    const result = document.getElementById('embed-test-result');
    result.innerHTML = '<span class="text-muted"><i class="bi bi-hourglass-split"></i> Testing...</span>';

    const payload = {
        base_url: (document.getElementById('embed-base-url') || {}).value || '',
        model: (document.getElementById('embed-model') || {}).value || '',
        api_key: (document.getElementById('embed-api-key') || {}).value || '',
    };

    try {
        const resp = await fetch('/admin/embedding/test', {
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

async function testRerankerConnection() {
    const result = document.getElementById('reranker-test-result');
    result.innerHTML = '<span class="text-muted"><i class="bi bi-hourglass-split"></i> Testing...</span>';

    const payload = {
        base_url: (document.getElementById('reranker-base-url') || {}).value || '',
        model: (document.getElementById('reranker-model') || {}).value || '',
        api_key: (document.getElementById('reranker-api-key') || {}).value || '',
    };

    try {
        const resp = await fetch('/admin/reranker/test', {
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
