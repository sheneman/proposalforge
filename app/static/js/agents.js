// ─── Agents Page JavaScript ──────────────────────────

let activeRunId = null;
let progressInterval = null;

// ─── Agent Configuration ─────────────────────────────

async function saveAgent(slug) {
    const form = document.querySelector(`.agent-config-form[data-slug="${slug}"]`);
    if (!form) return;

    const data = {
        enabled: document.getElementById(`enabled-${slug}`).checked,
        llm_base_url: form.querySelector('[name="llm_base_url"]').value || null,
        llm_model: form.querySelector('[name="llm_model"]').value || null,
        llm_api_key: form.querySelector('[name="llm_api_key"]').value || null,
        temperature: parseFloat(form.querySelector('[name="temperature"]').value),
        max_tokens: parseInt(form.querySelector('[name="max_tokens"]').value),
        persona: form.querySelector('[name="persona"]').value || null,
        system_prompt: form.querySelector('[name="system_prompt"]').value || null,
        mcp_server_slugs: Array.from(form.querySelectorAll('.mcp-check:checked')).map(cb => cb.value),
    };

    // Don't send empty API key (preserve existing)
    if (!data.llm_api_key) delete data.llm_api_key;

    const resultDiv = document.getElementById(`agent-result-${slug}`);
    try {
        const resp = await fetch(`/agents/api/${slug}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data),
        });
        const result = await resp.json();
        if (resp.ok) {
            resultDiv.innerHTML = '<div class="alert alert-success alert-sm py-1 small">Saved successfully</div>';
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">${result.detail || 'Save failed'}</div>`;
        }
    } catch (e) {
        resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">Error: ${e.message}</div>`;
    }
    setTimeout(() => { resultDiv.innerHTML = ''; }, 3000);
}

async function testAgent(slug) {
    const resultDiv = document.getElementById(`agent-result-${slug}`);
    resultDiv.innerHTML = '<div class="text-muted small"><span class="spinner-border spinner-border-sm"></span> Testing...</div>';

    try {
        const resp = await fetch(`/agents/api/${slug}/test`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({prompt: 'Say hello and confirm you are working. Respond in one sentence.'}),
        });
        const result = await resp.json();
        if (result.success) {
            resultDiv.innerHTML = `<div class="alert alert-success alert-sm py-1 small">
                <strong>Model:</strong> ${result.model}<br>
                <strong>Response:</strong> ${result.response.substring(0, 200)}
            </div>`;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">${result.error}</div>`;
        }
    } catch (e) {
        resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">Error: ${e.message}</div>`;
    }
}

async function resetAgent(slug) {
    if (!confirm(`Reset "${slug}" agent to AGENT.md defaults? This will overwrite all customizations.`)) return;

    const resultDiv = document.getElementById(`agent-result-${slug}`);
    try {
        const resp = await fetch(`/agents/api/${slug}/reset`, {method: 'POST'});
        if (resp.ok) {
            resultDiv.innerHTML = '<div class="alert alert-success alert-sm py-1 small">Reset to defaults. Reloading...</div>';
            setTimeout(() => location.reload(), 1000);
        } else {
            const result = await resp.json();
            resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">${result.detail || 'Reset failed'}</div>`;
        }
    } catch (e) {
        resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">Error: ${e.message}</div>`;
    }
}

// ─── MCP Server Testing ─────────────────────────────

async function testMCPServer(slug) {
    const resultDiv = document.getElementById(`mcp-result-${slug}`);
    resultDiv.innerHTML = '<div class="text-muted small"><span class="spinner-border spinner-border-sm"></span> Testing connection...</div>';

    try {
        const resp = await fetch(`/agents/api/mcp-servers/${slug}/test`, {method: 'POST'});
        const result = await resp.json();
        if (result.success) {
            resultDiv.innerHTML = `<div class="alert alert-success alert-sm py-1 small">
                Connected! Found ${result.tools_count} tools: ${result.tool_names.join(', ')}
            </div>`;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">${result.error}</div>`;
        }
    } catch (e) {
        resultDiv.innerHTML = `<div class="alert alert-danger alert-sm py-1 small">Error: ${e.message}</div>`;
    }
}

// ─── Workflow Execution ──────────────────────────────

async function startMatchmaking() {
    const btn = document.getElementById('btn-start-matching');
    if (btn.disabled) return;

    if (!confirm('Start a new matchmaking workflow run?')) return;

    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Starting...';

    try {
        const resp = await fetch('/agents/api/workflows/matchmaking/run', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({}),
        });
        const result = await resp.json();
        if (result.success) {
            activeRunId = result.run_id;
            showProgressPanel();
            startProgressPolling();
        } else {
            alert(result.error || 'Failed to start workflow');
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-play-fill"></i> Run Matchmaking';
        }
    } catch (e) {
        alert('Error: ' + e.message);
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-play-fill"></i> Run Matchmaking';
    }
}

function showProgressPanel() {
    const card = document.getElementById('live-progress-card');
    if (card) card.style.display = 'block';
}

function hideProgressPanel() {
    const card = document.getElementById('live-progress-card');
    if (card) card.style.display = 'none';
}

function startProgressPolling() {
    if (progressInterval) clearInterval(progressInterval);
    progressInterval = setInterval(pollProgress, 2500);
}

async function pollProgress() {
    if (!activeRunId) {
        stopProgressPolling();
        return;
    }

    try {
        const resp = await fetch(`/agents/api/workflows/runs/${activeRunId}/progress`);
        const data = await resp.json();

        const phaseEl = document.getElementById('progress-phase');
        const detailsEl = document.getElementById('progress-details');
        const barEl = document.getElementById('progress-bar');

        if (phaseEl) phaseEl.textContent = data.phase || 'Working...';
        if (detailsEl) {
            let details = `Status: ${data.status}`;
            if (data.matches_produced !== undefined) details += ` | Matches: ${data.matches_produced}`;
            detailsEl.textContent = details;
        }

        // Estimate progress based on phase
        const phaseProgress = {
            'initializing': 5, 'planning': 15, 'planning_complete': 20,
            'discovery_complete': 35, 'pre_filter_complete': 50,
            'match_complete': 70, 'critique_complete': 80,
            'summarize_complete': 90, 'done': 100,
        };
        const pct = phaseProgress[data.phase] || phaseProgress[data.status] || 50;
        if (barEl) barEl.style.width = pct + '%';

        if (data.status === 'completed' || data.status === 'failed' || data.status === 'cancelled') {
            stopProgressPolling();
            hideProgressPanel();
            const btn = document.getElementById('btn-start-matching');
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="bi bi-play-fill"></i> Run Matchmaking';
            }
            refreshRuns();
        }
    } catch (e) {
        console.error('Progress poll error:', e);
    }
}

function stopProgressPolling() {
    if (progressInterval) {
        clearInterval(progressInterval);
        progressInterval = null;
    }
}

async function cancelRun() {
    if (!activeRunId) return;
    if (!confirm('Cancel the running workflow?')) return;

    try {
        await fetch(`/agents/api/workflows/${activeRunId}/cancel`, {method: 'POST'});
    } catch (e) {
        console.error('Cancel error:', e);
    }
}

// ─── Workflow Runs ───────────────────────────────────

async function refreshRuns() {
    try {
        const resp = await fetch('/agents/partial/run-table');
        const html = await resp.text();
        document.getElementById('runs-table-body').innerHTML = html;
    } catch (e) {
        console.error('Refresh runs error:', e);
    }
}

async function loadRunDetail(runId) {
    const panel = document.getElementById('run-detail-panel');
    const content = document.getElementById('run-detail-content');
    panel.style.display = 'block';
    content.innerHTML = '<div class="text-center py-3"><span class="spinner-border spinner-border-sm"></span> Loading...</div>';

    try {
        const resp = await fetch(`/agents/api/workflows/runs/${runId}`);
        const data = await resp.json();

        let html = '';

        // Run info
        const run = data.run;
        html += `<div class="mb-3">
            <span class="badge bg-${run.status === 'completed' ? 'success' : run.status === 'failed' ? 'danger' : 'secondary'}">${run.status}</span>
            <span class="small text-muted ms-2">Trigger: ${run.trigger}</span>
        </div>`;

        if (run.output_summary) {
            const summary = run.output_summary;
            html += `<div class="small mb-3">
                <strong>Results:</strong>
                Matches: ${summary.matches_produced || 0} |
                Pairs: ${summary.candidate_pairs || 0} |
                Researchers: ${summary.researchers_processed || 0} |
                Opportunities: ${summary.opportunities_processed || 0} |
                Iterations: ${summary.iterations || 0}
            </div>`;
        }

        if (run.error_message) {
            html += `<div class="alert alert-danger py-1 small">${run.error_message}</div>`;
        }

        // Steps
        if (data.steps && data.steps.length > 0) {
            html += '<h6 class="mt-3">Steps</h6>';
            html += '<div class="list-group list-group-flush">';
            for (const step of data.steps) {
                const statusIcon = {
                    'completed': '<i class="bi bi-check-circle-fill text-success"></i>',
                    'failed': '<i class="bi bi-x-circle-fill text-danger"></i>',
                    'running': '<span class="spinner-border spinner-border-sm text-primary"></span>',
                    'skipped': '<i class="bi bi-skip-forward-fill text-muted"></i>',
                }[step.status] || '<i class="bi bi-circle text-muted"></i>';

                html += `<div class="list-group-item px-2 py-1">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            ${statusIcon}
                            <strong class="small ms-1">${step.node_name}</strong>
                            <span class="text-muted small ms-1">(${step.agent_slug})</span>
                        </div>
                        <div class="small text-muted">
                            ${step.duration_ms ? step.duration_ms + 'ms' : ''}
                            ${step.token_count ? ' | ' + step.token_count + ' tokens' : ''}
                        </div>
                    </div>
                    ${step.error_message ? `<div class="text-danger small mt-1">${step.error_message}</div>` : ''}
                    ${step.llm_model_used ? `<div class="text-muted small">${step.llm_model_used}</div>` : ''}
                </div>`;
            }
            html += '</div>';
        }

        // Top matches
        if (data.matches && data.matches.length > 0) {
            html += '<h6 class="mt-3">Top Matches</h6>';
            html += '<div class="table-responsive"><table class="table table-sm small">';
            html += '<thead><tr><th>Researcher</th><th>Opportunity</th><th>Score</th><th>Confidence</th></tr></thead><tbody>';
            for (const m of data.matches.slice(0, 10)) {
                html += `<tr>
                    <td>${m.researcher_id}</td>
                    <td>${m.opportunity_id}</td>
                    <td><strong>${m.overall_score.toFixed(1)}</strong></td>
                    <td><span class="badge bg-${m.confidence === 'high' ? 'success' : m.confidence === 'medium' ? 'warning' : 'secondary'}">${m.confidence}</span></td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        content.innerHTML = html;
    } catch (e) {
        content.innerHTML = `<div class="alert alert-danger py-1 small">Error loading run detail: ${e.message}</div>`;
    }
}

function closeRunDetail() {
    document.getElementById('run-detail-panel').style.display = 'none';
}

// ─── Init ────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    // Check if a run is currently active
    const isRunning = document.getElementById('btn-start-matching')?.disabled;
    if (isRunning) {
        // Try to find the active run
        fetch('/agents/api/workflows/runs').then(r => r.json()).then(runs => {
            const running = runs.find(r => r.status === 'running');
            if (running) {
                activeRunId = running.id;
                showProgressPanel();
                startProgressPolling();
            }
        }).catch(() => {});
    }
});
