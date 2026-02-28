// ─── Agents Page JavaScript ──────────────────────────

let activeRunId = null;
let eventSource = null;
let logVisible = false;

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

// ─── MCP Server Toggle ──────────────────────────────

async function toggleMCPServer(slug, enabled) {
    const label = document.querySelector(`label[for="mcp-enabled-${slug}"]`);
    try {
        const resp = await fetch(`/agents/api/mcp-servers/${slug}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled }),
        });
        if (resp.ok) {
            if (label) label.textContent = enabled ? 'Enabled' : 'Disabled';
        } else {
            // Revert toggle on failure
            const checkbox = document.getElementById(`mcp-enabled-${slug}`);
            if (checkbox) checkbox.checked = !enabled;
            if (label) label.textContent = !enabled ? 'Enabled' : 'Disabled';
            alert('Failed to update server: ' + (await resp.text()));
        }
    } catch (e) {
        const checkbox = document.getElementById(`mcp-enabled-${slug}`);
        if (checkbox) checkbox.checked = !enabled;
        alert('Error: ' + e.message);
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
            startLogStream();
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
    // Clear previous log entries
    const logContainer = document.getElementById('log-container');
    if (logContainer) logContainer.innerHTML = '';
}

function hideProgressPanel() {
    const card = document.getElementById('live-progress-card');
    if (card) card.style.display = 'none';
}

// ─── SSE Log Streaming ──────────────────────────────

let progressFallbackInterval = null;

function startLogStream() {
    stopLogStream();
    if (!activeRunId) return;

    eventSource = new EventSource(`/agents/api/workflows/runs/${activeRunId}/logs/stream`);

    eventSource.onmessage = function(e) {
        try {
            const event = JSON.parse(e.data);
            appendLogEntry(event);
            updateProgressFromEvent(event);

            if (event.type === 'workflow_end') {
                onWorkflowFinished(event);
            }
        } catch (err) {
            console.error('SSE parse error:', err);
        }
    };

    eventSource.onerror = function() {
        // SSE connection lost — check if workflow ended
        setTimeout(() => {
            if (activeRunId) {
                checkRunStatus();
            }
        }, 2000);
    };

    // Fallback: poll progress endpoint every 10s to catch zombie/stuck runs
    if (progressFallbackInterval) clearInterval(progressFallbackInterval);
    progressFallbackInterval = setInterval(() => {
        if (activeRunId) checkRunStatus();
    }, 10000);
}

async function checkRunStatus() {
    if (!activeRunId) return;
    try {
        const resp = await fetch(`/agents/api/workflows/runs/${activeRunId}/progress`);
        const data = await resp.json();
        if (data.status === 'completed' || data.status === 'failed' || data.status === 'cancelled') {
            onWorkflowFinished(data);
        }
    } catch (e) {
        // ignore
    }
}

function stopLogStream() {
    if (eventSource) {
        eventSource.close();
        eventSource = null;
    }
    if (progressFallbackInterval) {
        clearInterval(progressFallbackInterval);
        progressFallbackInterval = null;
    }
}

// ─── Log Rendering ──────────────────────────────────

const LOG_COLORS = {
    node_start: '#56b6c2',    // cyan
    node_end: '#56b6c2',
    llm_request: '#e5c07b',   // orange/gold
    llm_response: '#98c379',  // green
    info: '#abb2bf',          // light gray
    error: '#e06c75',         // red
    cancel: '#e06c75',
    workflow_start: '#c678dd', // purple
    workflow_end: '#c678dd',
};

function appendLogEntry(event) {
    const container = document.getElementById('log-container');
    if (!container) return;

    const color = LOG_COLORS[event.type] || '#abb2bf';
    const ts = event.ts ? new Date(event.ts).toLocaleTimeString() : '';
    const agent = event.agent ? `[${event.agent}]` : '';
    const node = event.node ? `<${event.node}>` : '';

    let line = document.createElement('div');
    line.style.borderBottom = '1px solid rgba(255,255,255,0.05)';
    line.style.padding = '2px 0';

    let html = `<span style="color:#636d83">${ts}</span> `;
    html += `<span style="color:${color};font-weight:600">${event.type}</span> `;
    if (node) html += `<span style="color:#61afef">${node}</span> `;
    if (agent) html += `<span style="color:#d19a66">${agent}</span> `;
    html += `<span style="color:#e0e0e0">${escapeHtml(event.message || '')}</span>`;

    // Duration and tokens inline
    if (event.duration_ms) html += ` <span style="color:#636d83">${event.duration_ms}ms</span>`;
    if (event.tokens) html += ` <span style="color:#636d83">${event.tokens}tok</span>`;

    // Expandable detail
    if (event.detail) {
        const detailId = 'detail-' + Math.random().toString(36).substr(2, 9);
        html += ` <a href="#" style="color:#61afef;text-decoration:none;font-size:0.7rem" onclick="event.preventDefault();document.getElementById('${detailId}').style.display=document.getElementById('${detailId}').style.display==='none'?'block':'none'">[detail]</a>`;
        html += `<div id="${detailId}" style="display:none;margin:4px 0 4px 20px;padding:6px 8px;background:rgba(255,255,255,0.05);border-radius:4px;white-space:pre-wrap;word-break:break-all;color:#98c379;font-size:0.72rem;max-height:200px;overflow-y:auto">`;
        if (event.detail.prompt_preview) html += `<strong style="color:#e5c07b">Prompt:</strong>\n${escapeHtml(event.detail.prompt_preview)}\n\n`;
        if (event.detail.response_preview) html += `<strong style="color:#98c379">Response:</strong>\n${escapeHtml(event.detail.response_preview)}`;
        if (event.detail.model) html += `\n<strong style="color:#abb2bf">Model:</strong> ${escapeHtml(event.detail.model)}`;
        html += '</div>';
    }

    line.innerHTML = html;
    container.appendChild(line);

    // Auto-scroll to bottom
    container.scrollTop = container.scrollHeight;
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// ─── Progress Updates from SSE Events ────────────────

const NODE_PROGRESS = {
    'plan': 10, 'discover': 25, 'pre_filter': 40,
    'match': 60, 'critique': 75, 'summarize': 88, 'persist': 95,
};

function updateProgressFromEvent(event) {
    const phaseEl = document.getElementById('progress-phase');
    const barEl = document.getElementById('progress-bar');
    const detailsEl = document.getElementById('progress-details');

    if (event.type === 'node_start' && event.node) {
        if (phaseEl) phaseEl.textContent = `Running: ${event.node}`;
        const pct = NODE_PROGRESS[event.node] || 50;
        if (barEl) barEl.style.width = pct + '%';
    } else if (event.type === 'node_end' && event.node) {
        if (phaseEl) phaseEl.textContent = `Completed: ${event.node}`;
    } else if (event.type === 'info') {
        if (detailsEl) detailsEl.textContent = event.message || '';
    } else if (event.type === 'workflow_end') {
        if (phaseEl) phaseEl.textContent = event.message || 'Done';
        if (barEl) barEl.style.width = '100%';
    } else if (event.type === 'cancel') {
        if (phaseEl) phaseEl.textContent = 'Cancelled';
    }
}

function onWorkflowFinished(event) {
    stopLogStream();

    // Hide spinner in header
    const spinner = document.getElementById('progress-spinner');
    if (spinner) spinner.style.display = 'none';

    // Update the progress bar to 100%
    const barEl = document.getElementById('progress-bar');
    if (barEl) {
        barEl.style.width = '100%';
        if (event.type === 'cancel' || event.status === 'cancelled') {
            barEl.className = 'progress-bar bg-warning';
        } else if (event.status === 'failed') {
            barEl.className = 'progress-bar bg-danger';
        } else {
            barEl.className = 'progress-bar bg-success';
        }
    }

    // Re-enable start button
    const btn = document.getElementById('btn-start-matching');
    if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-play-fill"></i> Run Matchmaking';
    }

    // Hide cancel button
    const cancelBtn = document.getElementById('btn-cancel');
    if (cancelBtn) cancelBtn.style.display = 'none';

    // Refresh the runs table
    refreshRuns();

    // Hide progress panel after a delay (so user can read final logs)
    setTimeout(() => {
        hideProgressPanel();
        // Reset bar
        if (barEl) barEl.className = 'progress-bar bg-navy';
        if (spinner) spinner.style.display = '';
        if (cancelBtn) cancelBtn.style.display = '';
        activeRunId = null;
    }, 5000);
}

// ─── Cancel ─────────────────────────────────────────

async function cancelRun() {
    if (!activeRunId) return;
    if (!confirm('Cancel the running workflow?')) return;

    const cancelBtn = document.getElementById('btn-cancel');
    if (cancelBtn) {
        cancelBtn.disabled = true;
        cancelBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Cancelling...';
    }

    try {
        await fetch(`/agents/api/workflows/${activeRunId}/cancel`, {method: 'POST'});
    } catch (e) {
        console.error('Cancel error:', e);
    }
}

// ─── Log Toggle ─────────────────────────────────────

function toggleLogDetail() {
    const container = document.getElementById('log-container');
    const btn = document.getElementById('btn-toggle-log');
    if (!container) return;

    logVisible = !logVisible;
    container.style.display = logVisible ? 'block' : 'none';
    if (btn) {
        btn.innerHTML = logVisible
            ? '<i class="bi bi-terminal-fill"></i> Hide Logs'
            : '<i class="bi bi-terminal"></i> Logs';
    }

    // Scroll to bottom when showing
    if (logVisible) container.scrollTop = container.scrollHeight;
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

        if (run.status === 'completed' && run.output_summary && run.output_summary.matches_produced > 0) {
            html += `<div class="mb-3">
                <a href="/agents/api/workflows/runs/${runId}/matches/csv" class="btn btn-sm btn-outline-success">
                    <i class="bi bi-download"></i> Export All Matches (CSV)
                </a>
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
                startLogStream();
            }
        }).catch(() => {});
    }
});

// Clean up SSE on page unload
window.addEventListener('beforeunload', () => {
    stopLogStream();
});
