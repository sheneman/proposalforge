// Chat with My Data - AI chat interface
(function() {
    'use strict';

    const chatHistory = [];
    let chatChartCounter = 0;

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function renderMarkdown(text) {
        if (!text) return '';
        if (typeof marked !== 'undefined') {
            return marked.parse(text);
        }
        // Fallback if marked.js not loaded
        return escapeHtml(text)
            .replace(/\n/g, '<br>')
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    }

    function scrollToBottom() {
        const container = document.getElementById('chat-messages');
        container.scrollTop = container.scrollHeight;
    }

    function appendUserMessage(text) {
        const container = document.getElementById('chat-messages');
        const bubble = document.createElement('div');
        bubble.className = 'chat-bubble chat-user';
        bubble.textContent = text;
        container.appendChild(bubble);
        scrollToBottom();
    }

    function showTyping() {
        const container = document.getElementById('chat-messages');
        const indicator = document.createElement('div');
        indicator.className = 'chat-bubble chat-assistant chat-typing';
        indicator.id = 'typing-indicator';
        indicator.innerHTML = '<span class="typing-dot"></span><span class="typing-dot"></span><span class="typing-dot"></span>';
        container.appendChild(indicator);
        scrollToBottom();
    }

    function hideTyping() {
        const el = document.getElementById('typing-indicator');
        if (el) el.remove();
    }

    function createSqlToggle(sql) {
        if (!sql) return '';
        const escaped = escapeHtml(sql);
        return `
            <div class="mt-2">
                <a class="small text-muted sql-toggle" href="#" onclick="this.nextElementSibling.classList.toggle('d-none'); return false;">
                    <i class="bi bi-code-slash"></i> Show SQL
                </a>
                <pre class="sql-block d-none mt-1 mb-0"><code>${escaped}</code></pre>
            </div>
        `;
    }

    function appendTextMessage(content, sql) {
        const container = document.getElementById('chat-messages');
        const bubble = document.createElement('div');
        bubble.className = 'chat-bubble chat-assistant';

        bubble.innerHTML = renderMarkdown(content) + createSqlToggle(sql);
        container.appendChild(bubble);
        scrollToBottom();
    }

    function appendSummaryMessage(content, value, label, sql) {
        const container = document.getElementById('chat-messages');
        const bubble = document.createElement('div');
        bubble.className = 'chat-bubble chat-assistant';
        bubble.innerHTML = `
            <div class="chat-summary-value">${escapeHtml(value)}</div>
            <div class="text-muted small mb-2">${escapeHtml(label)}</div>
            ${content ? renderMarkdown(content) : ''}
            ${createSqlToggle(sql)}
        `;
        container.appendChild(bubble);
        scrollToBottom();
    }

    function appendTableMessage(content, columns, rows, sql) {
        const container = document.getElementById('chat-messages');
        const bubble = document.createElement('div');
        bubble.className = 'chat-bubble chat-assistant';

        let tableHtml = '<div class="table-responsive"><table class="table table-sm table-striped mb-2">';
        tableHtml += '<thead><tr>';
        columns.forEach(col => {
            tableHtml += `<th class="small">${escapeHtml(col)}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';
        rows.forEach(row => {
            tableHtml += '<tr>';
            row.forEach(cell => {
                let val = cell;
                if (typeof val === 'number') {
                    val = val > 1000 ? val.toLocaleString() : val;
                }
                tableHtml += `<td class="small">${escapeHtml(String(val != null ? val : ''))}</td>`;
            });
            tableHtml += '</tr>';
        });
        tableHtml += '</tbody></table></div>';

        let contentHtml = content ? renderMarkdown(content) : '';

        bubble.innerHTML = contentHtml + tableHtml + createSqlToggle(sql);
        container.appendChild(bubble);
        scrollToBottom();
    }

    function buildChartOptions(chartType, chartData) {
        var isPolar = (chartType === 'pie' || chartType === 'doughnut');

        if (isPolar) {
            return {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'right',
                        labels: { font: { size: 10 }, boxWidth: 12 },
                    },
                },
            };
        }

        if (chartType === 'line') {
            return {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { grid: { display: false } },
                    y: { grid: { color: 'rgba(0,0,0,0.05)' }, beginAtZero: true },
                },
            };
        }

        if (chartType === 'scatter') {
            return {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { grid: { color: 'rgba(0,0,0,0.05)' } },
                    y: { grid: { color: 'rgba(0,0,0,0.05)' } },
                },
            };
        }

        // Bar chart (default)
        return {
            indexAxis: chartData.labels.length > 6 ? 'y' : 'x',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { display: false }, beginAtZero: true },
                y: {
                    grid: { display: false },
                    ticks: {
                        font: { size: 10 },
                        callback: function(v, i) {
                            var label = this.getLabelForValue(v);
                            if (typeof label === 'string' && label.length > 25) {
                                return label.substring(0, 25) + '...';
                            }
                            return label;
                        },
                    },
                },
            },
        };
    }

    function appendChartMessage(content, chartData, sql, chartType) {
        const container = document.getElementById('chat-messages');
        const bubble = document.createElement('div');
        bubble.className = 'chat-bubble chat-assistant';

        chartType = chartType || 'bar';
        chatChartCounter++;
        const canvasId = 'chat-chart-' + chatChartCounter;

        let contentHtml = content ? renderMarkdown(content) : '';

        bubble.innerHTML = contentHtml +
            `<div class="chat-chart-container"><canvas id="${canvasId}"></canvas></div>` +
            createSqlToggle(sql);
        container.appendChild(bubble);
        scrollToBottom();

        // Render chart after DOM insertion
        requestAnimationFrame(function() {
            const ctx = document.getElementById(canvasId);
            if (ctx) {
                new Chart(ctx, {
                    type: chartType,
                    data: chartData,
                    options: buildChartOptions(chartType, chartData),
                });
            }
        });
    }

    function renderResponse(data) {
        hideTyping();

        switch (data.type) {
            case 'summary':
                appendSummaryMessage(data.content, data.value, data.label, data.sql);
                break;
            case 'table':
                appendTableMessage(data.content, data.columns, data.rows, data.sql);
                break;
            case 'chart':
                appendChartMessage(data.content, data.chart_data, data.sql, data.chart_type || 'bar');
                break;
            case 'text':
            default:
                appendTextMessage(data.content || 'No response.', data.sql);
                break;
        }
    }

    async function sendMessage(message) {
        appendUserMessage(message);
        showTyping();

        // Add to history
        chatHistory.push({ role: 'user', content: message });

        try {
            const resp = await fetch('/analytics/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: message,
                    history: chatHistory.slice(-6),
                }),
            });

            if (!resp.ok) {
                hideTyping();
                appendTextMessage('Sorry, something went wrong. Please try again.', null);
                return;
            }

            const data = await resp.json();
            renderResponse(data);

            // Add assistant response to history
            chatHistory.push({ role: 'assistant', content: data.content || JSON.stringify(data) });

        } catch (e) {
            hideTyping();
            appendTextMessage('Failed to connect to the server. Please try again.', null);
            console.error('Chat error:', e);
        }
    }

    function clearChat() {
        chatHistory.length = 0;
        const container = document.getElementById('chat-messages');
        const welcome = document.getElementById('chat-welcome');
        // Save welcome message HTML, clear everything, restore it
        const welcomeHtml = welcome ? welcome.outerHTML : '';
        container.innerHTML = welcomeHtml;
    }

    // --- Event Listeners ---

    document.addEventListener('DOMContentLoaded', function() {
        const form = document.getElementById('chat-form');
        const input = document.getElementById('chat-input');
        const clearBtn = document.getElementById('chat-clear');

        if (form) {
            form.addEventListener('submit', function(e) {
                e.preventDefault();
                const message = input.value.trim();
                if (!message) return;
                input.value = '';
                sendMessage(message);
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', function() {
                clearChat();
                if (input) input.focus();
            });
        }
    });
})();
