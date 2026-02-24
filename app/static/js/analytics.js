// Analytics Dashboard - Chart rendering, filters, tab switching
(function() {
    'use strict';

    const charts = {};
    let activeTab = 'opportunities';

    // --- Helpers ---

    function formatCurrency(val) {
        if (val >= 1e9) return '$' + (val / 1e9).toFixed(1) + 'B';
        if (val >= 1e6) return '$' + (val / 1e6).toFixed(1) + 'M';
        if (val >= 1e3) return '$' + (val / 1e3).toFixed(0) + 'K';
        return '$' + val.toFixed(0);
    }

    function formatNumber(val) {
        return val.toLocaleString();
    }

    function getOppFilterParams() {
        const params = new URLSearchParams();

        const statuses = [];
        document.querySelectorAll('[data-tab="opportunities"] .form-check-input[id^="status-"]:checked').forEach(cb => {
            statuses.push(cb.value);
        });
        if (statuses.length > 0) params.set('status', statuses.join(','));

        const agencies = [];
        document.querySelectorAll('.agency-cb:checked').forEach(cb => {
            agencies.push(cb.value);
        });
        if (agencies.length > 0) params.set('agency', agencies.join(','));

        const categories = [];
        document.querySelectorAll('.category-cb:checked').forEach(cb => {
            categories.push(cb.value);
        });
        if (categories.length > 0) params.set('category', categories.join(','));

        const ds = document.getElementById('filter-date-start').value;
        const de = document.getElementById('filter-date-end').value;
        if (ds) params.set('date_start', ds);
        if (de) params.set('date_end', de);

        const gran = document.getElementById('filter-granularity').value;
        if (gran) params.set('granularity', gran);

        return params.toString();
    }

    function getResearcherFilterParams() {
        const params = new URLSearchParams();

        const depts = [];
        document.querySelectorAll('.dept-cb:checked').forEach(cb => {
            depts.push(cb.value);
        });
        if (depts.length > 0) params.set('department', depts.join(','));

        const statuses = [];
        document.querySelectorAll('.res-status-cb:checked').forEach(cb => {
            statuses.push(cb.value);
        });
        if (statuses.length > 0) params.set('researcher_status', statuses.join(','));

        const kw = document.getElementById('filter-keyword')?.value?.trim();
        if (kw) params.set('keyword', kw);

        return params.toString();
    }

    function getMatchFilterParams() {
        const params = new URLSearchParams();

        const minScore = document.getElementById('filter-min-score')?.value;
        if (minScore && parseFloat(minScore) > 0) params.set('min_score', minScore);

        const agencies = [];
        document.querySelectorAll('.match-agency-cb:checked').forEach(cb => {
            agencies.push(cb.value);
        });
        if (agencies.length > 0) params.set('agency', agencies.join(','));

        const depts = [];
        document.querySelectorAll('.match-dept-cb:checked').forEach(cb => {
            depts.push(cb.value);
        });
        if (depts.length > 0) params.set('department', depts.join(','));

        return params.toString();
    }

    function destroyChart(id) {
        if (charts[id]) {
            charts[id].destroy();
            delete charts[id];
        }
    }

    function createChart(canvasId, config) {
        destroyChart(canvasId);
        const ctx = document.getElementById(canvasId);
        if (!ctx) return null;
        charts[canvasId] = new Chart(ctx, config);
        return charts[canvasId];
    }

    async function fetchOppData(endpoint) {
        const params = getOppFilterParams();
        const sep = endpoint.includes('?') ? '&' : '?';
        const url = `/analytics/api/${endpoint}${params ? sep + params : ''}`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`API error: ${resp.status}`);
        return resp.json();
    }

    async function fetchResData(endpoint) {
        const params = getResearcherFilterParams();
        const sep = endpoint.includes('?') ? '&' : '?';
        const url = `/analytics/api/researchers/${endpoint}${params ? sep + params : ''}`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`API error: ${resp.status}`);
        return resp.json();
    }

    async function fetchMatchData(endpoint) {
        const params = getMatchFilterParams();
        const sep = endpoint.includes('?') ? '&' : '?';
        const url = `/analytics/api/matches/${endpoint}${params ? sep + params : ''}`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`API error: ${resp.status}`);
        return resp.json();
    }

    // --- KPI Loading ---

    async function loadKPIs() {
        try {
            const params = getOppFilterParams();
            const url = `/analytics/api/kpis${params ? '?' + params : ''}`;
            const resp = await fetch(url);
            const data = await resp.json();

            document.getElementById('kpi-total').textContent = formatNumber(data.total_opportunities);
            document.getElementById('kpi-funding').textContent = formatCurrency(data.total_funding);
            document.getElementById('kpi-researchers').textContent = formatNumber(data.researchers);
            document.getElementById('kpi-publications').textContent = formatNumber(data.publications);
            document.getElementById('kpi-grants').textContent = formatNumber(data.verso_grants);
            document.getElementById('kpi-matches').textContent = formatNumber(data.total_matches);
            document.getElementById('kpi-avg-match').textContent = data.avg_match_score.toFixed(1);
        } catch (e) {
            console.error('Failed to load KPIs:', e);
        }
    }

    // --- Filter Visibility ---

    function updateFilterVisibility(tab) {
        document.querySelectorAll('.filter-section').forEach(section => {
            section.style.display = section.getAttribute('data-tab') === tab ? '' : 'none';
        });
    }

    // --- Horizontal Bar Chart Helper ---

    function horizontalBarConfig(data, opts) {
        return {
            type: 'bar',
            data: data,
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: opts?.showLegend || false },
                    tooltip: {
                        callbacks: {
                            label: function(ctx) {
                                return opts?.currency ? formatCurrency(ctx.raw) : ctx.raw.toLocaleString();
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        grid: { color: '#f0f0f0' },
                        ticks: opts?.currency ? { callback: function(v) { return formatCurrency(v); } } : {},
                    },
                    y: {
                        grid: { display: false },
                        ticks: {
                            font: { size: 11 },
                            callback: function(v) {
                                const label = this.getLabelForValue(v);
                                return label.length > 30 ? label.substring(0, 30) + '...' : label;
                            },
                        },
                    },
                },
            },
        };
    }

    // --- Opportunities Tab Loader ---

    async function loadOpportunitiesTab() {
        try {
            const [timeline, closeDates, dist, byAgency, trends, floorCeil, comp, activity, heatmap, catFunding, classification, classifTrends] = await Promise.all([
                fetchOppData('timeline'),
                fetchOppData('close-dates'),
                fetchOppData('funding-distribution'),
                fetchOppData('funding-by-agency'),
                fetchOppData('funding-trends'),
                fetchOppData('floor-vs-ceiling'),
                fetchOppData('agency-comparison'),
                fetchOppData('agency-activity'),
                fetchOppData('agency-category'),
                fetchOppData('category-funding'),
                fetchOppData('classification'),
                fetchOppData('classification-trends'),
            ]);

            // Timeline
            createChart('chart-timeline', {
                type: 'line',
                data: timeline,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { position: 'top' } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            createChart('chart-close-dates', {
                type: 'bar',
                data: closeDates,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            // Funding
            createChart('chart-funding-dist', {
                type: 'bar',
                data: dist,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            createChart('chart-funding-agency', horizontalBarConfig(byAgency, { currency: true }));

            createChart('chart-funding-trends', {
                type: 'line',
                data: trends,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: { position: 'top' },
                        tooltip: {
                            callbacks: {
                                label: function(ctx) { return ctx.dataset.label + ': ' + formatCurrency(ctx.raw); }
                            }
                        }
                    },
                    scales: {
                        x: { grid: { display: false } },
                        y: {
                            type: 'linear', position: 'left', beginAtZero: true,
                            ticks: { callback: function(v) { return formatCurrency(v); } },
                            grid: { color: '#f0f0f0' },
                        },
                        y1: {
                            type: 'linear', position: 'right', beginAtZero: true,
                            ticks: { callback: function(v) { return formatCurrency(v); } },
                            grid: { drawOnChartArea: false },
                        },
                    },
                },
            });

            createChart('chart-floor-ceiling', {
                type: 'bar',
                data: floorCeil,
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: 'top' },
                        tooltip: {
                            callbacks: {
                                label: function(ctx) { return ctx.dataset.label + ': ' + formatCurrency(ctx.raw); }
                            }
                        }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            ticks: { callback: function(v) { return formatCurrency(v); } },
                            grid: { color: '#f0f0f0' },
                        },
                        y: {
                            grid: { display: false },
                            ticks: {
                                font: { size: 10 },
                                callback: function(v) {
                                    const label = this.getLabelForValue(v);
                                    return label.length > 25 ? label.substring(0, 25) + '...' : label;
                                },
                            },
                        },
                    },
                },
            });

            // Agencies
            createChart('chart-agency-comp', {
                type: 'bar',
                data: comp,
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: 'top' } },
                    scales: {
                        x: { stacked: true, beginAtZero: true, grid: { color: '#f0f0f0' } },
                        y: {
                            stacked: true, grid: { display: false },
                            ticks: {
                                font: { size: 11 },
                                callback: function(v) {
                                    const label = this.getLabelForValue(v);
                                    return label.length > 30 ? label.substring(0, 30) + '...' : label;
                                },
                            },
                        },
                    },
                },
            });

            createChart('chart-agency-activity', {
                type: 'line',
                data: activity,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: { position: 'top', labels: { font: { size: 10 }, boxWidth: 12 } },
                    },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            if (heatmap.agencies && heatmap.agencies.length > 0) {
                createChart('chart-agency-category', {
                    type: 'bubble',
                    data: heatmap,
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false },
                            tooltip: {
                                callbacks: {
                                    label: function(ctx) {
                                        const d = ctx.raw;
                                        const agency = heatmap.agencies[d.y] || '';
                                        const cat = heatmap.categories[d.x] || '';
                                        return `${agency} / ${cat}: ${d.count}`;
                                    }
                                }
                            }
                        },
                        scales: {
                            x: {
                                type: 'linear', min: -0.5, max: heatmap.categories.length - 0.5,
                                ticks: {
                                    stepSize: 1,
                                    callback: function(v) {
                                        const label = heatmap.categories[v] || '';
                                        return label.length > 15 ? label.substring(0, 15) + '..' : label;
                                    },
                                },
                                grid: { color: '#f0f0f0' },
                            },
                            y: {
                                type: 'linear', min: -0.5, max: heatmap.agencies.length - 0.5,
                                ticks: {
                                    stepSize: 1,
                                    callback: function(v) {
                                        const label = heatmap.agencies[v] || '';
                                        return label.length > 20 ? label.substring(0, 20) + '..' : label;
                                    },
                                },
                                grid: { color: '#f0f0f0' },
                            },
                        },
                    },
                });
            }

            // Categories
            createChart('chart-cat-funding', horizontalBarConfig(catFunding, { currency: true }));

            createChart('chart-classification', {
                type: 'bar',
                data: classification,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            createChart('chart-classif-trends', {
                type: 'line',
                data: classifTrends,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { position: 'top', labels: { font: { size: 10 }, boxWidth: 12 } } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });
        } catch (e) {
            console.error('Failed to load opportunities tab:', e);
        }
    }

    // --- Researchers Tab Loader ---

    async function loadResearchersTab() {
        try {
            const [byDept, statusBreak, keywords, pubTime, grantFunder, actTypes, engagement] = await Promise.all([
                fetchResData('by-department'),
                fetchResData('status-breakdown'),
                fetchResData('top-keywords'),
                fetchResData('publications-over-time'),
                fetchResData('grant-funding-by-funder'),
                fetchResData('activity-types'),
                fetchResData('engagement-summary'),
            ]);

            createChart('chart-res-dept', horizontalBarConfig(byDept));

            createChart('chart-res-status', {
                type: 'doughnut',
                data: statusBreak,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: 'bottom' } },
                },
            });

            createChart('chart-res-keywords', horizontalBarConfig(keywords));

            createChart('chart-pub-time', {
                type: 'line',
                data: pubTime,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            createChart('chart-grant-funder', horizontalBarConfig(grantFunder, { currency: true }));

            createChart('chart-act-types', {
                type: 'doughnut',
                data: actTypes,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: 'bottom' } },
                },
            });

            createChart('chart-res-engage', {
                type: 'bar',
                data: engagement,
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: 'top' } },
                    scales: {
                        x: { stacked: true, beginAtZero: true, grid: { color: '#f0f0f0' } },
                        y: {
                            stacked: true, grid: { display: false },
                            ticks: {
                                font: { size: 11 },
                                callback: function(v) {
                                    const label = this.getLabelForValue(v);
                                    return label.length > 30 ? label.substring(0, 30) + '...' : label;
                                },
                            },
                        },
                    },
                },
            });
        } catch (e) {
            console.error('Failed to load researchers tab:', e);
        }
    }

    // --- Matches Tab Loader ---

    async function loadMatchesTab() {
        try {
            const [scoreDist, compBreak, topRes, topOpp, byDept, byAgency, coverage] = await Promise.all([
                fetchMatchData('score-distribution'),
                fetchMatchData('component-breakdown'),
                fetchMatchData('top-researchers'),
                fetchMatchData('top-opportunities'),
                fetchMatchData('by-department'),
                fetchMatchData('by-agency'),
                fetchMatchData('coverage'),
            ]);

            createChart('chart-match-dist', {
                type: 'bar',
                data: scoreDist,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            createChart('chart-match-comp', {
                type: 'bar',
                data: compBreak,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            createChart('chart-match-researchers', horizontalBarConfig(topRes));
            createChart('chart-match-opportunities', horizontalBarConfig(topOpp));
            createChart('chart-match-dept', horizontalBarConfig(byDept));
            createChart('chart-match-agency', horizontalBarConfig(byAgency));

            // Coverage cards
            document.getElementById('coverage-opp-pct').textContent = coverage.opportunity_coverage_pct + '%';
            document.getElementById('coverage-opp-detail').textContent =
                `${formatNumber(coverage.opportunities_with_match)} of ${formatNumber(coverage.total_opportunities)}`;
            document.getElementById('coverage-res-pct').textContent = coverage.researcher_coverage_pct + '%';
            document.getElementById('coverage-res-detail').textContent =
                `${formatNumber(coverage.researchers_with_match)} of ${formatNumber(coverage.total_researchers)}`;
            document.getElementById('coverage-total').textContent = formatNumber(coverage.total_matches);
            document.getElementById('coverage-strong').textContent = formatNumber(coverage.strong_matches);
            document.getElementById('coverage-threshold').textContent = `score >= ${coverage.threshold}`;
        } catch (e) {
            console.error('Failed to load matches tab:', e);
        }
    }

    // --- Tab loaders map ---

    const tabLoaders = {
        opportunities: loadOpportunitiesTab,
        researchers: loadResearchersTab,
        matches: loadMatchesTab,
    };

    function loadActiveTab() {
        const loader = tabLoaders[activeTab];
        if (loader) loader();
    }

    // --- Event Listeners ---

    document.addEventListener('DOMContentLoaded', function() {
        // Initial load
        loadKPIs();
        loadOpportunitiesTab();

        // Min score slider label
        const minScoreSlider = document.getElementById('filter-min-score');
        const minScoreLabel = document.getElementById('min-score-label');
        if (minScoreSlider && minScoreLabel) {
            minScoreSlider.addEventListener('input', function() {
                minScoreLabel.textContent = this.value;
            });
        }

        // Tab switching
        document.querySelectorAll('#analyticsTabs button[data-bs-toggle="tab"]').forEach(btn => {
            btn.addEventListener('shown.bs.tab', function(e) {
                const target = e.target.getAttribute('data-bs-target');
                if (target === '#opportunities-pane') activeTab = 'opportunities';
                else if (target === '#researchers-pane') activeTab = 'researchers';
                else if (target === '#matches-pane') activeTab = 'matches';
                else if (target === '#chat-pane') activeTab = 'chat';

                updateFilterVisibility(activeTab);
                loadActiveTab();
            });
        });

        // Apply filters
        document.getElementById('btn-apply-filters').addEventListener('click', function() {
            loadKPIs();
            loadActiveTab();
        });

        // Reset filters
        document.getElementById('btn-reset-filters').addEventListener('click', function() {
            // Opportunity filters
            document.getElementById('filter-date-start').value = '';
            document.getElementById('filter-date-end').value = '';
            document.getElementById('filter-granularity').value = 'month';
            document.getElementById('status-posted').checked = true;
            document.getElementById('status-forecasted').checked = true;
            document.getElementById('status-closed').checked = false;
            document.getElementById('status-archived').checked = false;
            document.querySelectorAll('.agency-cb:checked, .category-cb:checked').forEach(cb => { cb.checked = false; });

            // Researcher filters
            document.querySelectorAll('.dept-cb:checked').forEach(cb => { cb.checked = false; });
            document.getElementById('res-status-active').checked = true;
            document.getElementById('res-status-inactive').checked = false;
            var kwField = document.getElementById('filter-keyword');
            if (kwField) kwField.value = '';

            // Match filters
            if (minScoreSlider) { minScoreSlider.value = 0; minScoreLabel.textContent = '0'; }
            document.querySelectorAll('.match-agency-cb:checked, .match-dept-cb:checked').forEach(cb => { cb.checked = false; });

            loadKPIs();
            loadActiveTab();
        });

        // Agency search filter (opportunity filters)
        const agencySearch = document.getElementById('agency-search');
        if (agencySearch) {
            agencySearch.addEventListener('input', function() {
                const query = this.value.toLowerCase();
                document.querySelectorAll('.agency-item').forEach(item => {
                    const label = item.querySelector('label').textContent.toLowerCase();
                    item.style.display = label.includes(query) ? '' : 'none';
                });
            });
        }

        // Department search filter
        const deptSearch = document.getElementById('dept-search');
        if (deptSearch) {
            deptSearch.addEventListener('input', function() {
                const query = this.value.toLowerCase();
                document.querySelectorAll('.dept-item').forEach(item => {
                    const label = item.querySelector('label').textContent.toLowerCase();
                    item.style.display = label.includes(query) ? '' : 'none';
                });
            });
        }

        // Match agency search filter
        const matchAgencySearch = document.getElementById('match-agency-search');
        if (matchAgencySearch) {
            matchAgencySearch.addEventListener('input', function() {
                const query = this.value.toLowerCase();
                document.querySelectorAll('.match-agency-item').forEach(item => {
                    const label = item.querySelector('label').textContent.toLowerCase();
                    item.style.display = label.includes(query) ? '' : 'none';
                });
            });
        }

        // Match department search filter
        const matchDeptSearch = document.getElementById('match-dept-search');
        if (matchDeptSearch) {
            matchDeptSearch.addEventListener('input', function() {
                const query = this.value.toLowerCase();
                document.querySelectorAll('.match-dept-item').forEach(item => {
                    const label = item.querySelector('label').textContent.toLowerCase();
                    item.style.display = label.includes(query) ? '' : 'none';
                });
            });
        }
    });
})();
