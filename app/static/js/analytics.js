// Analytics Dashboard - Chart rendering, filters, tab switching
(function() {
    'use strict';

    const charts = {};
    let activeTab = 'timeline';

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

    function getFilterParams() {
        const params = new URLSearchParams();

        // Status
        const statuses = [];
        document.querySelectorAll('#analytics-filters .form-check-input[id^="status-"]:checked').forEach(cb => {
            statuses.push(cb.value);
        });
        if (statuses.length > 0) params.set('status', statuses.join(','));

        // Agency
        const agencies = [];
        document.querySelectorAll('.agency-cb:checked').forEach(cb => {
            agencies.push(cb.value);
        });
        if (agencies.length > 0) params.set('agency', agencies.join(','));

        // Category
        const categories = [];
        document.querySelectorAll('.category-cb:checked').forEach(cb => {
            categories.push(cb.value);
        });
        if (categories.length > 0) params.set('category', categories.join(','));

        // Date range
        const ds = document.getElementById('filter-date-start').value;
        const de = document.getElementById('filter-date-end').value;
        if (ds) params.set('date_start', ds);
        if (de) params.set('date_end', de);

        // Granularity
        const gran = document.getElementById('filter-granularity').value;
        if (gran) params.set('granularity', gran);

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

    async function fetchData(endpoint) {
        const params = getFilterParams();
        const sep = endpoint.includes('?') ? '&' : '?';
        const url = `/analytics/api/${endpoint}${params ? sep + params : ''}`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`API error: ${resp.status}`);
        return resp.json();
    }

    // --- KPI Loading ---

    async function loadKPIs() {
        try {
            const data = await fetchData('kpis');
            document.getElementById('kpi-total').textContent = formatNumber(data.total_opportunities);
            document.getElementById('kpi-funding').textContent = formatCurrency(data.total_funding);
            document.getElementById('kpi-avg').textContent = formatCurrency(data.avg_ceiling);
            document.getElementById('kpi-agencies').textContent = formatNumber(data.unique_agencies);
            document.getElementById('kpi-categories').textContent = formatNumber(data.unique_categories);
        } catch (e) {
            console.error('Failed to load KPIs:', e);
        }
    }

    // --- Tab Loaders ---

    async function loadTimelineTab() {
        try {
            const [timeline, closeDates] = await Promise.all([
                fetchData('timeline'),
                fetchData('close-dates'),
            ]);

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
        } catch (e) {
            console.error('Failed to load timeline tab:', e);
        }
    }

    async function loadFundingTab() {
        try {
            const [dist, byAgency, trends, floorCeil] = await Promise.all([
                fetchData('funding-distribution'),
                fetchData('funding-by-agency'),
                fetchData('funding-trends'),
                fetchData('floor-vs-ceiling'),
            ]);

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

            createChart('chart-funding-agency', {
                type: 'bar',
                data: byAgency,
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: function(ctx) { return formatCurrency(ctx.raw); }
                            }
                        }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            grid: { color: '#f0f0f0' },
                            ticks: { callback: function(v) { return formatCurrency(v); } },
                        },
                        y: {
                            grid: { display: false },
                            ticks: {
                                font: { size: 11 },
                                callback: function(v, i) {
                                    const label = this.getLabelForValue(v);
                                    return label.length > 30 ? label.substring(0, 30) + '...' : label;
                                },
                            },
                        },
                    },
                },
            });

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
                            type: 'linear',
                            position: 'left',
                            beginAtZero: true,
                            ticks: { callback: function(v) { return formatCurrency(v); } },
                            grid: { color: '#f0f0f0' },
                        },
                        y1: {
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
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
                                callback: function(v, i) {
                                    const label = this.getLabelForValue(v);
                                    return label.length > 25 ? label.substring(0, 25) + '...' : label;
                                },
                            },
                        },
                    },
                },
            });
        } catch (e) {
            console.error('Failed to load funding tab:', e);
        }
    }

    async function loadAgencyTab() {
        try {
            const [comp, activity, heatmap] = await Promise.all([
                fetchData('agency-comparison'),
                fetchData('agency-activity'),
                fetchData('agency-category'),
            ]);

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
                            stacked: true,
                            grid: { display: false },
                            ticks: {
                                font: { size: 11 },
                                callback: function(v, i) {
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
                        legend: {
                            position: 'top',
                            labels: { font: { size: 10 }, boxWidth: 12 },
                        },
                    },
                    scales: {
                        x: { grid: { display: false } },
                        y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                    },
                },
            });

            // Bubble chart for agency x category
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
                                type: 'linear',
                                min: -0.5,
                                max: heatmap.categories.length - 0.5,
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
                                type: 'linear',
                                min: -0.5,
                                max: heatmap.agencies.length - 0.5,
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
        } catch (e) {
            console.error('Failed to load agency tab:', e);
        }
    }

    async function loadCategoryTab() {
        try {
            const [catFunding, classification, classifTrends] = await Promise.all([
                fetchData('category-funding'),
                fetchData('classification'),
                fetchData('classification-trends'),
            ]);

            createChart('chart-cat-funding', {
                type: 'bar',
                data: catFunding,
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: function(ctx) { return formatCurrency(ctx.raw); }
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
                            ticks: { font: { size: 11 } },
                        },
                    },
                },
            });

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
            console.error('Failed to load category tab:', e);
        }
    }

    // --- Tab loaders map ---

    const tabLoaders = {
        timeline: loadTimelineTab,
        funding: loadFundingTab,
        agencies: loadAgencyTab,
        categories: loadCategoryTab,
    };

    function loadActiveTab() {
        const loader = tabLoaders[activeTab];
        if (loader) loader();
    }

    // --- Event Listeners ---

    document.addEventListener('DOMContentLoaded', function() {
        // Initial load
        loadKPIs();
        loadTimelineTab();

        // Tab switching
        document.querySelectorAll('#analyticsTabs button[data-bs-toggle="tab"]').forEach(btn => {
            btn.addEventListener('shown.bs.tab', function(e) {
                const target = e.target.getAttribute('data-bs-target');
                if (target === '#timeline-pane') activeTab = 'timeline';
                else if (target === '#funding-pane') activeTab = 'funding';
                else if (target === '#agencies-pane') activeTab = 'agencies';
                else if (target === '#categories-pane') activeTab = 'categories';
                else if (target === '#chat-pane') activeTab = 'chat';
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
            document.getElementById('filter-date-start').value = '';
            document.getElementById('filter-date-end').value = '';
            document.getElementById('filter-granularity').value = 'month';

            // Reset status checkboxes to default
            document.getElementById('status-posted').checked = true;
            document.getElementById('status-forecasted').checked = true;
            document.getElementById('status-closed').checked = false;
            document.getElementById('status-archived').checked = false;

            // Uncheck all agency and category
            document.querySelectorAll('.agency-cb:checked, .category-cb:checked').forEach(cb => {
                cb.checked = false;
            });

            loadKPIs();
            loadActiveTab();
        });

        // Agency search filter
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
    });
})();
