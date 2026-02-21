// Dashboard Charts
document.addEventListener('DOMContentLoaded', function() {
    if (typeof statsData === 'undefined') return;

    const navyPalette = [
        '#1a365d', '#2c5282', '#3b6ba5', '#4a80b8', '#6b9bd2',
        '#8bb5e0', '#a8c8e8', '#c4dbf0', '#d4a843', '#e8c97a',
        '#2d6a4f', '#52b788', '#b5838d', '#6d6875', '#e5989b'
    ];

    // Agency Bar Chart
    const agencyCtx = document.getElementById('agencyChart');
    if (agencyCtx && statsData.top_agencies && statsData.top_agencies.length > 0) {
        new Chart(agencyCtx, {
            type: 'bar',
            data: {
                labels: statsData.top_agencies.map(a => {
                    const name = a.name || '';
                    return name.length > 30 ? name.substring(0, 30) + '...' : name;
                }),
                datasets: [{
                    label: 'Opportunities',
                    data: statsData.top_agencies.map(a => a.count),
                    backgroundColor: navyPalette.slice(0, statsData.top_agencies.length),
                    borderWidth: 0,
                    borderRadius: 4,
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: function(items) {
                                const idx = items[0].dataIndex;
                                return statsData.top_agencies[idx].name;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        grid: { display: false },
                    },
                    y: {
                        grid: { display: false },
                        ticks: { font: { size: 11 } },
                    }
                }
            }
        });
    }

    // Category Donut Chart
    const catCtx = document.getElementById('categoryChart');
    if (catCtx && statsData.top_categories && statsData.top_categories.length > 0) {
        new Chart(catCtx, {
            type: 'doughnut',
            data: {
                labels: statsData.top_categories.map(c => c.name || 'Unknown'),
                datasets: [{
                    data: statsData.top_categories.map(c => c.count),
                    backgroundColor: navyPalette.slice(0, statsData.top_categories.length),
                    borderWidth: 2,
                    borderColor: '#fff',
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            font: { size: 10 },
                            boxWidth: 12,
                            padding: 8,
                        }
                    }
                }
            }
        });
    }
});
