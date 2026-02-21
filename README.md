# ProposalForge

A web application for indexing and browsing federal grant opportunities from Grants.gov. Built for research offices, professional proposal developers, and potential PIs who need a powerful, visual tool to identify funding opportunities.

## Features

- **Faceted Search**: Filter opportunities by agency, category, deadline, budget, and collaboration type
- **Full-Text Search**: Search across opportunity titles and descriptions
- **Dashboard**: Visual overview with charts showing opportunities by agency and category
- **Auto-Sync**: Automatic ingestion from Grants.gov API every 6 hours
- **Responsive UI**: Bootstrap 5 + HTMX for dynamic, fast interactions

## Quick Start

```bash
# Clone the repo
git clone https://github.com/sheneman/proposalforge.git
cd proposalforge

# Copy environment file
cp .env.example .env

# Start all services
docker compose up --build

# Visit http://localhost/ for the dashboard
# Visit http://localhost/search for faceted search
# Visit http://localhost/docs for API documentation
```

## Architecture

- **Backend**: Python FastAPI + SQLAlchemy 2.0 (async)
- **Frontend**: Bootstrap 5.3 + HTMX + Chart.js
- **Database**: MariaDB 11.2 with FULLTEXT indexes
- **Cache**: Redis 7
- **Proxy**: Nginx

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/opportunities` | GET | Paginated opportunity list with filters |
| `/api/opportunities/{id}` | GET | Single opportunity detail |
| `/api/search` | GET | Full-text + faceted search |
| `/api/agencies` | GET | Agencies with opportunity counts |
| `/api/categories` | GET | Funding categories with counts |
| `/api/stats` | GET | Dashboard statistics |
| `/api/sync/trigger` | POST | Manually trigger data sync |
| `/api/sync/status` | GET | Check sync status |

## Data Source

Data is ingested from the [Grants.gov API](https://www.grants.gov/web-apis) public endpoints (no authentication required).

## License

MIT
