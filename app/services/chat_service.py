import logging
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Tables to exclude from dynamic schema introspection
_EXCLUDED_TABLES = {"site_settings", "alembic_version"}

# System prompt rules (appended after dynamic schema)
_RULES = """
## Rules
1. ALWAYS generate an executable SQL query for every question. Do NOT just describe or suggest a query — actually write it out. Every response MUST contain a ```sql ... ``` code block with a runnable query. The query will be executed automatically.
2. ONLY generate SELECT statements. Never INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, or REVOKE.
3. Always include LIMIT (max 200 rows) to prevent huge result sets.
4. Use MariaDB/MySQL syntax (DATE_FORMAT, IFNULL, etc.).
5. When joining opportunities to categories, use opportunity_funding_categories.
6. For agency names, JOIN with the agencies table on o.agency_code = a.code.
7. For funding instruments, JOIN with opportunity_funding_instruments on ofi.opportunity_id = o.id.
8. For applicant types, JOIN with opportunity_applicant_types on oat.opportunity_id = o.id.
9. Use 'posted' or 'forecasted' status for "open" or "active" opportunities.
10. Format monetary values readably when appropriate.
11. Always wrap your SQL in ```sql ... ``` code blocks.
12. Give a brief natural language explanation before the SQL.
"""

# Few-shot examples appended to the system prompt
FEW_SHOT_EXAMPLES = """
## Example Questions and SQL

Q: How many open opportunities are there?
```sql
SELECT COUNT(*) AS open_count FROM opportunities WHERE status IN ('posted', 'forecasted') LIMIT 1
```

Q: Top 10 agencies by total funding
```sql
SELECT a.name, SUM(o.award_ceiling) AS total_funding
FROM opportunities o
JOIN agencies a ON o.agency_code = a.code
WHERE o.award_ceiling IS NOT NULL
GROUP BY a.name
ORDER BY total_funding DESC
LIMIT 10
```

Q: Opportunities by funding category
```sql
SELECT ofc.category_name, COUNT(*) AS num_opportunities
FROM opportunity_funding_categories ofc
JOIN opportunities o ON ofc.opportunity_id = o.id
GROUP BY ofc.category_name
ORDER BY num_opportunities DESC
LIMIT 20
```

Q: What types of funding instruments have the largest awards?
```sql
SELECT ofi.instrument_name, MAX(o.award_ceiling) AS max_award
FROM opportunity_funding_instruments ofi
JOIN opportunities o ON ofi.opportunity_id = o.id
WHERE o.award_ceiling IS NOT NULL
GROUP BY ofi.instrument_name
ORDER BY max_award DESC
LIMIT 10
```

Q: Opportunities closing this month
```sql
SELECT title, close_date, award_ceiling
FROM opportunities
WHERE close_date BETWEEN CURDATE() AND LAST_DAY(CURDATE())
ORDER BY close_date
LIMIT 50
```

Q: Average award ceiling by agency
```sql
SELECT a.name, AVG(o.award_ceiling) AS avg_award
FROM opportunities o
JOIN agencies a ON o.agency_code = a.code
WHERE o.award_ceiling IS NOT NULL
GROUP BY a.name
ORDER BY avg_award DESC
LIMIT 15
```

Q: Top categories for NSF
```sql
SELECT ofc.category_name, COUNT(*) AS num_opps
FROM opportunities o
JOIN agencies a ON o.agency_code = a.code
JOIN opportunity_funding_categories ofc ON ofc.opportunity_id = o.id
WHERE a.name LIKE '%National Science Foundation%'
GROUP BY ofc.category_name
ORDER BY num_opps DESC
LIMIT 15
```

Q: How many team-based opportunities are there?
```sql
SELECT COUNT(*) AS team_based_count FROM opportunities WHERE is_team_based = 1 LIMIT 1
```

Q: Opportunities posted per month this year
```sql
SELECT DATE_FORMAT(posting_date, '%Y-%m') AS month, COUNT(*) AS num_posted
FROM opportunities
WHERE YEAR(posting_date) = YEAR(CURDATE())
GROUP BY month
ORDER BY month
LIMIT 12
```

Q: 5 largest awards
```sql
SELECT title, award_ceiling, agency_code
FROM opportunities
WHERE award_ceiling IS NOT NULL
ORDER BY award_ceiling DESC
LIMIT 5
```
"""

# Query templates for pattern matching
QUERY_TEMPLATES = [
    {
        "patterns": ["top agencies", "agencies by funding", "most funded agencies", "biggest agencies"],
        "template": "SELECT a.name, SUM(o.award_ceiling) AS total_funding FROM opportunities o JOIN agencies a ON o.agency_code = a.code WHERE o.award_ceiling IS NOT NULL GROUP BY a.name ORDER BY total_funding DESC LIMIT 15",
        "description": "Top agencies by total funding",
    },
    {
        "patterns": ["by category", "funding categories", "categories breakdown", "opportunities by category"],
        "template": "SELECT ofc.category_name, COUNT(*) AS num_opportunities FROM opportunity_funding_categories ofc JOIN opportunities o ON ofc.opportunity_id = o.id GROUP BY ofc.category_name ORDER BY num_opportunities DESC LIMIT 20",
        "description": "Opportunities by funding category",
    },
    {
        "patterns": ["funding instrument", "instrument type", "types of awards", "grant types", "types of.*largest"],
        "template": "SELECT ofi.instrument_name, COUNT(*) AS num_opportunities, MAX(o.award_ceiling) AS max_award FROM opportunity_funding_instruments ofi JOIN opportunities o ON ofi.opportunity_id = o.id GROUP BY ofi.instrument_name ORDER BY num_opportunities DESC LIMIT 10",
        "description": "Funding instrument breakdown",
    },
    {
        "patterns": ["closing soon", "closing this month", "upcoming deadlines", "expiring"],
        "template": "SELECT o.title, o.close_date, o.award_ceiling, a.name AS agency FROM opportunities o LEFT JOIN agencies a ON o.agency_code = a.code WHERE o.close_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY) ORDER BY o.close_date LIMIT 20",
        "description": "Opportunities closing soon",
    },
    {
        "patterns": ["largest awards", "biggest awards", "highest funding", "most money"],
        "template": "SELECT o.title, o.award_ceiling, a.name AS agency FROM opportunities o LEFT JOIN agencies a ON o.agency_code = a.code WHERE o.award_ceiling IS NOT NULL ORDER BY o.award_ceiling DESC LIMIT 10",
        "description": "Largest awards",
    },
    {
        "patterns": ["count by status", "status breakdown", "how many.*status", "status distribution"],
        "template": "SELECT status, COUNT(*) AS num_opportunities FROM opportunities GROUP BY status ORDER BY num_opportunities DESC LIMIT 10",
        "description": "Opportunity count by status",
    },
    {
        "patterns": ["from agency", "for agency", "agency.*opportunities", "opportunities.*agency"],
        "template": "SELECT o.title, o.award_ceiling, o.close_date, o.status FROM opportunities o JOIN agencies a ON o.agency_code = a.code WHERE a.name LIKE '%AGENCY_NAME%' ORDER BY o.posting_date DESC LIMIT 20",
        "description": "Opportunities for a specific agency",
    },
    {
        "patterns": ["team.based", "multi.institution", "multi.disciplinary", "multi.jurisdiction", "collaborative"],
        "template": "SELECT title, award_ceiling, status FROM opportunities WHERE is_team_based = 1 ORDER BY award_ceiling DESC LIMIT 20",
        "description": "Classification flag queries",
    },
]

# Forbidden SQL keywords (case-insensitive)
FORBIDDEN_PATTERNS = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|REPLACE\s+INTO|'
    r'LOAD\s+DATA|INTO\s+OUTFILE|INTO\s+DUMPFILE|CALL|EXECUTE|PREPARE)\b',
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> tuple[bool, str]:
    """Validate that SQL is a safe SELECT query."""
    stripped = sql.strip().rstrip(";").strip()

    # Strip leading SQL comments (-- and /* */)
    cleaned = re.sub(r'--[^\n]*\n?', '', stripped)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip()

    # Must start with SELECT or WITH (CTEs)
    if not cleaned.upper().startswith(("SELECT", "WITH")):
        return False, "Only SELECT queries are allowed."

    if FORBIDDEN_PATTERNS.search(cleaned):
        return False, "Query contains forbidden SQL operations."

    return True, ""


def _ensure_limit(sql: str, max_rows: int = 200) -> str:
    """Ensure the query has a LIMIT clause."""
    stripped = sql.strip().rstrip(";")
    if not re.search(r'\bLIMIT\b', stripped, re.IGNORECASE):
        stripped += f" LIMIT {max_rows}"
    return stripped


def _serialize_value(val):
    """Convert DB values to JSON-safe types."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return val


def _detect_response_format(columns: list[str], rows: list[tuple], force_chart: bool = False) -> str:
    """Detect the best display format for query results."""
    if not rows:
        return "empty"
    if len(rows) == 1 and len(columns) == 1 and not force_chart:
        return "summary"
    # Chart: 2 columns with numeric second column, or forced by user
    if len(columns) >= 2 and len(rows) <= 50:
        try:
            float(rows[0][1])
            if force_chart or (len(columns) == 2 and len(rows) <= 30):
                return "chart"
        except (ValueError, TypeError):
            pass
    if force_chart and len(columns) >= 2 and len(rows) >= 2:
        return "chart"
    return "table"


# Chart type detection from user message
_CHART_TYPE_PATTERNS = {
    "pie": re.compile(r'\b(pie\s*(chart)?|donut|doughnut)\b', re.IGNORECASE),
    "line": re.compile(r'\b(line\s*(chart|graph|plot)?|trend|over\s+time|time\s*series|monthly|yearly|per\s+month|per\s+year)\b', re.IGNORECASE),
    "scatter": re.compile(r'\b(scatter\s*(plot|chart)?|correlation)\b', re.IGNORECASE),
    "doughnut": re.compile(r'\b(doughnut|donut)\b', re.IGNORECASE),
}

_PLOT_PATTERN = re.compile(r'\b(plot|chart|graph|visuali[zs]e|draw)\b', re.IGNORECASE)


def _detect_chart_type(message: str) -> str:
    """Detect requested chart type from user message."""
    # Check doughnut before pie since donut also matches pie pattern
    if _CHART_TYPE_PATTERNS["doughnut"].search(message):
        return "doughnut"
    if _CHART_TYPE_PATTERNS["pie"].search(message):
        return "pie"
    if _CHART_TYPE_PATTERNS["line"].search(message):
        return "line"
    if _CHART_TYPE_PATTERNS["scatter"].search(message):
        return "scatter"
    return "bar"


def _wants_chart(message: str) -> bool:
    """Check if user is explicitly requesting a visual chart."""
    return bool(_PLOT_PATTERN.search(message))


class ChatService:

    def __init__(self):
        self._schema_cache: str | None = None

    def _make_client(self, base_url: str, api_key: str):
        try:
            from openai import AsyncOpenAI
            return AsyncOpenAI(base_url=base_url, api_key=api_key)
        except ImportError:
            logger.error("openai package not installed")
            raise RuntimeError("openai package is required for chat functionality")

    async def _get_llm_settings(self, session: AsyncSession) -> dict[str, str]:
        """Load LLM settings from DB, falling back to config.py defaults."""
        from app.services.settings_service import settings_service
        return await settings_service.get_llm_settings(session)

    async def _build_schema(self, session: AsyncSession) -> str:
        """Build schema description from INFORMATION_SCHEMA, cached after first call."""
        if self._schema_cache is not None:
            return self._schema_cache

        # Fetch columns
        col_query = text("""
            SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'proposalforge'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """)
        col_result = await session.execute(col_query)
        col_rows = col_result.fetchall()

        # Fetch foreign keys
        fk_query = text("""
            SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = 'proposalforge'
              AND REFERENCED_TABLE_NAME IS NOT NULL
        """)
        fk_result = await session.execute(fk_query)
        fk_rows = fk_result.fetchall()

        # Build FK lookup: (table, column) -> "FK -> ref_table.ref_column"
        fk_map: dict[tuple[str, str], str] = {}
        for row in fk_rows:
            fk_map[(row[0], row[1])] = f"FK -> {row[2]}.{row[3]}"

        # Group columns by table
        tables: dict[str, list[str]] = {}
        for row in col_rows:
            table_name = row[0]
            if table_name in _EXCLUDED_TABLES:
                continue
            col_name = row[1]
            col_type = row[2]
            col_key = row[3]
            extra = row[4] or ""
            comment = row[5] or ""

            annotations = []
            if col_key == "PRI":
                annotations.append("PK")
            if "auto_increment" in extra:
                annotations.append("auto-increment")
            fk_ref = fk_map.get((table_name, col_name))
            if fk_ref:
                annotations.append(fk_ref)
            if comment:
                annotations.append(f"-- {comment}")

            suffix = f" ({', '.join(annotations)})" if annotations else ""
            line = f"- {col_name} ({col_type.upper()}){suffix}"

            tables.setdefault(table_name, []).append(line)

        # Format as markdown
        parts = [
            "You are a SQL assistant for a MariaDB database of federal grant opportunities from Grants.gov.",
            "",
            "## Database Schema",
        ]
        for table_name, columns in tables.items():
            parts.append(f"\n### Table: {table_name}")
            parts.extend(columns)

        schema_text = "\n".join(parts)
        self._schema_cache = schema_text
        logger.info("Built dynamic schema from INFORMATION_SCHEMA (%d tables)", len(tables))
        return schema_text

    async def _get_system_prompt(self, session: AsyncSession) -> str:
        """Build the full system prompt: dynamic schema + rules + few-shot examples."""
        schema = await self._build_schema(session)
        return schema + "\n" + _RULES + "\n" + FEW_SHOT_EXAMPLES

    @staticmethod
    def _find_matching_template(message: str) -> str | None:
        """Find a query template matching the user message via keyword patterns."""
        msg_lower = message.lower()
        for tmpl in QUERY_TEMPLATES:
            for pattern in tmpl["patterns"]:
                if re.search(pattern, msg_lower):
                    return tmpl["template"]
        return None

    async def chat(
        self,
        session: AsyncSession,
        message: str,
        history: list[dict] | None = None,
    ) -> dict[str, Any]:
        """Process a chat message: call LLM, extract/validate/execute SQL, format results."""
        llm_settings = await self._get_llm_settings(session)
        client = self._make_client(llm_settings["base_url"], llm_settings["api_key"])
        model = llm_settings["model"]

        # Build messages with dynamic schema system prompt
        system_prompt = await self._get_system_prompt(session)
        messages = [{"role": "system", "content": system_prompt}]

        # Add last 6 turns of history
        if history:
            for turn in history[-6:]:
                messages.append({"role": turn["role"], "content": turn["content"]})

        # Check for matching query template and augment user message
        user_content = message
        template = self._find_matching_template(message)
        if template:
            user_content += (
                f"\n\nHere is a reference SQL template for this type of question:\n"
                f"```sql\n{template}\n```\n"
                f"Adapt this template to answer my specific question."
            )
            logger.debug("Injected query template hint for message: %s", message[:80])

        messages.append({"role": "user", "content": user_content})

        try:
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.1,
                max_tokens=2000,
            )
            assistant_text = response.choices[0].message.content or ""
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return {
                "type": "text",
                "content": f"Sorry, I couldn't process your request. The AI service returned an error: {str(e)}",
                "sql": None,
            }

        # Extract SQL from the response
        sql = self._extract_sql(assistant_text)

        if not sql:
            # LLM didn't produce SQL — nudge it to generate a query
            sql = await self._nudge_for_sql(client, model, messages, assistant_text)

        if not sql:
            # Still no SQL - return the text response as-is
            return {
                "type": "text",
                "content": assistant_text,
                "sql": None,
            }

        # Validate SQL
        is_valid, error_msg = _validate_sql(sql)
        if not is_valid:
            return {
                "type": "text",
                "content": f"I generated a query but it was blocked for safety: {error_msg}",
                "sql": sql,
            }

        # Ensure LIMIT
        sql = _ensure_limit(sql)

        # Execute SQL
        try:
            result = await session.execute(text(sql))
            columns = list(result.keys())
            rows = result.fetchall()
        except Exception as e:
            logger.warning(f"SQL execution failed: {e}")
            # Try one refinement
            retry_result = await self._retry_with_error(client, model, messages, sql, str(e))
            if retry_result:
                return retry_result

            return {
                "type": "text",
                "content": f"The query failed to execute: {str(e)}",
                "sql": sql,
            }

        # Format results
        force_chart = _wants_chart(message)
        chart_type = _detect_chart_type(message) if force_chart else "bar"
        return self._format_results(assistant_text, sql, columns, rows, force_chart=force_chart, chart_type=chart_type)

    def _extract_sql(self, text_content: str) -> str | None:
        """Extract SQL from LLM response."""
        # Try ```sql ... ``` blocks first
        match = re.search(r'```sql\s*(.*?)\s*```', text_content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Try ``` ... ``` blocks
        match = re.search(r'```\s*(SELECT.*?)\s*```', text_content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Try bare SELECT ... ; pattern
        match = re.search(r'(SELECT\b.*?;)', text_content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip().rstrip(";")

        # Try bare SELECT without semicolon (greedy to end of line or paragraph)
        match = re.search(r'(SELECT\b[^`]*?(?:LIMIT\s+\d+|$))', text_content, re.DOTALL | re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            # Only accept if it looks like a real query (has FROM)
            if re.search(r'\bFROM\b', candidate, re.IGNORECASE):
                return candidate

        return None

    async def _nudge_for_sql(
        self,
        client,
        model: str,
        original_messages: list[dict],
        assistant_text: str,
    ) -> str | None:
        """If the LLM described a query but didn't write SQL, ask it to produce one."""
        nudge_messages = list(original_messages)
        nudge_messages.append({"role": "assistant", "content": assistant_text})
        nudge_messages.append({
            "role": "user",
            "content": (
                "Please write the actual executable SQL query for this in a ```sql code block. "
                "Do not just describe it — provide the full runnable SELECT statement."
            ),
        })
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=nudge_messages,
                temperature=0.1,
                max_tokens=1000,
            )
            retry_text = response.choices[0].message.content or ""
            return self._extract_sql(retry_text)
        except Exception:
            return None

    async def _retry_with_error(
        self,
        client,
        model: str,
        original_messages: list[dict],
        failed_sql: str,
        error: str,
    ) -> dict[str, Any] | None:
        """Send the error back to the LLM for one refinement attempt."""
        retry_messages = list(original_messages)
        retry_messages.append({
            "role": "assistant",
            "content": f"```sql\n{failed_sql}\n```",
        })
        retry_messages.append({
            "role": "user",
            "content": f"That query failed with this error: {error}\nPlease fix the SQL and try again.",
        })

        try:
            response = await client.chat.completions.create(
                model=model,
                messages=retry_messages,
                temperature=0.1,
                max_tokens=2000,
            )
            retry_text = response.choices[0].message.content or ""
        except Exception:
            return None

        sql = self._extract_sql(retry_text)
        if not sql:
            return None

        is_valid, _ = _validate_sql(sql)
        if not is_valid:
            return None

        sql = _ensure_limit(sql)

        try:
            from app.database import async_session
            async with async_session() as retry_session:
                result = await retry_session.execute(text(sql))
                columns = list(result.keys())
                rows = result.fetchall()
                return self._format_results(retry_text, sql, columns, rows)
        except Exception:
            return None

    def _format_results(
        self,
        assistant_text: str,
        sql: str,
        columns: list[str],
        rows: list[tuple],
        force_chart: bool = False,
        chart_type: str = "bar",
    ) -> dict[str, Any]:
        """Format query results into the appropriate response type."""
        fmt = _detect_response_format(columns, rows, force_chart=force_chart)

        if fmt == "empty":
            clean_text = re.sub(r'```sql.*?```', '', assistant_text, flags=re.DOTALL).strip()
            return {
                "type": "text",
                "content": clean_text or "The query returned no results.",
                "sql": sql,
            }

        if fmt == "summary":
            value = _serialize_value(rows[0][0])
            clean_text = re.sub(r'```sql.*?```', '', assistant_text, flags=re.DOTALL).strip()
            if isinstance(value, (int, float)):
                formatted = f"{value:,.0f}" if value > 1000 else f"{value:,.2f}"
                return {
                    "type": "summary",
                    "content": clean_text,
                    "value": formatted,
                    "label": columns[0],
                    "sql": sql,
                }
            return {
                "type": "summary",
                "content": clean_text,
                "value": str(value),
                "label": columns[0],
                "sql": sql,
            }

        if fmt == "chart":
            labels = [str(_serialize_value(r[0])) for r in rows]
            values = []
            for r in rows:
                v = _serialize_value(r[1])
                if isinstance(v, str):
                    v = v.replace(",", "")
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    values.append(0.0)
            clean_text = re.sub(r'```sql.*?```', '', assistant_text, flags=re.DOTALL).strip()

            # Color palette for pie/doughnut charts
            palette = [
                "#1a365d", "#2c5282", "#d4a843", "#e8c97a", "#2d8659",
                "#c53030", "#6b46c1", "#2b6cb0", "#dd6b20", "#38a169",
                "#805ad5", "#d69e2e", "#3182ce", "#e53e3e", "#319795",
            ]

            dataset = {
                "label": columns[1],
                "data": values,
            }

            if chart_type in ("pie", "doughnut"):
                dataset["backgroundColor"] = palette[:len(values)]
            elif chart_type == "line":
                dataset["borderColor"] = "#2c5282"
                dataset["backgroundColor"] = "rgba(44, 82, 130, 0.1)"
                dataset["fill"] = True
                dataset["tension"] = 0.3
            elif chart_type == "scatter":
                dataset["backgroundColor"] = "#2c5282"
            else:
                dataset["backgroundColor"] = "#2c5282"
                dataset["borderRadius"] = 4

            return {
                "type": "chart",
                "content": clean_text,
                "chart_type": chart_type,
                "chart_data": {
                    "labels": labels,
                    "datasets": [dataset],
                },
                "sql": sql,
            }

        # Table format
        serialized_rows = [
            [_serialize_value(cell) for cell in row]
            for row in rows
        ]
        clean_text = re.sub(r'```sql.*?```', '', assistant_text, flags=re.DOTALL).strip()
        return {
            "type": "table",
            "content": clean_text,
            "columns": columns,
            "rows": serialized_rows,
            "sql": sql,
        }


chat_service = ChatService()
