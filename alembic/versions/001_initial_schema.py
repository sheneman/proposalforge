"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-02-21

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Agencies table
    op.create_table(
        "agencies",
        sa.Column("code", sa.String(20), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("parent_agency_code", sa.String(20), sa.ForeignKey("agencies.code"), nullable=True),
    )
    op.create_index("ix_agencies_parent", "agencies", ["parent_agency_code"])

    # Opportunities table
    op.create_table(
        "opportunities",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("opportunity_id", sa.Integer, unique=True, nullable=False),
        sa.Column("opportunity_number", sa.String(100), nullable=True),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="posted"),
        sa.Column("category", sa.String(5), nullable=True),
        sa.Column("category_explanation", sa.String(500), nullable=True),
        sa.Column("agency_code", sa.String(20), sa.ForeignKey("agencies.code"), nullable=True),
        sa.Column("posting_date", sa.Date, nullable=True),
        sa.Column("close_date", sa.Date, nullable=True),
        sa.Column("close_date_description", sa.String(500), nullable=True),
        sa.Column("archive_date", sa.Date, nullable=True),
        sa.Column("award_ceiling", sa.Numeric(15, 2), nullable=True),
        sa.Column("award_floor", sa.Numeric(15, 2), nullable=True),
        sa.Column("estimated_total_funding", sa.Numeric(15, 2), nullable=True),
        sa.Column("expected_number_of_awards", sa.Integer, nullable=True),
        sa.Column("cost_sharing", sa.Boolean, nullable=True),
        sa.Column("synopsis_description", sa.Text, nullable=True),
        sa.Column("contact_name", sa.String(255), nullable=True),
        sa.Column("contact_email", sa.String(255), nullable=True),
        sa.Column("contact_phone", sa.String(50), nullable=True),
        sa.Column("funding_instrument_type", sa.String(5), nullable=True),
        sa.Column("funding_instrument_description", sa.String(255), nullable=True),
        sa.Column("is_multi_institution", sa.Boolean, server_default="0"),
        sa.Column("is_team_based", sa.Boolean, server_default="0"),
        sa.Column("is_multi_disciplinary", sa.Boolean, server_default="0"),
        sa.Column("is_multi_jurisdiction", sa.Boolean, server_default="0"),
        sa.Column("grants_gov_url", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")),
        sa.Column("last_synced_at", sa.DateTime, nullable=True),
    )
    op.create_index("ix_opp_opportunity_id", "opportunities", ["opportunity_id"])
    op.create_index("ix_opp_status_close", "opportunities", ["status", "close_date"])
    op.create_index("ix_opp_agency_status", "opportunities", ["agency_code", "status"])
    op.create_index("ix_opp_ceiling", "opportunities", ["award_ceiling"])
    op.create_index("ix_opp_posting_date", "opportunities", ["posting_date"])

    # FULLTEXT index on title + synopsis_description
    op.execute("ALTER TABLE opportunities ADD FULLTEXT INDEX ft_opp_title_desc (title, synopsis_description)")

    # Association tables
    op.create_table(
        "opportunity_applicant_types",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("opportunity_id", sa.Integer, sa.ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False),
        sa.Column("type_code", sa.String(10), nullable=False),
        sa.Column("type_name", sa.String(255), nullable=False),
    )
    op.create_index("ix_app_type_opp", "opportunity_applicant_types", ["opportunity_id"])

    op.create_table(
        "opportunity_funding_instruments",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("opportunity_id", sa.Integer, sa.ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False),
        sa.Column("instrument_code", sa.String(10), nullable=False),
        sa.Column("instrument_name", sa.String(255), nullable=False),
    )
    op.create_index("ix_fund_inst_opp", "opportunity_funding_instruments", ["opportunity_id"])

    op.create_table(
        "opportunity_funding_categories",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("opportunity_id", sa.Integer, sa.ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False),
        sa.Column("category_code", sa.String(10), nullable=False),
        sa.Column("category_name", sa.String(255), nullable=False),
    )
    op.create_index("ix_fund_cat_opp", "opportunity_funding_categories", ["opportunity_id"])
    op.create_index("ix_fund_cat_code", "opportunity_funding_categories", ["category_code"])

    op.create_table(
        "opportunity_alns",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("opportunity_id", sa.Integer, sa.ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False),
        sa.Column("aln_number", sa.String(20), nullable=False),
        sa.Column("program_title", sa.String(500), nullable=True),
    )
    op.create_index("ix_aln_opp", "opportunity_alns", ["opportunity_id"])


def downgrade() -> None:
    op.drop_table("opportunity_alns")
    op.drop_table("opportunity_funding_categories")
    op.drop_table("opportunity_funding_instruments")
    op.drop_table("opportunity_applicant_types")
    op.drop_table("opportunities")
    op.drop_table("agencies")
