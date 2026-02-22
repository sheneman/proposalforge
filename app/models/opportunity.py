from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import (
    String, Text, Date, DateTime, Numeric, Boolean, Integer,
    ForeignKey, Index, text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.enums import OpportunityStatus


class Opportunity(Base):
    __tablename__ = "opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False, index=True)
    opportunity_number: Mapped[str | None] = mapped_column(String(100), nullable=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=OpportunityStatus.POSTED.value)
    category: Mapped[str | None] = mapped_column(String(5), nullable=True)
    category_explanation: Mapped[str | None] = mapped_column(String(500), nullable=True)

    agency_code: Mapped[str | None] = mapped_column(
        String(50), ForeignKey("agencies.code"), nullable=True
    )

    posting_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    close_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    close_date_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    archive_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    award_ceiling: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), nullable=True)
    award_floor: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), nullable=True)
    estimated_total_funding: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), nullable=True)
    expected_number_of_awards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_sharing: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    synopsis_description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Contact info
    contact_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    contact_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    contact_phone: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Funding instrument
    funding_instrument_type: Mapped[str | None] = mapped_column(String(5), nullable=True)
    funding_instrument_description: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Classification flags
    is_multi_institution: Mapped[bool] = mapped_column(Boolean, default=False)
    is_team_based: Mapped[bool] = mapped_column(Boolean, default=False)
    is_multi_disciplinary: Mapped[bool] = mapped_column(Boolean, default=False)
    is_multi_jurisdiction: Mapped[bool] = mapped_column(Boolean, default=False)

    # Grants.gov link
    grants_gov_url: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=datetime.utcnow,
    )
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Relationships
    agency = relationship("Agency", back_populates="opportunities", lazy="selectin")
    applicant_types = relationship("OpportunityApplicantType", back_populates="opportunity", cascade="all, delete-orphan", lazy="selectin")
    funding_instruments = relationship("OpportunityFundingInstrument", back_populates="opportunity", cascade="all, delete-orphan", lazy="selectin")
    funding_categories = relationship("OpportunityFundingCategory", back_populates="opportunity", cascade="all, delete-orphan", lazy="selectin")
    alns = relationship("OpportunityALN", back_populates="opportunity", cascade="all, delete-orphan", lazy="selectin")

    __table_args__ = (
        Index("ix_opp_status_close", "status", "close_date"),
        Index("ix_opp_agency_status", "agency_code", "status"),
        Index("ix_opp_ceiling", "award_ceiling"),
        Index("ix_opp_posting_date", "posting_date"),
    )
