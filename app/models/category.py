from sqlalchemy import String, Integer, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class OpportunityApplicantType(Base):
    __tablename__ = "opportunity_applicant_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False
    )
    type_code: Mapped[str] = mapped_column(String(10), nullable=False)
    type_name: Mapped[str] = mapped_column(String(255), nullable=False)

    opportunity = relationship("Opportunity", back_populates="applicant_types")

    __table_args__ = (
        Index("ix_app_type_opp", "opportunity_id"),
    )


class OpportunityFundingInstrument(Base):
    __tablename__ = "opportunity_funding_instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False
    )
    instrument_code: Mapped[str] = mapped_column(String(10), nullable=False)
    instrument_name: Mapped[str] = mapped_column(String(255), nullable=False)

    opportunity = relationship("Opportunity", back_populates="funding_instruments")

    __table_args__ = (
        Index("ix_fund_inst_opp", "opportunity_id"),
    )


class OpportunityFundingCategory(Base):
    __tablename__ = "opportunity_funding_categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False
    )
    category_code: Mapped[str] = mapped_column(String(10), nullable=False)
    category_name: Mapped[str] = mapped_column(String(255), nullable=False)

    opportunity = relationship("Opportunity", back_populates="funding_categories")

    __table_args__ = (
        Index("ix_fund_cat_opp", "opportunity_id"),
        Index("ix_fund_cat_code", "category_code"),
    )


class OpportunityALN(Base):
    __tablename__ = "opportunity_alns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False
    )
    aln_number: Mapped[str] = mapped_column(String(20), nullable=False)
    program_title: Mapped[str | None] = mapped_column(String(500), nullable=True)

    opportunity = relationship("Opportunity", back_populates="alns")

    __table_args__ = (
        Index("ix_aln_opp", "opportunity_id"),
    )
