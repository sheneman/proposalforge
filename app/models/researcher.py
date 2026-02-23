from datetime import datetime

from sqlalchemy import (
    String, Text, DateTime, Integer, Float, Boolean, LargeBinary,
    ForeignKey, Index, UniqueConstraint, text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Researcher(Base):
    __tablename__ = "researchers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    collabnet_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    primary_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    first_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    full_name: Mapped[str] = mapped_column(String(500), nullable=False)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    photo_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    profile_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    position_title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    position_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    ai_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    keyword_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    embedding: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)

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
    keywords = relationship("ResearcherKeyword", back_populates="researcher", cascade="all, delete-orphan", lazy="selectin")
    affiliations = relationship("ResearcherAffiliation", back_populates="researcher", cascade="all, delete-orphan", lazy="selectin")
    education = relationship("ResearcherEducation", back_populates="researcher", cascade="all, delete-orphan", lazy="selectin")
    publication_links = relationship("ResearcherPublication", back_populates="researcher", cascade="all, delete-orphan", lazy="noload")
    opportunity_matches = relationship("ResearcherOpportunityMatch", back_populates="researcher", cascade="all, delete-orphan", lazy="noload")

    __table_args__ = (
        Index("ix_researcher_status", "status"),
        Index("ix_researcher_position", "position_code"),
    )


class ResearcherKeyword(Base):
    __tablename__ = "researcher_keywords"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    keyword: Mapped[str] = mapped_column(String(255), nullable=False)

    researcher = relationship("Researcher", back_populates="keywords")

    __table_args__ = (
        Index("ix_rk_researcher", "researcher_id"),
        Index("ix_rk_keyword", "keyword"),
    )


class ResearcherAffiliation(Base):
    __tablename__ = "researcher_affiliations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    organization_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    organization_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)

    researcher = relationship("Researcher", back_populates="affiliations")

    __table_args__ = (
        Index("ix_ra_researcher", "researcher_id"),
    )


class ResearcherEducation(Base):
    __tablename__ = "researcher_education"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    institution: Mapped[str | None] = mapped_column(String(500), nullable=True)
    degree: Mapped[str | None] = mapped_column(String(255), nullable=True)
    field_of_study: Mapped[str | None] = mapped_column(String(500), nullable=True)

    researcher = relationship("Researcher", back_populates="education")

    __table_args__ = (
        Index("ix_re_researcher", "researcher_id"),
    )


class Publication(Base):
    __tablename__ = "publications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    collabnet_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(1000), nullable=False)
    abstract: Mapped[str | None] = mapped_column(Text, nullable=True)
    keywords: Mapped[str | None] = mapped_column(Text, nullable=True)
    doi: Mapped[str | None] = mapped_column(String(500), nullable=True)
    uri: Mapped[str | None] = mapped_column(String(500), nullable=True)
    resource_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    publication_date: Mapped[str | None] = mapped_column(String(50), nullable=True)
    publication_info: Mapped[str | None] = mapped_column(Text, nullable=True)
    affiliation: Mapped[str | None] = mapped_column(String(500), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    researcher_links = relationship("ResearcherPublication", back_populates="publication", cascade="all, delete-orphan", lazy="noload")


class ResearcherPublication(Base):
    __tablename__ = "researcher_publications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    publication_id: Mapped[int] = mapped_column(Integer, ForeignKey("publications.id", ondelete="CASCADE"), nullable=False)

    researcher = relationship("Researcher", back_populates="publication_links")
    publication = relationship("Publication", back_populates="researcher_links")

    __table_args__ = (
        UniqueConstraint("researcher_id", "publication_id", name="uq_researcher_publication"),
        Index("ix_rp_researcher", "researcher_id"),
        Index("ix_rp_publication", "publication_id"),
    )


class ResearcherOpportunityMatch(Base):
    __tablename__ = "researcher_opportunity_matches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    opportunity_id: Mapped[int] = mapped_column(Integer, ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    keyword_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    text_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    agency_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    researcher = relationship("Researcher", back_populates="opportunity_matches")
    opportunity = relationship("Opportunity")

    __table_args__ = (
        UniqueConstraint("researcher_id", "opportunity_id", name="uq_researcher_opportunity"),
        Index("ix_rom_opp_score", "opportunity_id", "score"),
        Index("ix_rom_researcher_score", "researcher_id", "score"),
    )
