from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import (
    String, Text, DateTime, Date, Integer, Float, Boolean, LargeBinary,
    Numeric, ForeignKey, Index, UniqueConstraint, text,
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
    ai_summary_themes: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_summary_methods: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_summary_impacts: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_summary_collabs: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    identifiers = relationship("ResearcherIdentifier", back_populates="researcher", cascade="all, delete-orphan", lazy="selectin")
    publication_links = relationship("ResearcherPublication", back_populates="researcher", cascade="all, delete-orphan", lazy="noload")
    grant_links = relationship("ResearcherGrant", back_populates="researcher", cascade="all, delete-orphan", lazy="noload")
    project_links = relationship("ResearcherProject", back_populates="researcher", cascade="all, delete-orphan", lazy="noload")
    activity_links = relationship("ResearcherActivity", back_populates="researcher", cascade="all, delete-orphan", lazy="noload")
    opportunity_matches = relationship("ResearcherOpportunityMatch", back_populates="researcher", cascade="all, delete-orphan", lazy="noload")

    __table_args__ = (
        Index("ix_researcher_status", "status"),
        Index("ix_researcher_position", "position_code"),
        Index(
            "ft_researcher_search",
            "full_name", "keyword_text", "ai_summary", "position_title",
            mysql_prefix="FULLTEXT",
        ),
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


class ResearcherIdentifier(Base):
    __tablename__ = "researcher_identifiers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    identifier_type: Mapped[str] = mapped_column(String(50), nullable=False)
    identifier_value: Mapped[str] = mapped_column(String(255), nullable=False)

    researcher = relationship("Researcher", back_populates="identifiers")

    __table_args__ = (
        Index("ix_ri_researcher", "researcher_id"),
        Index("ix_ri_type", "identifier_type"),
        UniqueConstraint("researcher_id", "identifier_type", "identifier_value", name="uq_researcher_identifier"),
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
    open_access: Mapped[str | None] = mapped_column(String(10), nullable=True)
    file_download_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    contributing_faculty: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    researcher_links = relationship("ResearcherPublication", back_populates="publication", cascade="all, delete-orphan", lazy="noload")

    __table_args__ = (
        Index(
            "ft_publication_search",
            "title", "abstract", "keywords",
            mysql_prefix="FULLTEXT",
        ),
    )


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


class Grant(Base):
    __tablename__ = "grants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    verso_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    title: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    funder: Mapped[str | None] = mapped_column(String(500), nullable=True)
    funder_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    grant_number: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(10), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=datetime.utcnow,
    )

    researcher_links = relationship("ResearcherGrant", back_populates="grant", cascade="all, delete-orphan", lazy="noload")


class ResearcherGrant(Base):
    __tablename__ = "researcher_grants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    grant_id: Mapped[int] = mapped_column(Integer, ForeignKey("grants.id", ondelete="CASCADE"), nullable=False)
    role: Mapped[str | None] = mapped_column(String(100), nullable=True)

    researcher = relationship("Researcher", back_populates="grant_links")
    grant = relationship("Grant", back_populates="researcher_links")

    __table_args__ = (
        UniqueConstraint("researcher_id", "grant_id", name="uq_researcher_grant"),
        Index("ix_rg_researcher", "researcher_id"),
        Index("ix_rg_grant", "grant_id"),
    )


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    verso_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    title: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    researcher_links = relationship("ResearcherProject", back_populates="project", cascade="all, delete-orphan", lazy="noload")


class ResearcherProject(Base):
    __tablename__ = "researcher_projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    project_id: Mapped[int] = mapped_column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    role: Mapped[str | None] = mapped_column(String(100), nullable=True)

    researcher = relationship("Researcher", back_populates="project_links")
    project = relationship("Project", back_populates="researcher_links")

    __table_args__ = (
        UniqueConstraint("researcher_id", "project_id", name="uq_researcher_project"),
        Index("ix_rpj_researcher", "researcher_id"),
        Index("ix_rpj_project", "project_id"),
    )


class Activity(Base):
    __tablename__ = "activities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    verso_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    title: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    activity_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    date: Mapped[date | None] = mapped_column(Date, nullable=True)
    location: Mapped[str | None] = mapped_column(String(500), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    researcher_links = relationship("ResearcherActivity", back_populates="activity", cascade="all, delete-orphan", lazy="noload")


class ResearcherActivity(Base):
    __tablename__ = "researcher_activities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    researcher_id: Mapped[int] = mapped_column(Integer, ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False)
    activity_id: Mapped[int] = mapped_column(Integer, ForeignKey("activities.id", ondelete="CASCADE"), nullable=False)
    role: Mapped[str | None] = mapped_column(String(100), nullable=True)

    researcher = relationship("Researcher", back_populates="activity_links")
    activity = relationship("Activity", back_populates="researcher_links")

    __table_args__ = (
        UniqueConstraint("researcher_id", "activity_id", name="uq_researcher_activity"),
        Index("ix_ract_researcher", "researcher_id"),
        Index("ix_ract_activity", "activity_id"),
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
