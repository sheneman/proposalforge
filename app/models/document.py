from datetime import datetime

from sqlalchemy import (
    String, Text, DateTime, Integer,
    ForeignKey, Index, text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class OpportunityDocument(Base):
    __tablename__ = "opportunity_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("opportunities.id", ondelete="CASCADE"), nullable=False
    )
    attachment_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    file_name: Mapped[str] = mapped_column(String(500), nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    file_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    file_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    folder_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    local_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    download_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    ocr_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    embed_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")

    extracted_text_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    chunk_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=datetime.utcnow,
    )

    # Relationships
    opportunity = relationship("Opportunity", back_populates="documents", lazy="selectin")
    chunks = relationship(
        "DocumentChunk", back_populates="document",
        cascade="all, delete-orphan", lazy="noload",
    )

    __table_args__ = (
        Index("ix_doc_opp_id", "opportunity_id"),
        Index("ix_doc_download_status", "download_status"),
        Index("ix_doc_ocr_status", "ocr_status"),
        Index("ix_doc_embed_status", "embed_status"),
    )


class DocumentChunk(Base):
    __tablename__ = "document_chunks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("opportunity_documents.id", ondelete="CASCADE"), nullable=False
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    char_offset: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    char_length: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    chroma_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    # Relationships
    document = relationship("OpportunityDocument", back_populates="chunks")

    __table_args__ = (
        Index("ix_chunk_doc_id", "document_id"),
    )
