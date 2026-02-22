from sqlalchemy import String, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Agency(Base):
    __tablename__ = "agencies"

    code: Mapped[str] = mapped_column(String(50), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent_agency_code: Mapped[str | None] = mapped_column(
        String(50), ForeignKey("agencies.code"), nullable=True
    )

    parent_agency = relationship("Agency", remote_side=[code], lazy="selectin")
    opportunities = relationship("Opportunity", back_populates="agency", lazy="noload")

    __table_args__ = (
        Index("ix_agencies_parent", "parent_agency_code"),
    )
