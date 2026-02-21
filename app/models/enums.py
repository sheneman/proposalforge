import enum


class OpportunityStatus(str, enum.Enum):
    POSTED = "posted"
    FORECASTED = "forecasted"
    CLOSED = "closed"
    ARCHIVED = "archived"
