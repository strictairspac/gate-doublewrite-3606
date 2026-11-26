from pydantic import BaseModel, Field
from src.constants.job_state import JobState


class RunInput(BaseModel):
    parameter: int = Field(strict=True, ge=1)


class RunEntry(BaseModel):
    id: int
    case_id: int
    case_title: str
    status: JobState
    parameter: int
    actor: str
    created_at: int
    finished_at: int | None
    result: dict | None
    failure: str | None
    digest: str | None
    # Lease / handoff visibility for the inspection page.
    owner_id: str | None = None
    owner_epoch: int = 0
    lease_expires_at: int | None = None
    total_pages: int | None = None
    landed_pages: int = 0
    attempts: int = 0
    last_owner_id: str | None = None
    handed_off: bool = False
