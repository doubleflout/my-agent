from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class RegisterRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=256)
    display_name: str | None = Field(default=None, max_length=120)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        email = value.strip().lower()
        if "@" not in email or email.startswith("@") or email.endswith("@"):
            raise ValueError("invalid email")
        return email


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        email = value.strip().lower()
        if "@" not in email or email.startswith("@") or email.endswith("@"):
            raise ValueError("invalid email")
        return email


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: str
    email: str
    display_name: str | None
    created_at: datetime


class CreateConversationRequest(BaseModel):
    title: str | None = Field(default=None, max_length=200)


class ConversationResponse(BaseModel):
    id: str
    title: str
    session_key: str | None = None
    created_at: datetime
    updated_at: datetime


class MessageSourceResponse(BaseModel):
    id: str
    name: str
    type: str
    enabled: bool = True
    server: str | None = None
    get_tool: str | None = None
    ack_tool: str | None = None
    description: str | None = None


class ScheduleResponse(BaseModel):
    id: str
    name: str
    trigger: str
    tier: str
    enabled: bool = True
    fire_at: datetime | None = None
    timezone: str | None = None
    channel: str | None = None
    chat_id: str | None = None
    session_key: str | None = None
    run_count: int = 0
    action_preview: str = ""


class SkillResponse(BaseModel):
    id: str
    name: str
    title: str | None = None
    description: str
    skill_type: str
    scope: str
    user_id: str | None = None
    source: str
    relative_path: str
    entry_file: str
    metadata: dict[str, Any]
    enabled: bool = True
    created_at: datetime
    updated_at: datetime


class UpdateScheduleRequest(BaseModel):
    enabled: bool


class MessageResponse(BaseModel):
    id: str
    conversation_id: str
    role: str
    content: str
    metadata: dict[str, Any]
    created_at: datetime


class CreateMessageRequest(BaseModel):
    content: str = Field(min_length=1, max_length=12000)


class CreateMessageResponse(BaseModel):
    message: MessageResponse
    turn_id: str
    session_key: str
