# src/core/models.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Text, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ----------------------------
# Tasks
# ----------------------------

class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    raw: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    comments: Mapped[list["TaskComment"]] = relationship(back_populates="task", cascade="all,delete-orphan")
    files: Mapped[list["TaskFile"]] = relationship(back_populates="task", cascade="all,delete-orphan")
    snapshot: Mapped[Optional["TaskSnapshot"]] = relationship(back_populates="task", uselist=False, cascade="all,delete-orphan")


class TaskComment(Base):
    __tablename__ = "task_comments"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    task_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tasks.id", ondelete="CASCADE"), index=True)

    author_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    author_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)

    raw: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    task: Mapped["Task"] = relationship(back_populates="comments")


class File(Base):
    __tablename__ = "files"

    # attached_id из disk.attachedObject.get
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    size: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    download_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    local_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    tasks: Mapped[list["TaskFile"]] = relationship(back_populates="file", cascade="all,delete-orphan")
    chats: Mapped[list["ChatFile"]] = relationship(back_populates="file", cascade="all,delete-orphan")


class TaskFile(Base):
    __tablename__ = "task_files"

    task_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True)
    file_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("files.id", ondelete="CASCADE"), primary_key=True)
    comment_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    task: Mapped["Task"] = relationship(back_populates="files")
    file: Mapped["File"] = relationship(back_populates="tasks")


class TaskSnapshot(Base):
    __tablename__ = "task_snapshots"

    task_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True)

    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    # ВАЖНО: эти колонки у тебя как раз и отсутствовали в БД после переноса
    last_task_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    last_comments_hash: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    last_files_hash: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    task: Mapped["Task"] = relationship(back_populates="snapshot")


# ----------------------------
# Supplier Chat (Bitrix IM)
# ----------------------------

class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    dialog_id: Mapped[str] = mapped_column(Text, index=True)

    author_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    body: Mapped[str] = mapped_column(Text, nullable=False, default="")
    raw: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    files: Mapped[list["ChatFile"]] = relationship(back_populates="message", cascade="all,delete-orphan")


class ChatFile(Base):
    __tablename__ = "chat_files"

    # делаем композитный PK — это и дедуп и быстрый upsert
    dialog_id: Mapped[str] = mapped_column(Text, primary_key=True)
    message_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("chat_messages.id", ondelete="CASCADE"), primary_key=True)
    file_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("files.id", ondelete="CASCADE"), primary_key=True)

    message: Mapped["ChatMessage"] = relationship(back_populates="files")
    file: Mapped["File"] = relationship(back_populates="chats")
