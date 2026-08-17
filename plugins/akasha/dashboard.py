"""Expose the read-only Akasha Inspector to the v3 dashboard."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from agent.plugin_composition import DashboardContext

from .config import load_akasha_config
from .inspector import AkashaInspectorReader


def register(
    app: FastAPI,
    context: DashboardContext,
) -> None:
    """Register read-only retrieval-inspection routes."""

    reader = AkashaInspectorReader(
        memory_root=context.workspace_root("memory"),
        config=load_akasha_config(context.data_root / "config.local.toml"),
    )

    @app.get("/api/dashboard/akasha-inspector/overview")
    def get_overview() -> dict[str, object]:
        return reader.get_overview()

    @app.get("/api/dashboard/akasha-inspector/turns")
    def list_turns(
        session_key: str = "",
        q: str = "",
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=200),
    ) -> dict[str, object]:
        items, total = reader.list_turns(
            session_key=session_key,
            q=q.strip(),
            page=page,
            page_size=page_size,
        )
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    @app.get("/api/dashboard/akasha-inspector/turns/{query_id:path}")
    def get_turn(query_id: str) -> dict[str, Any]:
        item = reader.get_turn(query_id)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail="Akasha 检索记录不存在",
            )
        return item
