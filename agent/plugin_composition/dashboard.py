from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DashboardContext:
    """向一个 v3 Dashboard 暴露 Core 分配的窄运行边界。"""

    plugin_id: str
    plugin_dir: Path
    data_root: Path
    validation: bool
