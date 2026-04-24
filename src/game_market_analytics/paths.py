"""Repository path helpers for local development."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ProjectPaths:
    """Important repository paths used by local utilities."""

    project_root: Path = PROJECT_ROOT

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_data_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def stage_data_dir(self) -> Path:
        return self.data_dir / "stage"

    @property
    def marts_data_dir(self) -> Path:
        return self.data_dir / "marts"

    @property
    def dbt_dir(self) -> Path:
        return self.project_root / "dbt"

    @property
    def local_dir(self) -> Path:
        return self.project_root / ".local"

    @property
    def duckdb_path(self) -> Path:
        return self.local_dir / "game_market_analytics.duckdb"

    @property
    def required_directories(self) -> tuple[Path, ...]:
        return (
            self.raw_data_dir,
            self.raw_data_dir / "steam",
            self.raw_data_dir / "igdb",
            self.raw_data_dir / "itad",
            self.stage_data_dir,
            self.marts_data_dir,
            self.dbt_dir,
        )

    @property
    def writable_directories(self) -> tuple[Path, ...]:
        return (
            self.local_dir,
            self.raw_data_dir,
            self.stage_data_dir,
            self.marts_data_dir,
        )


def get_project_paths() -> ProjectPaths:
    """Return the default repository path configuration."""

    return ProjectPaths()
