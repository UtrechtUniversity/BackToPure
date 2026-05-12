import os
import secrets
from pathlib import Path

from flask import Flask

from .db import init_db, project_root


def _secret_key() -> str:
    secret_key = os.environ.get("BTP_SECRET_KEY") or os.environ.get("SECRET_KEY")
    if secret_key:
        return secret_key
    if os.environ.get("BTP_ENV") == "production":
        raise RuntimeError("BTP_SECRET_KEY or SECRET_KEY must be set when BTP_ENV=production")
    return secrets.token_hex(32)


def _configured_path(raw_value: str | None, *, base_dir: Path, default: Path) -> Path:
    if not raw_value:
        return default
    candidate = Path(raw_value)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def create_app():
    app = Flask(__name__)
    root_dir = project_root()
    runtime_root = _configured_path(
        os.environ.get("BTP_RUNTIME_ROOT"),
        base_dir=root_dir,
        default=root_dir,
    )
    data_dir = _configured_path(
        os.environ.get("BTP_DATA_DIR"),
        base_dir=root_dir,
        default=runtime_root / "data",
    )
    logs_dir = _configured_path(
        os.environ.get("BTP_LOGS_DIR"),
        base_dir=root_dir,
        default=runtime_root / "logs" / "jobs",
    )
    frontend_dist = _configured_path(
        os.environ.get("BTP_FRONTEND_DIST"),
        base_dir=root_dir,
        default=root_dir / "frontend" / "dist",
    )

    app.secret_key = _secret_key()
    app.config.setdefault("BTP_PROJECT_ROOT", str(root_dir))
    app.config.setdefault("BTP_RUNTIME_ROOT", str(runtime_root))
    app.config.setdefault("BTP_DATA_DIR", str(data_dir))
    app.config.setdefault("BTP_LOGS_DIR", str(logs_dir))
    app.config.setdefault("BTP_FRONTEND_DIST", str(frontend_dist))

    init_db(app)

    from . import routes
    routes.init_app(app)

    return app
