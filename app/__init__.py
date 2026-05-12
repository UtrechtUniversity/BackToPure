import os
import secrets

from flask import Flask

from .db import init_db, project_root


def _secret_key() -> str:
    secret_key = os.environ.get("BTP_SECRET_KEY") or os.environ.get("SECRET_KEY")
    if secret_key:
        return secret_key
    if os.environ.get("BTP_ENV") == "production":
        raise RuntimeError("BTP_SECRET_KEY or SECRET_KEY must be set when BTP_ENV=production")
    return secrets.token_hex(32)


def create_app():
    app = Flask(__name__)

    app.secret_key = _secret_key()
    app.config.setdefault('BTP_DATA_DIR', str(init_db.default_data_dir()))
    app.config.setdefault('BTP_PROJECT_ROOT', str(project_root()))
    app.config.setdefault('BTP_FRONTEND_DIST', str(project_root() / "frontend" / "dist"))

    init_db(app)

    from . import routes
    routes.init_app(app)

    return app
