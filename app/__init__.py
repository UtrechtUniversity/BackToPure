from flask import Flask

from .db import init_db
from .db import project_root

def create_app():
    app = Flask(__name__)

    # Set the secret key to some random bytes
    app.secret_key = 'key'  # Replace with a secure key
    app.config.setdefault('BTP_DATA_DIR', str(init_db.default_data_dir()))
    app.config.setdefault('BTP_PROJECT_ROOT', str(project_root()))
    app.config.setdefault('BTP_FRONTEND_DIST', str(project_root() / "frontend" / "dist"))

    init_db(app)

    from . import routes
    routes.init_app(app)

    return app
