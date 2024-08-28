from flask import Flask


def create_app():
    app = Flask(__name__)

    # Set the secret key to some random bytes
    app.secret_key = 'key'  # Replace with a secure key

    from . import routes
    routes.init_app(app)

    return app
