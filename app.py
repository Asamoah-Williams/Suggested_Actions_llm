
from flask import Flask
from routes import bp as api_bp

import threading
from helper import auto_scheduler


def create_app():
    app = Flask(__name__)
    app.register_blueprint(api_bp) 
    return app


def start_auto_scheduler():
    """Launch scheduler in background thread."""
   
    thread = threading.Thread(target=auto_scheduler, daemon=True)
    thread.start()

if __name__ == "__main__":
    app = create_app()
    start_auto_scheduler()
    app.run(host="0.0.0.0", port=8080, debug=True)
