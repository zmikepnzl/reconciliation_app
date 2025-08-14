import os
import traceback
import logging
from flask import Flask, render_template

# --- Blueprint Imports ---
from linking.link_items import link_items_bp
from imports.zeus_imports import zeus_imports_bp
from imports.supplier_imports import supplier_imports_bp
from imports.supplier_setup import supplier_setup_bp
from exports.zeus_export import zeus_export_bp
from admin.admin import admin_bp
from linking.customer_bp import customer_bp
from services.mapping_manager import mapping_manager_bp # Correct import path

# Set up basic logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_app():
    """
    The application factory function.
    Initializes and configures the Flask application.
    """
    app = Flask(__name__, instance_relative_config=True)

    # Load default and instance configurations
    app.config.from_mapping(
        SECRET_KEY=os.environ.get('FLASK_SECRET_KEY', 'a-dev-secret-key-for-production'),
        TEMPLATES_AUTO_RELOAD=True,
    )

    if app.debug:
        app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

    # --- Register Blueprints ---
    app.register_blueprint(link_items_bp, url_prefix='/link_items')
    app.register_blueprint(zeus_imports_bp, url_prefix='/zeus_imports')
    app.register_blueprint(supplier_imports_bp, url_prefix='/supplier_imports')
    app.register_blueprint(supplier_setup_bp, url_prefix='/supplier_setup')
    app.register_blueprint(zeus_export_bp, url_prefix='/zeus_export')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(customer_bp, url_prefix='/customers')
    # FIX: Removed the redundant url_prefix from mapping_manager_bp registration.
    # The blueprint itself already defines its prefix as '/admin/mapping_rules'.
    app.register_blueprint(mapping_manager_bp) 

    @app.route("/")
    def index():
        """
        The main landing page for the application.
        """
        return render_template("index.html")

    return app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)