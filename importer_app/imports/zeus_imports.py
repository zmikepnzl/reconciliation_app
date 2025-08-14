import os
import traceback
import logging
from flask import Blueprint, render_template, request, flash, redirect, url_for

# REFACTOR: Import the new DBClient and the refactored service functions
from db_client import DBClient
from services.importer_sci import import_sci_csv 
from services.importer_circuit import import_circuit_csv
from services.link_sci_processor import process_sci_linking

zeus_imports_bp = Blueprint('zeus_imports', __name__, template_folder='../../templates')

@zeus_imports_bp.route("/", methods=["GET"])
def index():
    """Renders the main Zeus imports page."""
    return render_template("zeus_imports.html")

def _handle_upload(file_request_key, import_function):
    """
    A helper function to manage the file upload and import process.
    """
    upload_file = request.files.get(file_request_key)
    
    if not upload_file or not upload_file.filename:
        flash("No file selected. Please choose a file to upload.", "error")
        return
        
    if not upload_file.filename.lower().endswith('.csv'):
        flash("The uploaded file must be a CSV.", "error")
        return

    uploads_dir = "uploads"
    os.makedirs(uploads_dir, exist_ok=True)
    filepath = os.path.join(uploads_dir, upload_file.filename)
    
    try:
        upload_file.save(filepath)
        # Use DBClient as a context manager
        with DBClient() as client:
            imported_count = import_function(client, filepath)
        flash(f"Successfully imported or updated {imported_count} rows from '{upload_file.filename}'.", "success")
    except Exception as e:
        flash(f"An error occurred during import: {e}", "error")
        logging.error(traceback.format_exc())
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

@zeus_imports_bp.route("/upload_sci", methods=["POST"])
def upload_sci():
    _handle_upload("sci_file", import_sci_csv)
    return redirect(url_for("zeus_imports.index"))
    
@zeus_imports_bp.route("/upload_circuit", methods=["POST"])
def upload_circuit():
    _handle_upload("circuit_file", import_circuit_csv)
    return redirect(url_for("zeus_imports.index"))

@zeus_imports_bp.route("/link_sci", methods=["POST"])
def link_sci():
    """
    Triggers the process to link all unlinked circuits to their
    corresponding service catalog items.
    """
    try:
        # Use DBClient as a context manager
        with DBClient() as client:
            linked_count = process_sci_linking(client)
            flash(f"Auto-linking process complete. {linked_count} new circuits were linked to SCIs.", "success")
    except Exception as e:
        flash(f"An error occurred during the SCI linking process: {e}", "error")
        logging.error(traceback.format_exc())
        
    return redirect(url_for("zeus_imports.index"))