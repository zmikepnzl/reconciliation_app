import os
import traceback
import logging
import json
from flask import Blueprint, render_template, request, flash, redirect, url_for
import pandas as pd
import io

# REFACTOR: Import new client and the new service function
from db_client import DBClient
from services.importer_invoice import import_invoice_csv
# Corrected import: We import the helper functions directly from the refactored module
from services.mapping_manager import get_active_mappings_for_supplier, get_mapping_row_by_name

supplier_imports_bp = Blueprint("supplier_imports", __name__, template_folder="../../templates")

@supplier_imports_bp.route("/", methods=["GET"])
def index():
    """Renders the main supplier import page."""
    supplier_options = []
    mapping_options = []
    
    # Get the supplier ID from the request if it exists
    selected_supplier_id = request.args.get("supplier_id")

    try:
        with DBClient() as client:
            supplier_options = client.get_rows("suppliers", columns="id, name", order_by="name")
            
            if selected_supplier_id:
                # Use the helper function from the new module
                mapping_options = get_active_mappings_for_supplier(client, selected_supplier_id)
            else:
                mapping_options = []

    except Exception as e:
        logging.error(f"Could not load page options: {traceback.format_exc()}")
        flash("Could not connect to the database to load page options.", "error")

    return render_template(
        "supplier_imports.html",
        supplier_options=supplier_options,
        mapping_options=mapping_options,
        selected_supplier_id=selected_supplier_id
    )

@supplier_imports_bp.route("/upload_invoice", methods=["POST"])
def upload_invoice():
    """Handles the file upload and triggers the import process."""
    supplier_id = request.form.get("supplier_id_select")
    mapping_name = request.form.get("mapping_name_select")
    invoice_files = request.files.getlist("invoice_file")

    if not all([supplier_id, mapping_name, invoice_files and invoice_files[0].filename]):
        flash("Please provide a Supplier, a Mapping, and at least one Invoice file.", "error")
        return redirect(url_for("supplier_imports.index"))

    uploads_dir = "uploads"
    os.makedirs(uploads_dir, exist_ok=True)
    
    # Process the file in a single with block
    with DBClient() as client:
        for invoice_file in invoice_files:
            if not invoice_file.filename.lower().endswith('.csv'):
                flash(f"File '{invoice_file.filename}' is not a CSV and was skipped.", "warning")
                continue
            
            # Use io.StringIO to process file streams in memory, avoiding disk I/O
            file_stream = io.StringIO(invoice_file.stream.read().decode("utf-8-sig"))

            try:
                # Get the mapping row to check headers
                mapping_row = get_mapping_row_by_name(client, mapping_name)
                
                headers = pd.read_csv(file_stream, nrows=0).columns.tolist()
                file_stream.seek(0)
                
                if mapping_row and mapping_row.get('sample_csv_headers'):
                    expected_headers = json.loads(mapping_row['sample_csv_headers'])
                    if set(headers) != set(expected_headers):
                        flash("Uploaded file headers do not match the headers from the selected mapping's sample file. Please upload a file with the correct format.", "error")
                        return redirect(url_for('supplier_imports.index', supplier_id=supplier_id))

                imported_headers, imported_lines = import_invoice_csv(
                    client, int(supplier_id), mapping_name, file_stream
                )
                flash(f"Successfully processed '{invoice_file.filename}': {imported_headers} headers and {imported_lines} lines were created or updated.", "success")
                
            except Exception as e:
                flash(f"An error occurred while processing '{invoice_file.filename}': {e}", "error")
                logging.error(traceback.format_exc())

    return redirect(url_for("supplier_imports.index", supplier_id=supplier_id))