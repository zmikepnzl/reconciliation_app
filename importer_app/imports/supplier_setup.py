import os
import traceback
import logging
import json
from flask import Blueprint, render_template, request, flash, redirect, url_for, Response, jsonify
from werkzeug.utils import secure_filename
from db_client import DBClient
from datetime import datetime, timedelta
from psycopg2 import errors

# Import the correct service function for invoice processing
from services.importer_invoice import import_invoice_csv
import io


# --- Blueprint Setup ---
supplier_setup_bp = Blueprint("supplier_setup", __name__, template_folder="../../templates")

# --- Main Supplier & Mapping Routes ---

@supplier_setup_bp.route("/")
def index():
    """
    Renders the main supplier list page with summary data.
    """
    suppliers = []
    try:
        with DBClient() as client:
            # Reverted to a simpler query as account numbers are no longer displayed on this page.
            sql = """
                SELECT
                    s.id, s.name,
                    COUNT(DISTINCT sa.id) AS account_count,
                    COUNT(DISTINCT sii.id) AS total_items,
                    COUNT(DISTINCT CASE WHEN cil.circuit_id IS NULL THEN sii.id END) AS unlinked_items,
                    COUNT(DISTINCT CASE WHEN sii.audit_date >= (NOW() - INTERVAL '3 months') THEN sii.id END) AS audited_recent_count,
                    COUNT(DISTINCT CASE WHEN sii.audit_date < (NOW() - INTERVAL '3 months') THEN sii.id END) AS audited_old_count,
                    COUNT(DISTINCT CASE WHEN sii.audit_date IS NULL THEN sii.id END) AS unaudited_count
                FROM suppliers s
                LEFT JOIN supplier_account sa ON s.id = sa.supplier_id
                LEFT JOIN supplier_invoice_items sii ON s.id = sii.supplier_id
                LEFT JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
                LEFT JOIN cmdb_circuits cir ON cil.circuit_id = cir.id
                GROUP BY s.id, s.name
                ORDER BY s.name;
            """
            suppliers = client._execute_query(sql, fetch='all')
    except Exception as e:
        flash("Could not load supplier data.", "error")
        logging.error(f"Failed to fetch suppliers: {traceback.format_exc()}")
        suppliers = []
    return render_template("supplier_setup.html", suppliers=suppliers)


@supplier_setup_bp.route("/supplier/manage/", methods=["GET"])
@supplier_setup_bp.route("/supplier/manage/<int:supplier_id>", methods=["GET"])
def manage_supplier(supplier_id=None):
    """
    Renders the redesigned two-pane supplier management page.
    """
    if not supplier_id:
        return render_template("manage_supplier.html", supplier={'id': None, 'name': 'New Supplier'}, mappings=[], accounts=[])

    try:
        with DBClient() as client:
            supplier = client.get_row_by_id("suppliers", supplier_id)
            if not supplier:
                flash("Supplier not found.", "error")
                return redirect(url_for('supplier_setup.index'))
            
            mappings = client.get_rows("import_mappings", where_clause="supplier_id = %s", params=(supplier_id,), order_by="mapping_name")
            
            # QUERY 1: Corrected JOIN path (sii -> sil -> sih)
            accounts_summary_sql = """
                SELECT
                    sa.id,
                    sa.account_number,
                    MAX(sih.billing_month) AS last_invoice_month,
                    COUNT(DISTINCT sii.id) AS total_items_count,
                    COUNT(DISTINCT CASE WHEN cil.circuit_id IS NOT NULL THEN sii.id END) as linked_items_count,
                    COUNT(DISTINCT CASE WHEN cil.circuit_id IS NULL THEN sii.id END) as unlinked_items_count,
                    COUNT(DISTINCT CASE WHEN sii.audit_date >= (NOW() - INTERVAL '3 months') THEN sii.id END) AS audited_recent_count,
                    COUNT(DISTINCT CASE WHEN sii.audit_date < (NOW() - INTERVAL '3 months') THEN sii.id END) AS audited_old_count,
                    COUNT(DISTINCT CASE WHEN sii.audit_date IS NULL THEN sii.id END) AS unaudited_count
                FROM supplier_account sa
                LEFT JOIN supplier_invoice_items sii ON sa.id = sii.account_number_id
                LEFT JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
                LEFT JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
                LEFT JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
                WHERE sa.supplier_id = %s
                GROUP BY sa.id, sa.account_number
                ORDER BY sa.account_number;
            """
            accounts_summary_data = client._execute_query(accounts_summary_sql, (supplier_id,), fetch='all')

            accounts = {row['id']: dict(row, months=[]) for row in accounts_summary_data}

            # QUERY 2: Corrected JOIN path (sih -> sil -> sii)
            monthly_data_sql = """
                SELECT
                    sa.id AS account_id,
                    sih.billing_month,
                    MAX(sih.invoice_date) as invoice_date,
                    COALESCE(SUM(sih.total_amount), 0.0) as monthly_total,
                    COUNT(DISTINCT sii.id) as item_count
                FROM supplier_account sa
                LEFT JOIN supplier_invoice_headers sih ON sa.id = sih.account_number_id
                LEFT JOIN supplier_invoice_lines sil ON sih.id = sil.invoice_header_id
                LEFT JOIN supplier_invoice_items sii ON sil.item_id = sii.id
                WHERE sa.supplier_id = %s AND sih.billing_month IS NOT NULL
                GROUP BY sa.id, sih.billing_month
                ORDER BY sa.id, sih.billing_month DESC;
            """
            monthly_data = client._execute_query(monthly_data_sql, (supplier_id,), fetch='all')
            
            for row in monthly_data:
                acc_id = row['account_id']
                if acc_id in accounts:
                    accounts[acc_id]['months'].append({
                        'billing_month_obj': row['billing_month'],
                        'invoice_date_obj': row['invoice_date'],
                        'monthly_total': row['monthly_total'],
                        'item_count': row['item_count']
                    })

    except Exception as e:
        flash("An error occurred while loading supplier data.", "error")
        logging.error(f"Failed to fetch supplier details for ID {supplier_id}: {traceback.format_exc()}")
        return redirect(url_for('supplier_setup.index'))
        
    return render_template("manage_supplier.html", supplier=supplier, mappings=mappings, accounts=list(accounts.values()))


@supplier_setup_bp.route("/supplier/save", methods=["POST"])
def save_supplier():
    """Handles the form submission for creating or updating a supplier."""
    supplier_id = request.form.get("supplier_id") or None
    
    # Retrieve all fields from the form
    supplier_data = {
        'name': request.form.get("name"),
        'type': request.form.get("type"),
        'account_manager_email': request.form.get("account_manager_email"),
        'contact_person': request.form.get("contact_person"),
        'supplier_short_name': request.form.get("supplier_short_name"),
        'other_names': request.form.get("other_names"),
        'override_name': request.form.get("override_name")
    }

    try:
        with DBClient() as client:
            if supplier_id and supplier_id != 'None':
                client.update_row("suppliers", int(supplier_id), supplier_data)
                flash(f"Supplier '{supplier_data['name']}' updated successfully!", "success")
            else:
                new_supplier = client.create_row("suppliers", supplier_data)
                supplier_id = new_supplier['id']
                flash(f"Supplier '{supplier_data['name']}' added successfully!", "success")
    except Exception as e:
        flash(f"Could not save supplier. Error: {e}", "error")
        logging.error(f"Failed to save supplier: {traceback.format_exc()}")
        return redirect(url_for("supplier_setup.index"))
        
    return redirect(url_for("supplier_setup.manage_supplier", supplier_id=supplier_id))

@supplier_setup_bp.route("/supplier/delete/<int:supplier_id>", methods=["POST"])
def delete_supplier(supplier_id):
    """Deletes a supplier only if it has no associated data, preventing orphaned records."""
    try:
        with DBClient() as client:
            if client.get_rows("supplier_account", where_clause="supplier_id = %s", params=(supplier_id,)):
                flash("Cannot delete supplier: It has associated accounts.", "error")
                return redirect(url_for("supplier_setup.index"))
            if client.get_rows("import_mappings", where_clause="supplier_id = %s", params=(supplier_id,)):
                flash("Cannot delete supplier: It has associated import mappings.", "error")
                return redirect(url_for("supplier_setup.index"))
            if client.get_rows("supplier_invoice_items", where_clause="supplier_id = %s", params=(supplier_id,)):
                flash("Cannot delete supplier: It has associated invoice items.", "error")
                return redirect(url_for("supplier_setup.index"))
            supplier = client.get_row_by_id("suppliers", supplier_id)
            client.delete_row("suppliers", supplier_id)
            flash(f"Supplier '{supplier.get('name', 'Unknown')}' has been deleted successfully.", "success")
    except Exception as e:
        flash("An error occurred while trying to delete the supplier.", "error")
        logging.error(f"Failed to delete supplier {supplier_id}: {traceback.format_exc()}")
    return redirect(url_for("supplier_setup.index"))

@supplier_setup_bp.route("/create_mapping/<int:supplier_id>", methods=["POST"])
def create_mapping(supplier_id):
    """Creates a new mapping record and its default lines, then redirects to the editor."""
    try:
        with DBClient() as client:
            supplier = client.get_row_by_id("suppliers", supplier_id)
            default_name = f"New Mapping for {supplier.get('name', 'Unknown')} - {datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            mapping_data = {'mapping_name': default_name, 'supplier_id': supplier_id}
            new_mapping = client.create_row("import_mappings", mapping_data)
            mapping_id = new_mapping['id']
            
            all_metadata_rules = client.get_rows("mapping_rules_metadata")
            for rule in all_metadata_rules:
                source_type = (rule.get('source_type_options') or 'CSV').split(',')[0]
                default_transformation = (rule.get('default_transformation') or 'None').split(',')[0]
                new_line_data = {
                    'mapping_name_id': mapping_id, 'name': rule['destination_field'],
                    'field_role': rule['field_role'], 'source_type': source_type,
                    'transformation': default_transformation, 'transformation_args': rule.get('transformation_args')
                }
                client.create_row('import_mapping_lines', new_line_data)
            
            flash("New mapping created. Please provide a name and configure the lines.", "success")
            return redirect(url_for('mapping_manager.manage_mapping_lines', mapping_id=mapping_id))
    except Exception as e:
        flash(f"Error creating new mapping: {e}", "error")
        logging.error(f"Failed to create mapping: {traceback.format_exc()}")
        return redirect(url_for('supplier_setup.manage_supplier', supplier_id=supplier_id))

# --- MODIFIED: This function is now completely updated ---
@supplier_setup_bp.route("/import_invoice/<int:supplier_id>", methods=["POST"])
def import_invoice(supplier_id):
    """Handles the invoice file import process by calling the correct service."""
    
    mapping_name = request.form.get("mapping_name_select")
    files = request.files.getlist("invoice_file")
    
    if not mapping_name or not files or not files[0].filename:
        flash("Mapping and invoice file are required.", "error")
        return redirect(url_for('supplier_setup.manage_supplier', supplier_id=supplier_id))

    # Use a single DB client for all file processing in this request
    try:
        with DBClient() as client:
            for file in files:
                if file:
                    # Create an in-memory stream from the uploaded file to avoid saving to disk
                    file_stream = io.StringIO(file.stream.read().decode("utf-8-sig"))
                    
                    try:
                        # Call the correct, imported function with the correct arguments
                        imported_headers, imported_lines = import_invoice_csv(client, supplier_id, mapping_name, file_stream)
                        flash(f"Successfully processed '{file.filename}': {imported_headers} headers and {imported_lines} lines were created or updated.", "success")
                    except Exception as e:
                        # This inner try/except handles errors for a single file, allowing others to proceed
                        flash(f"Error processing file {file.filename}: {e}", "error")
                        logging.error(f"Invoice import failed for supplier {supplier_id}, file {file.filename}: {traceback.format_exc()}")

    except Exception as e:
        # This outer try/except handles broader errors (e.g., failed DB connection)
        flash(f"A major error occurred during the import process: {e}", "error")
        logging.error(f"Invoice import failed for supplier {supplier_id}: {traceback.format_exc()}")
    
    return redirect(url_for('supplier_setup.manage_supplier', supplier_id=supplier_id))


# --- view_account_items, API endpoints, etc. are below and unchanged ---

@supplier_setup_bp.route("/supplier/view_items/<int:account_id>")
def view_account_items(account_id):
    """Renders a page to view all invoice items for a specific account with filtering."""
    account, invoice_items, supplier, invoice_dates = None, [], {'id': None, 'name': 'Unknown'}, []
    header_info = {'due_date': 'N/A', 'total_amount': 'N/A', 'invoice_number': 'N/A'}
    
    billing_ref_filter = request.args.get("billing_ref", "")
    invoice_date_filter = request.args.get("invoice_date", "")
    link_status_filter = request.args.get("link_status", "")
    audit_status_filter = request.args.get("audit_status", "")
    flagged_filter = request.args.get("flagged_filter", "")
    
    three_months_ago = datetime.now().date() - timedelta(days=90)

    try:
        with DBClient() as client:
            account = client.get_row_by_id("supplier_account", account_id)
            if not account:
                flash("Account not found.", "error")
                return redirect(url_for('supplier_setup.index'))
            supplier = client.get_row_by_id("suppliers", account['supplier_id'])
            sql_dates = "SELECT DISTINCT sih.billing_month FROM supplier_invoice_headers sih WHERE sih.account_number_id = %s ORDER BY sih.billing_month DESC;"
            invoice_dates_raw = client._execute_query(sql_dates, (account_id,), fetch='all')
            invoice_dates = [d['billing_month'].strftime('%Y-%m-%d') for d in invoice_dates_raw if d.get('billing_month')]
            if not invoice_date_filter and invoice_dates:
                invoice_date_filter = invoice_dates[0]
            if invoice_date_filter:
                sql_header = "SELECT invoice_number, due_date, total_amount FROM supplier_invoice_headers WHERE account_number_id = %s AND billing_month = %s;"
                header_data = client._execute_query(sql_header, (account_id, invoice_date_filter), fetch='one')
                if header_data:
                    header_info.update({
                        'invoice_number': header_data.get('invoice_number', 'N/A'),
                        'due_date': header_data['due_date'].strftime('%Y-%m-%d') if header_data.get('due_date') else 'N/A',
                        'total_amount': header_data.get('total_amount', 'N/A')
                    })
            base_sql = """
                SELECT
                    sii.id, sii.billing_reference, sii.audit_date, sii.review_flag, sii.notes AS review_notes,
                    sil.description, sil.start_date, sil.end_date, sil.quantity, sil.unit_price, sil.total_amount,
                    (CASE WHEN COUNT(cil.circuit_id) > 0 THEN TRUE ELSE FALSE END) AS is_linked,
                    (
                        SELECT json_agg(
                            json_build_object(
                                'circuit_id', cc.id, 'circuit_name', cc.circuit_id, 'customer_id', cu.id, 'customer_name', cu.name,
                                'cost_price', sci.datacom_cost_price, 'sell_price', sci.customer_sell_price
                            )
                        )
                        FROM circuit_invoice_links cl
                        JOIN cmdb_circuits cc ON cl.circuit_id = cc.id
                        JOIN customers cu ON cc.customer_id = cu.id
                        LEFT JOIN service_catalog_items sci ON cc.service_catalogue_item_linked_id = sci.id
                        WHERE cl.invoice_item_id = sii.id
                    ) AS linked_circuits
                FROM supplier_invoice_items sii
                JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
                LEFT JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
                LEFT JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
                WHERE sii.account_number_id = %s
            """
            params = [account_id]
            
            if billing_ref_filter:
                base_sql += " AND sii.billing_reference ILIKE %s"
                params.append(f"%{billing_ref_filter}%")
            if invoice_date_filter:
                base_sql += " AND sih.billing_month = %s"
                params.append(invoice_date_filter)
            if flagged_filter == 'yes':
                base_sql += " AND sii.review_flag = TRUE"
            if audit_status_filter == '1m':
                one_month_ago = datetime.now() - timedelta(days=30)
                base_sql += " AND (sii.audit_date IS NULL OR sii.audit_date < %s)"
                params.append(one_month_ago.strftime('%Y-%m-%d'))
            elif audit_status_filter == '3m':
                three_months_ago_filter = datetime.now() - timedelta(days=90)
                base_sql += " AND (sii.audit_date IS NULL OR sii.audit_date < %s)"
                params.append(three_months_ago_filter.strftime('%Y-%m-%d'))
            elif audit_status_filter == 'older':
                three_months_ago_filter = datetime.now() - timedelta(days=90)
                base_sql += " AND sii.audit_date < %s"
                params.append(three_months_ago_filter.strftime('%Y-%m-%d'))

            base_sql += " GROUP BY sii.id, sil.id"
            if link_status_filter == 'linked':
                base_sql += " HAVING COUNT(cil.circuit_id) > 0"
            elif link_status_filter == 'unlinked':
                base_sql += " HAVING COUNT(cil.circuit_id) = 0"
            base_sql += " ORDER BY sii.billing_reference;"
            invoice_items = client._execute_query(base_sql, tuple(params), fetch='all')
    except Exception as e:
        flash("An error occurred while loading invoice items.", "error")
        logging.error(f"Failed to load invoice items for account ID {account_id}: {traceback.format_exc()}")
    
    return render_template("supplier_account_items.html",
                           account=account, invoice_items=invoice_items, supplier=supplier,
                           invoice_dates=invoice_dates, header_info=header_info,
                           selected_date=invoice_date_filter, billing_ref_filter=billing_ref_filter,
                           link_status_filter=link_status_filter, audit_status_filter=audit_status_filter,
                           flagged_filter=flagged_filter, three_months_ago=three_months_ago)

@supplier_setup_bp.route("/supplier/manage_item/<int:item_id>")
def manage_invoice_item(item_id):
    """Renders the modal page for managing a single invoice item."""
    try:
        with DBClient() as client:
            item = client.get_row_by_id("supplier_invoice_items", item_id)
            if not item:
                flash("Invoice item not found.", "error")
                return "<p>Invoice item not found. Please close this modal.</p>"

            account = client.get_row_by_id("supplier_account", item['account_number_id'])
            supplier = client.get_row_by_id("suppliers", item['supplier_id'])
            
            invoice_lines_sql = """
                SELECT sil.*, sih.invoice_date
                FROM supplier_invoice_lines sil
                JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
                WHERE sil.item_id = %s
                ORDER BY sih.invoice_date DESC;
            """
            invoice_lines = client._execute_query(invoice_lines_sql, (item_id,), fetch='all')

            linked_circuits_sql = """
                SELECT cc.id, cc.circuit_id, cu.name as customer_name
                FROM circuit_invoice_links cil
                JOIN cmdb_circuits cc ON cil.circuit_id = cc.id
                JOIN customers cu ON cc.customer_id = cu.id
                WHERE cil.invoice_item_id = %s;
            """
            linked_circuits = client._execute_query(linked_circuits_sql, (item_id,), fetch='all')

    except Exception as e:
        logging.error(f"Failed to load invoice item {item_id}: {traceback.format_exc()}")
        return f"<p>An error occurred: {e}</p>"

    return render_template("manage_invoice_item.html", item=item, supplier=supplier, account=account,
                           invoice_lines=invoice_lines, linked_circuits=linked_circuits)

@supplier_setup_bp.route("/supplier/bulk_audit_items", methods=["POST"])
def bulk_audit_items():
    """Handles the auto-audit functionality for all eligible items matching the current filters."""
    account_id = request.form.get("account_id")
    invoice_date_filter = request.form.get("invoice_date")
    billing_ref_filter = request.form.get("billing_ref")
    link_status_filter = request.form.get("link_status")
    audit_status_filter = request.form.get("audit_status")
    flagged_filter = request.form.get("flagged_filter")
    
    audited_count = 0
    now_date = datetime.now().strftime("%Y-%m-%d")

    try:
        with DBClient() as client:
            base_sql = """
                SELECT
                    sii.id, sil.total_amount,
                    (
                        SELECT json_agg(
                            json_build_object(
                                'circuit_id', cc.id, 'cost_price', sci.datacom_cost_price, 'sell_price', sci.customer_sell_price
                            )
                        )
                        FROM circuit_invoice_links cl
                        JOIN cmdb_circuits cc ON cl.circuit_id = cc.id
                        LEFT JOIN service_catalog_items sci ON cc.service_catalogue_item_linked_id = sci.id
                        WHERE cl.invoice_item_id = sii.id
                    ) AS linked_circuits
                FROM supplier_invoice_items sii
                JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
                LEFT JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
                LEFT JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
                WHERE sii.account_number_id = %s
            """
            params = [account_id]
            
            if billing_ref_filter:
                base_sql += " AND sii.billing_reference ILIKE %s"
                params.append(f"%{billing_ref_filter}%")
            if invoice_date_filter:
                base_sql += " AND sih.billing_month = %s"
                params.append(invoice_date_filter)
            if flagged_filter == 'yes':
                base_sql += " AND sii.review_flag = TRUE"
            if audit_status_filter:
                if audit_status_filter == '1m':
                    one_month_ago = datetime.now() - timedelta(days=30)
                    base_sql += " AND (sii.audit_date IS NULL OR sii.audit_date < %s)"
                    params.append(one_month_ago.strftime('%Y-%m-%d'))
                elif audit_status_filter == '3m':
                    three_months_ago_filter = datetime.now() - timedelta(days=90)
                    base_sql += " AND (sii.audit_date IS NULL OR sii.audit_date < %s)"
                    params.append(three_months_ago_filter.strftime('%Y-%m-%d'))
                elif audit_status_filter == 'older':
                    three_months_ago_filter = datetime.now() - timedelta(days=90)
                    base_sql += " AND sii.audit_date < %s"
                    params.append(three_months_ago_filter.strftime('%Y-%m-%d'))

            base_sql += " GROUP BY sii.id, sil.id"
            if link_status_filter == 'linked':
                base_sql += " HAVING COUNT(cil.circuit_id) > 0"
            elif link_status_filter == 'unlinked':
                base_sql += " HAVING COUNT(cil.circuit_id) = 0"
            
            items_to_audit = client._execute_query(base_sql, tuple(params), fetch='all')

            for item in items_to_audit:
                if not item.get('linked_circuits') or len(item['linked_circuits']) != 1:
                    continue

                circuit = item['linked_circuits'][0]
                cost_price = circuit.get('cost_price')
                sell_price = circuit.get('sell_price')
                invoice_total = item.get('total_amount')

                if (cost_price is not None and sell_price is not None and invoice_total is not None and
                        float(cost_price) == float(invoice_total) and float(sell_price) >= float(cost_price)):
                    
                    client.update_row("supplier_invoice_items", item['id'], {'audit_date': now_date})
                    client.update_row("cmdb_circuits", circuit['circuit_id'], {'audit_date': now_date})
                    audited_count += 1

            flash(f"{audited_count} items were successfully auto-audited based on the current filters.", "success")
    except Exception as e:
        flash("An error occurred during the auto-audit process.", "error")
        logging.error(f"Bulk audit failed: {traceback.format_exc()}")

    return redirect(url_for('supplier_setup.view_account_items', account_id=account_id, 
                            invoice_date=invoice_date_filter, billing_ref=billing_ref_filter,
                            link_status=link_status_filter, audit_status=audit_status_filter,
                            flagged_filter=flagged_filter))

# --- API Endpoints ---

@supplier_setup_bp.route("/api/save_invoice_header", methods=["POST"])
def save_invoice_header():
    """API endpoint to save editable invoice header fields via AJAX."""
    data = request.json
    account_id, invoice_date_str = data.get('account_id'), data.get('invoice_date')
    due_date_str, total_amount = data.get('due_date'), data.get('total_amount')
    if not all([account_id, invoice_date_str]):
        return jsonify({"status": "error", "message": "Missing required fields."}), 400
    update_fields = {}
    if due_date_str: update_fields['due_date'] = due_date_str
    if total_amount is not None:
        try:
            update_fields['total_amount'] = float(total_amount)
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "Invalid total amount."}), 400
    if not update_fields:
        return jsonify({"status": "success", "message": "No changes to save."})
    try:
        with DBClient() as client:
            set_clause = ', '.join([f'"{key}" = %s' for key in update_fields.keys()])
            params = list(update_fields.values()) + [account_id, invoice_date_str]
            sql = f"UPDATE supplier_invoice_headers SET {set_clause} WHERE account_number_id = %s AND billing_month = %s;"
            client._execute_query(sql, tuple(params))
        return jsonify({"status": "success", "message": "Invoice header updated successfully."})
    except Exception as e:
        logging.error(f"Error updating invoice header: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to update invoice header."}), 500

@supplier_setup_bp.route("/api/save_invoice_item/<int:item_id>", methods=["POST"])
def save_invoice_item(item_id):
    """API endpoint to save details for a single invoice item."""
    data = request.json
    update_data = {
        'contract_start_date': data.get('contract_start_date') or None,
        'contract_end_date': data.get('contract_end_date') or None,
        'review_flag': data.get('review_flag', False),
        'notes': data.get('notes')
    }
    try:
        with DBClient() as client:
            client.update_row("supplier_invoice_items", item_id, update_data)
        return jsonify({"status": "success", "message": "Item updated."})
    except Exception as e:
        logging.error(f"Error saving invoice item {item_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to save item."}), 500

@supplier_setup_bp.route("/api/update_item_review_flag", methods=["POST"])
def update_item_review_flag():
    """API endpoint to toggle the review flag for a supplier invoice item."""
    data = request.json
    item_id, review_flag = data.get('item_id'), data.get('review_flag')
    if item_id is None or review_flag is None:
        return jsonify({"status": "error", "message": "Missing item_id or review_flag"}), 400
    try:
        with DBClient() as client:
            update_data = {'review_flag': review_flag}
            if not review_flag: update_data['notes'] = None
            client.update_row("supplier_invoice_items", item_id, update_data)
        return jsonify({"status": "success", "message": "Review flag updated."})
    except Exception as e:
        logging.error(f"Error updating review flag for item {item_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to update review flag."}), 500

@supplier_setup_bp.route("/api/update_item_review_notes", methods=["POST"])
def update_item_review_notes():
    """API endpoint to update the review notes for a supplier invoice item."""
    data = request.json
    item_id, review_notes = data.get('item_id'), data.get('review_notes')
    if item_id is None or review_notes is None:
        return jsonify({"status": "error", "message": "Missing item_id or review_notes"}), 400
    try:
        with DBClient() as client:
            client.update_row("supplier_invoice_items", item_id, {'notes': review_notes})
        return jsonify({"status": "success", "message": "Review notes updated."})
    except Exception as e:
        logging.error(f"Error updating review notes for item {item_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to update review notes."}), 500

@supplier_setup_bp.route("/api/clear_item_audit_date", methods=["POST"])
def clear_item_audit_date():
    """API endpoint to clear the audit date for a single invoice item."""
    data = request.json
    item_id = data.get('item_id')
    if not item_id:
        return jsonify({"status": "error", "message": "Missing item_id"}), 400
    try:
        with DBClient() as client:
            client.update_row("supplier_invoice_items", item_id, {'audit_date': None})
        return jsonify({"status": "success", "message": "Audit date cleared."})
    except Exception as e:
        logging.error(f"Error clearing audit date for item {item_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to clear audit date."}), 500

@supplier_setup_bp.route("/api/unlink_circuit_from_item", methods=["POST"])
def unlink_circuit_from_item():
    """API endpoint to unlink a single circuit from a supplier invoice item."""
    data = request.json
    item_id, circuit_id = data.get('invoice_item_id'), data.get('circuit_id')
    if not all([item_id, circuit_id]):
        return jsonify({"status": "error", "message": "Missing invoice_item_id or circuit_id"}), 400
    try:
        with DBClient() as client:
            client.delete_row_where("circuit_invoice_links", "invoice_item_id = %s AND circuit_id = %s", (item_id, circuit_id))
        return jsonify({"status": "success", "message": "Circuit unlinked."})
    except Exception as e:
        logging.error(f"Error unlinking item {item_id} from circuit {circuit_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Unlink failed."}), 500

@supplier_setup_bp.route("/api/search_circuits")
def search_circuits():
    """API endpoint to search for circuits by circuit ID or customer name."""
    query = request.args.get('query', '')
    if len(query) < 2:
        return jsonify([])
    
    sql = """
        SELECT cc.id, cc.circuit_id, c.name as customer_name
        FROM cmdb_circuits cc
        JOIN customers c ON cc.customer_id = c.id
        WHERE cc.circuit_id ILIKE %s OR c.name ILIKE %s
        LIMIT 20;
    """
    search_term = f"%{query}%"
    try:
        with DBClient() as client:
            results = client._execute_query(sql, (search_term, search_term), fetch='all')
        return jsonify(results)
    except Exception as e:
        logging.error(f"Error searching circuits with query '{query}': {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Search failed."}), 500

@supplier_setup_bp.route("/api/link_circuit_to_item", methods=["POST"])
def link_circuit_to_item():
    """API endpoint to link a circuit to an invoice item."""
    data = request.json
    item_id, circuit_id = data.get('invoice_item_id'), data.get('circuit_id')
    if not all([item_id, circuit_id]):
        return jsonify({"status": "error", "message": "Missing invoice_item_id or circuit_id"}), 400
    try:
        with DBClient() as client:
            client.create_row("circuit_invoice_links", {'invoice_item_id': item_id, 'circuit_id': circuit_id}, returning_id=False)
        return jsonify({"status": "success", "message": "Circuit linked."})
    except errors.UniqueViolation:
        return jsonify({"status": "error", "message": "This circuit is already linked to this item."}), 409
    except Exception as e:
        logging.error(f"Error linking item {item_id} to circuit {circuit_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Link failed."}), 500
        
@supplier_setup_bp.route("/mapping/delete/<int:mapping_id>", methods=["POST"])
def delete_mapping(mapping_id):
    """Deletes a mapping and all of its associated lines."""
    supplier_id = None
    try:
        with DBClient() as client:
            # First, get the supplier_id so we can redirect back to the correct page.
            mapping = client.get_row_by_id("import_mappings", mapping_id)
            if mapping:
                supplier_id = mapping['supplier_id']

            # Delete all lines associated with this mapping to prevent orphaned records.
            client.delete_row_where("import_mapping_lines", "mapping_name_id = %s", (mapping_id,))
            
            # Finally, delete the mapping itself.
            client.delete_row("import_mappings", mapping_id)
            
            flash("Mapping has been deleted successfully.", "success")
    except Exception as e:
        flash("An error occurred while trying to delete the mapping.", "error")
        logging.error(f"Failed to delete mapping {mapping_id}: {traceback.format_exc()}")

    if supplier_id:
        return redirect(url_for('supplier_setup.manage_supplier', supplier_id=supplier_id))
    else:
        # Fallback redirect if the supplier_id couldn't be found
        return redirect(url_for('supplier_setup.index'))