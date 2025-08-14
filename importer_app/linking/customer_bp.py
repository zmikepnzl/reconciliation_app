import os
import traceback
import logging
import json
import io
import csv
from flask import Blueprint, render_template, request, flash, redirect, url_for, Response, jsonify
from werkzeug.utils import secure_filename
from db_client import DBClient
from datetime import datetime, timedelta
from collections import defaultdict
from psycopg2 import errors

# Create a Blueprint for customer-related routes
customer_bp = Blueprint('customer', __name__, template_folder='../../templates')
logger = logging.getLogger(__name__)

@customer_bp.route("/")
def index():
    """
    Renders the main customer list page.
    Fetches all customers and aggregates counts for their circuits and SCIs,
    including a breakdown of audit statuses.
    """
    customers = []
    try:
        with DBClient() as client:
            # SQL query to get customer data along with circuit and audit stats
            sql = """
                SELECT
                    c.id,
                    c.name,
                    COUNT(DISTINCT cir.id) AS circuit_count,
                    COUNT(DISTINCT sci.id) AS sci_count,
                    COUNT(DISTINCT CASE WHEN cir.audit_date >= (NOW() - INTERVAL '3 months') THEN cir.id END) AS audited_recent_count,
                    COUNT(DISTINCT CASE WHEN cir.audit_date < (NOW() - INTERVAL '3 months') THEN cir.id END) AS audited_old_count,
                    COUNT(DISTINCT CASE WHEN cir.audit_date IS NULL THEN cir.id END) AS unaudited_count
                FROM customers c
                LEFT JOIN cmdb_circuits cir ON c.id = cir.customer_id
                LEFT JOIN service_catalog_items sci ON c.id = sci.customer_id
                GROUP BY c.id, c.name
                ORDER BY c.name;
            """
            customers = client._execute_query(sql, fetch='all')
    except Exception as e:
        flash("Could not load customer data.", "error")
        logging.error(f"Failed to fetch customers: {e}", exc_info=True)
        customers = []
        
    return render_template("customers.html", customers=customers)

@customer_bp.route("/<int:customer_id>", methods=["GET"])
def customer_details(customer_id):
    """
    Renders the detailed view for a specific customer, showing their circuits.
    Supports filtering circuits by various criteria like site, telco, status, etc.
    """
    # Initialize variables for template context
    customer = None
    circuits = []
    sites, telcos, statuses, suppliers, usage_month_options = [], [], [], [], []
    
    # Get filter parameters from the request URL
    site_filter = request.args.get("site", "").strip()
    telco_filter = request.args.getlist("telco") 
    circuit_id_filter = request.args.get("circuit_id", "").strip() 
    status_filter = request.args.get("status", "All").strip()
    sci_linked_filter = request.args.get("sci_linked", "").strip()
    invoice_linked_filter = request.args.get("invoice_linked", "").strip()
    supplier_filter = request.args.get("supplier", "").strip()
    audit_date_filter = request.args.get("audit_date_filter", "").strip()
    billing_month = request.args.get("billing_month", "").strip() 
    three_months_ago = datetime.now().date() - timedelta(days=90)

    try:
        with DBClient() as client:
            customer = client.get_row_by_id("customers", customer_id)
            
            if not customer:
                flash("Customer not found.", "error")
                return redirect(url_for('customer.index'))
            
            # Fetch available billing months for the filter dropdown
            usage_month_sql = """
                SELECT DISTINCT TO_CHAR(sih.invoice_date, 'YYYY-MM') as month
                FROM supplier_invoice_headers sih
                JOIN supplier_invoice_lines sil ON sih.id = sil.invoice_header_id
                JOIN supplier_invoice_items sii ON sil.item_id = sii.id
                JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
                WHERE cil.circuit_id IN (SELECT id FROM cmdb_circuits WHERE customer_id = %s)
                ORDER BY month DESC;
            """
            usage_months_result = client._execute_query(usage_month_sql, (customer_id,), fetch='all')
            usage_month_options = [r['month'] for r in usage_months_result]

            # Base SQL query to fetch circuit details
            base_sql = """
                SELECT
                    c.id, c.circuit_id, c.site_name, c.telco, c.status, c.audit_date,
                    c.review_flag, c.review_notes, c.review_notes_date,
                    sci.billing_name AS sci_name, sci.datacom_cost_price, sci.customer_sell_price,
                    COALESCE(STRING_AGG(DISTINCT sii.billing_reference, ', '), '') AS linked_invoice_items,
                    (SELECT COUNT(cil.invoice_item_id) > 0 FROM circuit_invoice_links cil WHERE cil.circuit_id = c.id) as has_invoice_link,
                    (
                        SELECT COALESCE(SUM(sil_filtered.total_amount::NUMERIC), 0) 
                        FROM circuit_invoice_links cil_filtered
                        JOIN supplier_invoice_items sii_filtered ON cil_filtered.invoice_item_id = sii_filtered.id
                        JOIN supplier_invoice_lines sil_filtered ON sii_filtered.id = sil_filtered.item_id
                        JOIN supplier_invoice_headers sih_filtered ON sil_filtered.invoice_header_id = sih_filtered.id
                        WHERE cil_filtered.circuit_id = c.id
                          AND (%s IS NULL OR TO_CHAR(sih_filtered.invoice_date, 'YYYY-MM') = %s)
                    ) AS invoice_month_total,
                    MAX(sih.invoice_date) as latest_invoice_date,
                    s.name as supplier_name,
                    s.supplier_short_name as supplier_override_name,
                    MAX(sii.id) AS linked_invoice_item_id,
                    c.service_catalogue_item_linked_id AS sci_id
                FROM cmdb_circuits c
                LEFT JOIN service_catalog_items sci ON c.service_catalogue_item_linked_id = sci.id
                LEFT JOIN circuit_invoice_links cil ON c.id = cil.circuit_id
                LEFT JOIN supplier_invoice_items sii ON cil.invoice_item_id = sii.id
                LEFT JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
                LEFT JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
                LEFT JOIN suppliers s ON sii.supplier_id = s.id
                WHERE c.customer_id = %s
            """
            params = [billing_month, billing_month, customer_id] 
            
            # Dynamically add filter conditions to the SQL query
            if site_filter:
                base_sql += " AND c.site_name ILIKE %s"
                params.append(f"%{site_filter}%")
            
            if telco_filter and '' not in telco_filter: 
                base_sql += " AND c.telco IN %s"
                params.append(tuple(telco_filter))
            
            if circuit_id_filter:
                base_sql += " AND c.circuit_id ILIKE %s"
                params.append(f"%{circuit_id_filter}%")

            if status_filter and status_filter != "All":
                base_sql += " AND c.status = %s"
                params.append(status_filter)
            
            if sci_linked_filter == "linked":
                base_sql += " AND c.service_catalogue_item_linked_id IS NOT NULL"
            elif sci_linked_filter == "unlinked":
                base_sql += " AND c.service_catalogue_item_linked_id IS NULL"
            
            if invoice_linked_filter == "linked":
                base_sql += " AND EXISTS (SELECT 1 FROM circuit_invoice_links cil WHERE cil.circuit_id = c.id)"
            elif invoice_linked_filter == "unlinked":
                base_sql += " AND NOT EXISTS (SELECT 1 FROM circuit_invoice_links cil WHERE cil.circuit_id = c.id)"
            
            if supplier_filter:
                base_sql += " AND s.name ILIKE %s"
                params.append(f"%{supplier_filter}%")

            if audit_date_filter == '3m':
                three_months_ago_filter = datetime.now() - timedelta(days=90)
                base_sql += " AND (c.audit_date IS NULL OR c.audit_date < %s)"
                params.append(three_months_ago_filter.strftime('%Y-%m-%d'))
            elif audit_date_filter == 'older':
                three_months_ago_filter = datetime.now() - timedelta(days=90)
                base_sql += " AND c.audit_date < %s"
                params.append(three_months_ago_filter.strftime('%Y-%m-%d'))

            base_sql += """
                GROUP BY c.id, sci.id, s.id
                ORDER BY c.site_name, c.circuit_id;
            """
            
            circuits = client._execute_query(base_sql, tuple(params), fetch='all')
            
            # Fetch distinct values for filter dropdowns
            sites = [r['site_name'] for r in client.get_rows("cmdb_circuits", columns="DISTINCT site_name", where_clause="customer_id = %s AND site_name IS NOT NULL", params=(customer_id,), order_by="site_name")]
            telcos = [r['telco'] for r in client.get_rows("cmdb_circuits", columns="DISTINCT telco", where_clause="customer_id = %s AND telco IS NOT NULL", params=(customer_id,), order_by="telco")]
            statuses = [r['status'] for r in client.get_rows("cmdb_circuits", columns="DISTINCT status", where_clause="customer_id = %s AND status IS NOT NULL", params=(customer_id,), order_by="status")]
            suppliers = [r['name'] for r in client.get_rows("suppliers", columns="DISTINCT name", order_by="name")]
            
    except Exception as e:
        flash(f"An error occurred while loading customer details: {e}", "error")
        logging.error(f"Error loading customer details: {e}", exc_info=True)
        customer = {'id': customer_id, 'name': 'Error Loading'}
        circuits, sites, telcos, statuses, suppliers = [], [], [], [], []
            
    return render_template("customer_details.html",
        customer=customer, circuits=circuits, sites=sites, telcos=telcos,
        circuit_id_filter=circuit_id_filter, status_filter=status_filter, suppliers=suppliers,
        site_filter=site_filter, telco_filter=telco_filter, sci_linked_filter=sci_linked_filter,
        invoice_linked_filter=invoice_linked_filter, supplier_filter=supplier_filter,
        audit_date_filter=audit_date_filter, billing_month=billing_month, 
        usage_month_options=usage_month_options, three_months_ago=three_months_ago
    )

@customer_bp.route("/manage", methods=["GET"])
@customer_bp.route("/manage/<int:customer_id>", methods=["GET"])
def manage_customer(customer_id=None):
    """Renders the page to add a new customer or edit an existing one."""
    customer = {}
    if customer_id:
        try:
            with DBClient() as client:
                customer = client.get_row_by_id("customers", customer_id)
                if not customer:
                    flash("Customer not found.", "error")
                    return redirect(url_for('customer.index'))
        except Exception as e:
            flash(f"An error occurred: {e}", "error")
            logging.error(f"Failed to fetch customer {customer_id}: {e}", exc_info=True)
            return redirect(url_for('customer.index'))
    
    return render_template("manage_customer.html", customer=customer)

@customer_bp.route("/save_customer", methods=["POST"])
def save_customer():
    """Handles the form submission for creating or updating a customer."""
    customer_id = request.form.get("customer_id")
    customer_name = request.form.get("name")
    
    if not customer_name:
        flash("Customer name cannot be empty.", "error")
        return redirect(url_for('customer.index'))

    customer_data = {"name": customer_name}

    try:
        with DBClient() as client:
            if customer_id and customer_id.isdigit():
                client.update_row("customers", int(customer_id), customer_data)
                flash(f"Customer '{customer_name}' updated successfully.", "success")
            else:
                client.create_row("customers", customer_data)
                flash(f"Customer '{customer_name}' added successfully.", "success")
    except Exception as e:
        flash(f"An error occurred while saving the customer: {e}", "error")
        logging.error(f"Error saving customer: {e}", exc_info=True)

    return redirect(url_for('customer.index'))

@customer_bp.route("/<int:customer_id>/circuit/manage", methods=["GET"])
@customer_bp.route("/<int:customer_id>/circuit/manage/<int:circuit_id>", methods=["GET"])
def manage_circuit(customer_id, circuit_id=None):
    """Renders the page to add or edit a circuit for a specific customer."""
    customer, circuit, sci = {}, {}, {}
    linked_invoice_items, invoice_dates, suppliers, supplier_accounts, search_billing_month_options = [], [], [], [], []
    monthly_totals = defaultdict(lambda: {'total_amount': 0.0, 'items': [], 'supplier_name': 'N/A'})

    try:
        with DBClient() as client:
            customer = client.get_row_by_id("customers", customer_id)
            if not customer:
                flash("Customer not found.", "error")
                return redirect(url_for('customer.index'))

            # Fetch billing month options for search dropdown
            search_sql = "SELECT DISTINCT TO_CHAR(invoice_date, 'YYYY-MM') as month FROM supplier_invoice_headers ORDER BY month DESC;"
            search_results = client._execute_query(search_sql, fetch='all')
            search_billing_month_options = [r['month'] for r in search_results]

            if circuit_id:
                circuit = client.get_row_by_id("cmdb_circuits", circuit_id)
                if circuit and circuit.get('service_catalogue_item_linked_id'):
                    sci = client.get_row_by_id("service_catalog_items", circuit['service_catalogue_item_linked_id'])
                
                # Fetch and process linked invoice items for the circuit
                linked_items_sql = """
                    SELECT
                        cil.circuit_id, cil.invoice_item_id, sii.billing_reference, s.name as supplier_name, 
                        sih.invoice_date, sil.total_amount, sil.description as line_description
                    FROM circuit_invoice_links cil
                    JOIN supplier_invoice_items sii ON cil.invoice_item_id = sii.id
                    LEFT JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
                    LEFT JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
                    LEFT JOIN suppliers s ON sii.supplier_id = s.id
                    WHERE cil.circuit_id = %s ORDER BY sih.invoice_date DESC;
                """
                raw_linked_items = client._execute_query(linked_items_sql, (circuit_id,), fetch='all')

                # Aggregate linked items by month
                agg_items = defaultdict(lambda: defaultdict(lambda: {'total_amount': 0.0, 'line_descriptions': [], 'billing_reference': '', 'supplier_name': '', 'circuit_id': '', 'invoice_item_id': ''}))
                for item in raw_linked_items:
                    if not item['invoice_date']: continue
                    month_str, item_key = item['invoice_date'].strftime('%Y-%m'), item['invoice_item_id']
                    agg_item = agg_items[month_str][item_key]
                    agg_item['total_amount'] += float(item.get('total_amount') or 0.0)
                    if item.get('line_description') and item['line_description'] not in agg_item['line_descriptions']:
                        agg_item['line_descriptions'].append(item['line_description'])
                    if not agg_item['billing_reference']:
                        agg_item.update(billing_reference=item['billing_reference'], supplier_name=item['supplier_name'], circuit_id=item['circuit_id'], invoice_item_id=item['invoice_item_id'])

                for month_str, items_by_id in agg_items.items():
                    monthly_totals[month_str]['total_amount'] = sum(i['total_amount'] for i in items_by_id.values())
                    monthly_totals[month_str]['supplier_name'] = next(iter(items_by_id.values()), {}).get('supplier_name', 'N/A')
                    monthly_totals[month_str]['items'] = [{'circuit_id': i['circuit_id'], 'invoice_item_id': i['invoice_item_id'], 'billing_reference': i['billing_reference'], 'total_amount': i['total_amount'], 'line_description': "; ".join(i['line_descriptions'])} for i in items_by_id.values()]
                
                invoice_dates = sorted(list(monthly_totals.keys()), reverse=True)

            suppliers = client.get_rows("suppliers", columns="id, name", order_by="name")
            supplier_accounts = client.get_rows("supplier_account", columns="id, account_number, supplier_id", order_by="account_number")

    except Exception as e:
        flash(f"An error occurred while loading circuit data: {e}", "error")
        logging.error(f"Error loading circuit for management: {e}", exc_info=True)
        customer = {'id': customer_id, 'name': 'Error Loading'}
    
    return render_template("manage_circuit.html",
                           customer=customer, circuit=circuit, sci=sci, linked_invoice_items=linked_invoice_items, 
                           invoice_dates=invoice_dates, monthly_totals=monthly_totals, 
                           selected_date=invoice_dates[0] if invoice_dates else None, suppliers=suppliers, 
                           supplier_accounts=supplier_accounts, search_billing_month_options=search_billing_month_options)

@customer_bp.route("/save_circuit", methods=["POST"])
def save_circuit():
    """Handles the AJAX submission for creating or updating a circuit and its linked SCI."""
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Invalid JSON data received."}), 400

    customer_id = data.get("customer_id")
    circuit_id = data.get("circuit_id")

    def _clean_and_convert_price(price_str):
        if price_str and isinstance(price_str, str):
            clean_str = price_str.replace('$', '').replace(',', '')
            try: return float(clean_str)
            except ValueError: return None
        return None

    if customer_id is None:
        return jsonify({"status": "error", "message": "Customer ID is missing."}), 400

    circuit_data = {
        "circuit_id": data.get("circuit_id_val"), "circuit_name": data.get("circuit_name"),
        "site_name": data.get("site_name"), "telco": data.get("telco"), "status": data.get("status"),
        "vendor_account_number": data.get("vendor_account_number"), "notes": data.get("notes"),
        "review_flag": data.get("review_flag"), "review_notes": data.get("review_notes"),
        "customer_id": int(customer_id), "service_type": data.get("service_type"),
        "circuit_type": data.get("circuit_type"), "cir_pri": data.get("cir_pri"),
        "access": data.get("access"), "priority": data.get("priority")
    }
    
    sci_data = {
        "billing_name": data.get("sci_billing_name"), "service_line": data.get("sci_service_line"),
        "datacom_cost_price": _clean_and_convert_price(data.get("sci_cost_price")),
        "customer_sell_price": _clean_and_convert_price(data.get("sci_sell_price")),
        "customer_id": int(customer_id), "additional_info": data.get("additional_info")
    }

    try:
        with DBClient() as client:
            sci_id = None
            if sci_name := sci_data.get('billing_name'):
                if existing_sci := client.get_rows("service_catalog_items", where_clause="billing_name = %s AND customer_id = %s", params=(sci_name, customer_id)):
                    sci_id = existing_sci[0]['id']
                    client.update_row("service_catalog_items", sci_id, sci_data)
                else:
                    sci_id = client.create_row("service_catalog_items", sci_data)['id']
            
            if sci_id: circuit_data['service_catalogue_item_linked_id'] = sci_id
            
            if circuit_id: client.update_row("cmdb_circuits", int(circuit_id), circuit_data)
            else: client.create_row("cmdb_circuits", circuit_data)
        
        return jsonify({"status": "success", "message": "Circuit and SCI saved successfully."}), 200
    except Exception as e:
        logging.error(f"Error saving circuit/sci via AJAX: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# --- API Endpoints ---

@customer_bp.route("/api/update_review_flag", methods=["POST"])
def update_review_flag():
    """API endpoint to toggle the review flag for a circuit."""
    data = request.json
    circuit_id, review_flag = data.get('circuit_id'), data.get('review_flag')
    if not circuit_id or review_flag is None:
        return jsonify({"status": "error", "message": "Missing circuit_id or review_flag"}), 400
    try:
        with DBClient() as client:
            update_data = {'review_flag': False, 'review_notes': None, 'review_notes_date': None} if not review_flag else {'review_flag': True}
            client.update_row("cmdb_circuits", circuit_id, update_data)
        return jsonify({"status": "success", "message": "Review flag updated."})
    except Exception as e:
        logging.error(f"Error updating review flag for circuit {circuit_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to update review flag."}), 500

@customer_bp.route("/api/update_review_notes", methods=["POST"])
def update_review_notes():
    """API endpoint to update the review notes for a circuit."""
    data = request.json
    circuit_id, review_notes = data.get('circuit_id'), data.get('review_notes')
    if not circuit_id or review_notes is None:
        return jsonify({"status": "error", "message": "Missing circuit_id or review_notes"}), 400
    try:
        with DBClient() as client:
            client.update_row("cmdb_circuits", circuit_id, {'review_notes': review_notes, 'review_notes_date': datetime.now()})
        return jsonify({"status": "success", "message": "Review notes updated."})
    except Exception as e:
        logging.error(f"Error updating review notes for circuit {circuit_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to update review notes."}), 500
        
@customer_bp.route("/api/clear_review_flag", methods=["POST"])
def clear_review_flag():
    """API endpoint to clear the review flag and notes for a circuit."""
    circuit_id = request.json.get('circuit_id')
    if not circuit_id:
        return jsonify({"status": "error", "message": "Missing circuit_id"}), 400
    try:
        with DBClient() as client:
            client.update_row("cmdb_circuits", circuit_id, {'review_flag': False, 'review_notes': None, 'review_notes_date': None})
        return jsonify({"status": "success", "message": "Review flag and notes cleared."})
    except Exception as e:
        logging.error(f"Error clearing review flag for circuit {circuit_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Failed to clear review flag."}), 500

@customer_bp.route("/bulk_update", methods=["POST"])
def customer_bulk_update():
    """Handles bulk updates for circuits based on user selections."""
    customer_id, updates_json = request.form.get("customer_id"), request.form.get("updates_json")
    if not updates_json:
        flash("No updates provided.", "warning")
        return redirect(url_for('customer.customer_details', customer_id=customer_id))
    
    updates_data, nowdate = json.loads(updates_json), datetime.now().strftime("%Y-%m-%d")
    try:
        with DBClient() as client:
            for update in updates_data:
                cmdb_id, circuit_update = update.get("cmdb_id"), {}
                if update.get("do_override_vendor_name") and (s_name := update.get('supplier_override_name')):
                    circuit_update["telco"] = s_name
                if update.get("do_update_cost") and (sci_id := update.get("sci_id")) and (cost := update.get("invoice_month_total")):
                    try:
                        if (cost_float := float(cost)) >= 0:
                            client.update_row("service_catalog_items", sci_id, {"datacom_cost_price": cost_float})
                    except (ValueError, TypeError): pass
                if update.get("do_audit_circuit"): circuit_update["audit_date"] = nowdate
                if update.get("do_audit_invoice_item") and (item_id := update.get("linked_invoice_item_id")):
                    client.update_row("supplier_invoice_items", item_id, {"audit_date": nowdate})
                if circuit_update: client.update_row("cmdb_circuits", cmdb_id, circuit_update)
            flash("Bulk updates applied successfully.", "success")
    except Exception as e:
        flash(f"An error occurred during bulk update: {e}. Changes were rolled back.", "error")
        logging.error(f"Bulk update failed: {traceback.format_exc()}")
    
    return redirect(url_for('customer.customer_details', customer_id=customer_id))

@customer_bp.route("/export_circuits_csv", methods=["POST"])
def export_circuits_csv():
    """Exports selected circuit data to a CSV file."""
    try:
        customer_id = request.form.get("customer_id")
        export_ids_json = request.form.get("export_ids")
        if not export_ids_json:
            flash("No IDs provided for export.", "error")
            return redirect(url_for('customer.customer_details', customer_id=customer_id))

        export_ids = tuple(json.loads(export_ids_json))
        headers = [
            'id', 'name', 'description', 'state', 'service_catalogue_item', 'site', 
            'site_code', 'telco', 'priority', 'circuit_type', 'circuit_service_type', 
            'circuit_termination_type', 'access', 'cir', 'pir', 'vlan', 'pe_host', 
            'ip_pe', 'ce_host', 'host', 'ip_router', 'interface', 'linknet', 
            'qosprofile', 'notes', 'netops_sensor_id', 'netflow', 'contract_start', 
            'contract_end', 'exchange', 'region', 'activated', 'decommissioned', 
            'vendor_billing_end_date', 'service_end_date', 'datacom_last_billing_run_date', 
            'datacom_billing_code', 'vendor_account_number'
        ]
        with DBClient() as client:
            circuits = client._execute_query("SELECT * FROM cmdb_circuits WHERE id IN %s", (export_ids,), fetch='all')

        if not circuits:
            flash("No matching circuits found for export.", "warning")
            return redirect(url_for('customer.customer_details', customer_id=customer_id))

        string_io, writer = io.StringIO(), csv.writer(string_io)
        writer.writerow(headers)
        for c in circuits:
            writer.writerow([c.get(h) for h in headers])

        mem = io.BytesIO(string_io.getvalue().encode('utf-8'))
        string_io.close()
        filename = f"{request.form.get('customer', 'Export')}_Circuits_{datetime.now().strftime('%Y-%m-%d')}.csv"
        return Response(mem, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename={filename}"})

    except Exception as e:
        flash(f"An error occurred during CSV export: {e}", "error")
        logging.error(f"Circuit export failed: {traceback.format_exc()}")
        return redirect(url_for('customer.customer_details', customer_id=request.form.get("customer_id")))

@customer_bp.route("/export_scis_csv", methods=["POST"])
def export_scis_csv():
    """Exports selected Service Catalogue Item data to a CSV file."""
    try:
        customer_id = request.form.get("customer_id")
        export_ids_json = request.form.get("export_ids")
        if not export_ids_json:
            flash("No IDs provided for export.", "error")
            return redirect(url_for('customer.customer_details', customer_id=customer_id))

        export_ids = tuple(json.loads(export_ids_json))
        headers = [
            'id', 'service_line', 'master_service_catalogue_item', 'netsuite_ru_item_code', 
            'unit', 'service_billing_method', 'billing_name', 'host_types', 
            'service_description', 'billable', 'include_in_billing_run', 'contract_number', 
            'service_key', 'datacom_cost_price', 'customer_standard_price', 
            'adjustment_percentage', 'customer_sell_price', 'additional_info', 
            'billable_quantity', 'pipeline_quantity', 'total_bill_price'
        ]
        with DBClient() as client:
            scis = client._execute_query("SELECT * FROM service_catalog_items WHERE id IN %s", (export_ids,), fetch='all')

        if not scis:
            flash("No matching SCIs found for export.", "warning")
            return redirect(url_for('customer.customer_details', customer_id=customer_id))

        string_io, writer = io.StringIO(), csv.writer(string_io)
        writer.writerow(headers)
        for sci in scis:
            writer.writerow([sci.get(h) for h in headers])

        mem = io.BytesIO(string_io.getvalue().encode('utf-8'))
        string_io.close()
        filename = f"{request.form.get('customer', 'Export')}_SCIs_{datetime.now().strftime('%Y-%m-%d')}.csv"
        return Response(mem, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename={filename}"})

    except Exception as e:
        flash(f"An error occurred during SCI CSV export: {e}", "error")
        logging.error(f"SCI export failed: {traceback.format_exc()}")
        return redirect(url_for('customer.customer_details', customer_id=request.form.get("customer_id")))

@customer_bp.route("/api/search_invoice_items", methods=["GET"])
def search_invoice_items():
    """
    API endpoint to search for invoice items.
    If 'show_linked' is true, it returns all items linked to the given circuit_id, ignoring other filters.
    Otherwise, it searches for unlinked items based on the provided search criteria.
    """
    args = request.args
    show_linked = args.get('show_linked', 'false').lower() == 'true'
    circuit_id = args.get('circuit_id')

    # If "Show Linked" is checked, we ONLY care about the circuit_id and ignore other filters.
    if show_linked:
        if not circuit_id:
            return jsonify({"status": "error", "message": "Circuit ID is required to show linked items."}), 400
        
        sql = """
            SELECT sii.id, sii.billing_reference, s.supplier_short_name as supplier_name, 
                   MAX(sih.invoice_date) as invoice_date, SUM(sil.total_amount) as total_amount, 
                   STRING_AGG(sil.description, '; ') as line_description
            FROM circuit_invoice_links cil
            JOIN supplier_invoice_items sii ON cil.invoice_item_id = sii.id
            JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
            JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
            JOIN suppliers s ON sii.supplier_id = s.id
            WHERE cil.circuit_id = %s
            GROUP BY sii.id, s.supplier_short_name
            ORDER BY MAX(sih.invoice_date) DESC;
        """
        params = (circuit_id,)
    
    # Otherwise, we are searching for UNLINKED items based on the filters.
    else:
        query = args.get('query', '')
        supplier_id = args.get('supplier_id')
        acct_id = args.get('account_number_id')
        month = args.get('billing_month')

        # Don't perform a search if all filters are empty.
        if not any([query, supplier_id, acct_id, month]):
            return jsonify([])

        where_conditions = ["sii.id NOT IN (SELECT invoice_item_id FROM circuit_invoice_links)"]
        params_list = []

        if query:
            where_conditions.append("sii.billing_reference ILIKE %s")
            params_list.append(f"%{query}%")
        if supplier_id:
            where_conditions.append("sii.supplier_id = %s")
            params_list.append(supplier_id)
        if acct_id:
            where_conditions.append("sii.account_number_id = %s")
            params_list.append(acct_id)
        if month:
            where_conditions.append("TO_CHAR(sih.invoice_date, 'YYYY-MM') = %s")
            params_list.append(month)
        
        sql = f"""
            SELECT sii.id, sii.billing_reference, s.supplier_short_name as supplier_name, 
                   MAX(sih.invoice_date) as invoice_date, SUM(sil.total_amount) as total_amount, 
                   STRING_AGG(sil.description, '; ') as line_description
            FROM supplier_invoice_items sii
            JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
            JOIN supplier_invoice_headers sih ON sil.invoice_header_id = sih.id
            JOIN suppliers s ON sii.supplier_id = s.id
            WHERE {" AND ".join(where_conditions)}
            GROUP BY sii.id, s.supplier_short_name
            ORDER BY MAX(sih.invoice_date) DESC
            LIMIT 50;
        """
        params = tuple(params_list)

    try:
        with DBClient() as client:
            results = client._execute_query(sql, params, fetch='all')
        
        # Format results for JSON response
        formatted_results = [
            {
                'id': r['id'], 
                'billing_reference': r['billing_reference'], 
                'supplier_name': r['supplier_name'], 
                'invoice_date': r['invoice_date'].strftime('%Y-%m-%d') if r['invoice_date'] else None, 
                'total_amount': float(r['total_amount'] or 0.0), 
                'line_description': r['line_description']
            } for r in results
        ]
        return jsonify(formatted_results)
    except Exception as e:
        logging.error(f"Error searching invoice items: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Search failed."}), 500


@customer_bp.route("/api/link_invoice_item", methods=["POST"])
def link_invoice_item():
    """API endpoint to link an invoice item to a circuit."""
    data = request.json
    circuit_id, item_id = data.get('circuit_id'), data.get('invoice_item_id')
    if not all([circuit_id, item_id]):
        return jsonify({"status": "error", "message": "Missing circuit_id or invoice_item_id"}), 400
    try:
        with DBClient() as client:
            client.create_row("circuit_invoice_links", {'circuit_id': circuit_id, 'invoice_item_id': item_id}, returning_id=False)
        return jsonify({"status": "success", "message": "Item linked."})
    except errors.UniqueViolation:
        return jsonify({"status": "error", "message": "This item is already linked."}), 409
    except Exception as e:
        logging.error(f"Error linking item {item_id} to circuit {circuit_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Link failed."}), 500

@customer_bp.route("/api/unlink_invoice_item", methods=["POST"])
def unlink_invoice_item():
    """API endpoint to unlink an invoice item from a circuit."""
    data = request.json
    circuit_id, item_id = data.get('circuit_id'), data.get('invoice_item_id')
    if not all([circuit_id, item_id]):
        return jsonify({"status": "error", "message": "Missing circuit_id or invoice_item_id"}), 400
    try:
        with DBClient() as client:
            client.delete_row_where("circuit_invoice_links", "circuit_id = %s AND invoice_item_id = %s", (circuit_id, item_id))
            # Reset audit date since the link has changed
            client.update_row("cmdb_circuits", circuit_id, {'audit_date': None})
        return jsonify({"status": "success", "message": "Item unlinked."})
    except Exception as e:
        logging.error(f"Error unlinking item {item_id} from circuit {circuit_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Unlink failed."}), 