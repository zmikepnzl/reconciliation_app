import logging
import unicodedata
import math
import traceback
from flask import Blueprint, request, render_template, redirect, url_for, flash, jsonify
from db_client import DBClient
from psycopg2 import errors

link_items_bp = Blueprint('link_items', __name__, template_folder='../../templates')
logger = logging.getLogger("link_items.py")

def normalize(val):
    if val is None:
        return ""
    val = unicodedata.normalize('NFKC', str(val))
    val = val.replace('\u00A0', ' ').replace('\r', '').replace('\n', '').strip().lower()
    return "".join(val.split()).replace("–", "-").replace("—", "-")

@link_items_bp.route("/link_items_to_circuits_ui", methods=["GET"])
def link_items_to_circuits_ui():
    with DBClient() as client:
        suppliers = client.get_rows("suppliers", columns="id, name", order_by="name")
        customers = client.get_rows("customers", columns="id, name", order_by="name")
    current_filters = request.args.to_dict()
    return render_template("link_items.html",
                           supplier_options=suppliers,
                           customer_options=customers,
                           current_filters=current_filters)

@link_items_bp.route("/get_account_numbers_by_supplier", methods=["GET"])
def get_account_numbers_by_supplier():
    supplier_name = request.args.get('supplier_name')
    logging.debug(f"Received request for account numbers for supplier: {supplier_name}")
    if not supplier_name:
        logging.warning("No supplier name provided, returning empty list.")
        return jsonify([])
    sql = """
        SELECT sa.account_number
        FROM supplier_account sa
        JOIN suppliers s ON sa.supplier_id = s.id
        WHERE s.name = %s
        ORDER BY sa.account_number;
    """
    logging.debug(f"Executing SQL query for account numbers: {sql} with param: {supplier_name}")
    try:
        with DBClient() as client:
            results = client._execute_query(sql, (supplier_name,), fetch='all')
        account_numbers = [r['account_number'] for r in results]
        logging.debug(f"Found {len(account_numbers)} account numbers: {account_numbers}")
        return jsonify(account_numbers)
    except Exception as e:
        logging.error(f"Error fetching account numbers: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@link_items_bp.route("/get_invoice_items", methods=["GET"])
def get_invoice_items_for_display():
    supplier_name = request.args.get('supplier')
    account_number = request.args.get('account_number')
    billing_ref = request.args.get('billing_ref')
    view_mode = request.args.get('view_mode', 'unlinked')

    logging.debug(f"get_invoice_items_for_display: Filters received - Supplier: {supplier_name}, Account: {account_number}, Billing Ref: {billing_ref}, View Mode: {view_mode}")

    sql = """
        SELECT sii.id, sii.billing_reference, sah.account_number,
               EXISTS(SELECT 1 FROM circuit_invoice_links cil WHERE cil.invoice_item_id = sii.id) AS is_linked,
               -- CORRECTED: Join and aggregate linked circuit IDs
               STRING_AGG(c.circuit_id, ', ') AS linked_circuit_ids
        FROM supplier_invoice_items sii
        JOIN suppliers s ON sii.supplier_id = s.id
        LEFT JOIN supplier_account sah ON sii.account_number_id = sah.id
        -- New joins for the linked circuit IDs
        LEFT JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
        LEFT JOIN cmdb_circuits c ON cil.circuit_id = c.id
        WHERE s.name = %s
    """
    params = [supplier_name]

    if account_number:
        sql += " AND sah.account_number = %s"
        params.append(account_number)

    if billing_ref:
        sql += " AND sii.billing_reference ILIKE %s"
        params.append(f"%{billing_ref}%")

    if view_mode == 'unlinked':
        sql += " AND NOT EXISTS(SELECT 1 FROM circuit_invoice_links cil WHERE cil.invoice_item_id = sii.id)"
    elif view_mode == 'linked':
        sql += " AND EXISTS(SELECT 1 FROM circuit_invoice_links cil WHERE cil.invoice_item_id = sii.id)"

    sql += """
        GROUP BY sii.id, sii.billing_reference, sah.account_number
        ORDER BY sii.billing_reference;
    """

    logging.debug(f"get_invoice_items_for_display: Executing SQL: {sql} with params: {params}")
    with DBClient() as client:
        invoice_items = client._execute_query(sql, tuple(params), fetch='all')
    logging.debug(f"get_invoice_items_for_display: Found {len(invoice_items)} invoice items. First 5: {invoice_items[:5]}")

    return render_template("_invoice_items_table.html", invoice_items=invoice_items)

@link_items_bp.route("/get_cmdb_circuits", methods=["GET"])
def get_cmdb_circuits_for_display():
    customer_id = request.args.get('customer_filter')
    site_name = request.args.get('site_name_filter')
    telco = request.args.get('telco_filter')
    search_all = request.args.get('search_all_circuits')
    logging.debug(f"get_cmdb_circuits_for_display: Filters received - Customer ID: {customer_id}, Site: {site_name}, Telco: {telco}, Search All: {search_all}")
    sql = """
        SELECT c.id, c.circuit_id, c.site_name, c.telco, c.status,
               EXISTS(SELECT 1 FROM circuit_invoice_links cil WHERE cil.circuit_id = c.id) AS is_linked
        FROM cmdb_circuits c
        WHERE c.customer_id = %s
    """
    params = [customer_id]
    if site_name:
        sql += " AND c.site_name ILIKE %s"
        params.append(f"%{site_name}%")
    if telco:
        sql += " AND c.telco ILIKE %s"
        params.append(f"%{telco}%")
    if search_all:
        sql += """ AND (
            c.circuit_id ILIKE %s OR
            c.site_name ILIKE %s OR
            c.telco ILIKE %s OR
            c.status ILIKE %s
        )"""
        search_param = f"%{search_all}%"
        params.extend([search_param, search_param, search_param, search_param])
    sql += " ORDER BY c.circuit_id;"
    logging.debug(f"get_cmdb_circuits_for_display: Executing SQL: {sql} with params: {params}")
    with DBClient() as client:
        cmdb_circuits = client._execute_query(sql, tuple(params), fetch='all')
    logging.debug(f"get_cmdb_circuits_for_display: Found {len(cmdb_circuits)} circuits. First 5: {cmdb_circuits[:5]}")
    return render_template("_cmdb_circuits_table.html", cmdb_circuits=cmdb_circuits)

@link_items_bp.route("/link_item_to_circuit", methods=["POST"])
def link_item_to_circuit():
    item_id = request.form.get("item_id")
    circuit_id_to_link = request.form.get("circuit_id")
    if not item_id or not circuit_id_to_link:
        logger.error("Missing item_id or circuit_id in linking request.")
        return jsonify({'status': 'error', 'message': 'Missing item_id or circuit_id.'}), 400
    try:
        circuit_id_int = int(circuit_id_to_link)
        item_id_int = int(item_id)
        link_payload = {
            "circuit_id": circuit_id_int,
            "invoice_item_id": item_id_int,
        }
        with DBClient() as client:
            client.create_row("circuit_invoice_links", link_payload)
        flash(f"Successfully linked item {item_id_int} to circuit {circuit_id_int}.", "success")
        return jsonify({'status': 'success', 'message': 'Item linked successfully.'})
    except ValueError as ve:
        logger.error(f"ValueError during linking: {ve} for input item_id: '{item_id}', circuit_id: '{circuit_id_to_link}'", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Invalid ID format provided.'}), 400
    except errors.UniqueViolation as uv:
        logger.warning(f"Link already exists for item {item_id} and circuit {circuit_id_to_link}. {uv}")
        return jsonify({'status': 'warning', 'message': 'This item is already linked to this circuit.'}), 409
    except Exception as e:
        logger.error(f"Exception during linking: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'An error occurred while linking the item.'}), 500

@link_items_bp.route("/bulk_match", methods=["GET"])
def bulk_match_ui():
    with DBClient() as client:
        suppliers = client.get_rows("suppliers", columns="id, name", order_by="name")
        customers = client.get_rows("customers", columns="id, name", order_by="name")
    return render_template("bulk_match.html",
                           supplier_options=suppliers,
                           customer_options=customers,
                           matches=None, page=1, total_pages=0)

@link_items_bp.route("/bulk_match_logic", methods=["POST"])
def bulk_match_logic():
    supplier_filter = request.form.get("supplier", "").strip()
    account_filter = request.form.get("account_number", "").strip()
    customer_id_filter = request.form.get("customer_filter", "").strip()
    if not all([supplier_filter, customer_id_filter]):
        flash("Please select both a Supplier and a Customer to find matches.", "error")
        return redirect(url_for('link_items.bulk_match_ui'))

    # --- CORRECTED SQL QUERY CONSTRUCTION ---
    sql_parts = [
        """
        SELECT DISTINCT
            sii.id as item_id,
            sii.billing_reference,
            c.id as circuit_id,
            c.circuit_id as circuit_identifier
        FROM supplier_invoice_items sii
        JOIN suppliers s ON sii.supplier_id = s.id
        JOIN cmdb_circuits c ON c.customer_id = %s
        LEFT JOIN supplier_account san ON sii.account_number_id = san.id
        WHERE s.name = %s
            AND NOT EXISTS(SELECT 1 FROM circuit_invoice_links cil WHERE cil.invoice_item_id = sii.id)
            AND (
                c.circuit_id ILIKE '%%' || sii.billing_reference || '%%' OR
                c.circuit_name ILIKE '%%' || sii.billing_reference || '%%' OR
                c.vendor_account_number ILIKE '%%' || sii.billing_reference || '%%'
            )
        """
    ]
    params = [customer_id_filter, supplier_filter]

    if account_filter:
        sql_parts.append("AND san.account_number = %s")
        params.append(account_filter)

    sql_parts.append("ORDER BY sii.billing_reference;")
    
    match_sql = "\n".join(sql_parts)
    # --- END OF CORRECTION ---

    try:
        with DBClient() as client:
            matches = client._execute_query(match_sql, tuple(params), fetch='all')
        flash(f"Found {len(matches)} potential matches.", "info")
    except Exception as e:
        logger.error(f"Error executing bulk_match_logic query: {traceback.format_exc()}")
        flash("An error occurred while trying to find matches. Please check the logs.", "error")
        matches = []

    with DBClient() as client:
        suppliers = client.get_rows("suppliers", columns="id, name", order_by="name")
        customers = client.get_rows("customers", columns="id, name", order_by="name")
    return render_template("bulk_match.html",
                           matches=matches,
                           supplier_options=suppliers,
                           customer_options=customers,
                           supplier=supplier_filter,
                           account_number=account_filter,
                           customer_filter=customer_id_filter
                           )

@link_items_bp.route("/bulk_link_logic", methods=["POST"])
def bulk_link_logic():
    selections = request.form.getlist("link_selection")
    linked_count = 0
    if not selections:
        flash("No items were selected to link.", "warning")
        return redirect(url_for('link_items.bulk_match_ui'))
    try:
        with DBClient() as client_tx:
            insert_query = "INSERT INTO circuit_invoice_links (circuit_id, invoice_item_id) VALUES (%s, %s);"
            for selection in selections:
                try:
                    item_id, circuit_id = selection.split('_')
                    item_id_int = int(item_id)
                    circuit_id_int = int(circuit_id)
                    client_tx._execute_query(insert_query, (circuit_id_int, item_id_int))
                    linked_count += 1
                except (ValueError, IndexError):
                    logger.warning(f"Skipping malformed selection: {selection}")
                    continue
                except errors.UniqueViolation as uv:
                    logger.warning(f"Skipping existing link: {selection}. Error: {uv}")
                    # In a bulk operation, we just skip duplicates rather than failing
                    client_tx.conn.rollback() # Rollback the failed transaction
                    continue # Continue with the next item
    except Exception as e:
        flash(f"An error occurred during the bulk link: {e}. Changes were rolled back.", "error")
        logging.error(f"Bulk link failed: {traceback.format_exc()}")
        linked_count = 0
    
    if linked_count > 0:
        flash(f"Successfully linked {linked_count} items.", "success")
        
    return redirect(url_for('link_items.bulk_match_ui'))
