import os
import traceback
import logging
import json
import io
import csv
from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from datetime import datetime, timedelta

from db_client import DBClient

# 1. Define the Blueprint FIRST
zeus_export_bp = Blueprint('zeus_export', __name__, template_folder='../../templates')

# 2. Define the main page route
@zeus_export_bp.route("/", methods=["GET"])
def zeus_export():
    customer_options = []
    usage_month_options = []
    cmdb_vendors = []
    invoice_suppliers = []
    display_rows = []
    customer_filter = request.values.get("customer", "").strip()
    usage_month = request.values.get("usage_month", "").strip()
    audit_filter = request.values.get("audit_filter", "1m") # Default to 1m
    cmdb_vendor_filter = request.values.get("cmdb_vendor", "").strip()
    invoice_supplier_filter = request.values.get("invoice_supplier", "").strip()
    three_months_ago = datetime.now().date() - timedelta(days=90)

    try:
        with DBClient() as client:
            # Always fetch all customer options
            customers = client.get_rows("customers", columns="name", order_by="name")
            customer_options = [c['name'] for c in customers]

            customer_id = None
            if customer_filter:
                sql = "SELECT id FROM customers WHERE name = %s LIMIT 1"
                result = client._execute_query(sql, (customer_filter,), fetch='all')
                if result:
                    customer_id = result[0]['id']

            # Filter Usage Month options based on selected customer
            if customer_id:
                usage_month_sql = """
                    SELECT DISTINCT TO_CHAR(sil.start_date, 'YYYY-MM') as month
                    FROM supplier_invoice_lines sil
                    JOIN supplier_invoice_items sii ON sil.item_id = sii.id
                    JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
                    JOIN cmdb_circuits cc ON cil.circuit_id = cc.id
                    WHERE sil.start_date IS NOT NULL AND cc.customer_id = %s
                    ORDER BY month DESC;
                """
                usage_months_result = client._execute_query(usage_month_sql, (customer_id,), fetch='all')
                usage_month_options = [r['month'] for r in usage_months_result]
            else:
                usage_month_sql = """
                    SELECT DISTINCT TO_CHAR(start_date, 'YYYY-MM') as month
                    FROM supplier_invoice_lines
                    WHERE start_date IS NOT NULL
                    ORDER BY month DESC;
                """
                usage_months_result = client._execute_query(usage_month_sql, fetch='all')
                usage_month_options = [r['month'] for r in usage_months_result]

            # Filter CMDB Vendors based on selected customer
            if customer_id:
                cmdb_vendors = [r['telco'] for r in client.get_rows("cmdb_circuits", columns="DISTINCT telco", where_clause="customer_id = %s AND telco IS NOT NULL", params=(customer_id,), order_by="telco")]
            else:
                cmdb_vendors = [r['telco'] for r in client.get_rows("cmdb_circuits", columns="DISTINCT telco", where_clause="telco IS NOT NULL", order_by="telco")]

            # Filter Invoice Suppliers based on selected customer
            if customer_id:
                invoice_suppliers_sql = """
                    SELECT DISTINCT s.supplier_short_name
                    FROM suppliers s
                    JOIN supplier_invoice_items sii ON s.id = sii.supplier_id
                    JOIN circuit_invoice_links cil ON sii.id = cil.invoice_item_id
                    JOIN cmdb_circuits cc ON cil.circuit_id = cc.id
                    WHERE s.supplier_short_name IS NOT NULL AND cc.customer_id = %s
                    ORDER BY s.supplier_short_name;
                """
                invoice_suppliers_result = client._execute_query(invoice_suppliers_sql, (customer_id,), fetch='all')
                invoice_suppliers = [r['supplier_short_name'] for r in invoice_suppliers_result]
            else:
                invoice_suppliers = [r['supplier_short_name'] for r in client.get_rows("suppliers", columns="DISTINCT supplier_short_name", where_clause="supplier_short_name IS NOT NULL", order_by="supplier_short_name")]


            if customer_filter and usage_month:
                # REVISED: Use LEFT JOINs in the CTE to ensure all circuits are returned
                export_sql = """
                    WITH invoice_summary AS (
                        SELECT
                            cil.circuit_id,
                            STRING_AGG(sii.billing_reference, ', ') AS invoice_billing_reference,
                            SUM(sil.total_amount) AS invoice_month_total,
                            COUNT(sil.id) AS invoice_month_linecount,
                            MAX(sii.id) AS linked_invoice_item_id,
                            MAX(s.override_name) as supplier_override_name
                        FROM circuit_invoice_links cil
                        LEFT JOIN supplier_invoice_items sii ON cil.invoice_item_id = sii.id
                        LEFT JOIN supplier_invoice_lines sil ON sii.id = sil.item_id
                        LEFT JOIN suppliers s ON sii.supplier_id = s.id
                        WHERE TO_CHAR(sil.start_date, 'YYYY-MM') = %s
                        GROUP BY cil.circuit_id
                    )
                    SELECT
                        c.id as cmdb_id, c.circuit_id, c.site_name, c.status, c.telco as vendor_name,
                        c.vendor_account_number, c.audit_date as circuit_audit_date,
                        sci.billing_name as sci_service_description, sci.customer_sell_price as sci_customer_sell_price,
                        sci.datacom_cost_price as sci_datacom_cost_price,
                        COALESCE(inv_sum.invoice_month_total, 0) as invoice_month_total,
                        COALESCE(inv_sum.invoice_month_linecount, 0) as invoice_month_linecount,
                        inv_sum.linked_invoice_item_id, inv_sum.invoice_billing_reference,
                        inv_sum.supplier_override_name,
                        c.service_catalogue_item_linked_id AS sci_id
                    FROM cmdb_circuits c
                    LEFT JOIN service_catalog_items sci ON c.service_catalogue_item_linked_id = sci.id
                    LEFT JOIN invoice_summary inv_sum ON inv_sum.circuit_id = c.id
                    WHERE c.customer_id = (SELECT id FROM customers WHERE name = %s)
                """
                params = [usage_month, customer_filter]
                
                if cmdb_vendor_filter:
                    export_sql += " AND c.telco = %s"
                    params.append(cmdb_vendor_filter)

                if invoice_supplier_filter:
                    # This requires a more complex subquery or join modification
                    export_sql += " AND c.id IN (SELECT cil.circuit_id FROM circuit_invoice_links cil JOIN supplier_invoice_items sii ON cil.invoice_item_id = sii.id JOIN suppliers s ON sii.supplier_id = s.id WHERE s.supplier_short_name = %s)"
                    params.append(invoice_supplier_filter)

                if audit_filter == '1m':
                    one_month_ago = datetime.now() - timedelta(days=30)
                    export_sql += " AND (c.audit_date IS NULL OR c.audit_date < %s)"
                    params.append(one_month_ago.strftime('%Y-%m-%d'))
                elif audit_filter == '3m':
                    three_months_ago_filter = datetime.now() - timedelta(days=90)
                    export_sql += " AND (c.audit_date IS NULL OR c.audit_date < %s)"
                    params.append(three_months_ago_filter.strftime('%Y-%m-%d'))
                elif audit_filter == 'older':
                    three_months_ago_filter = datetime.now() - timedelta(days=90)
                    export_sql += " AND c.audit_date < %s"
                    params.append(three_months_ago_filter.strftime('%Y-%m-%d'))

                export_sql += " ORDER BY c.site_name, c.circuit_id;"
                
                display_rows = client._execute_query(export_sql, params=tuple(params), fetch='all')

    except Exception as e:
        flash(f"An error occurred while loading data: {e}", "error")
        logging.error(f"Failed to load zeus export data: {traceback.format_exc()}")
        display_rows = []

    return render_template("zeus_export.html",
        customer=customer_filter,
        customer_options=customer_options,
        usage_month=usage_month,
        usage_month_options=usage_month_options,
        audit_filter=audit_filter,
        cmdb_vendor_filter=cmdb_vendor_filter,
        invoice_supplier_filter=invoice_supplier_filter,
        cmdb_vendors=cmdb_vendors,
        invoice_suppliers=invoice_suppliers,
        rows=display_rows,
        three_months_ago=three_months_ago
    )


# 3. Define the bulk update route
@zeus_export_bp.route("/zeus_bulk_update", methods=["POST"])
def zeus_bulk_update():
    customer = request.form.get("customer")
    usage_month = request.form.get("usage_month")
    nowdate = datetime.now().strftime("%Y-%m-%d")
    
    updates_json = request.form.get("updates_json")
    if not updates_json:
        flash("No updates provided.", "warning")
        return redirect(url_for('zeus_export.zeus_export', customer=customer, usage_month=usage_month))
    
    updates_data = json.loads(updates_json)
    
    try:
        with DBClient() as client_tx:
            for update in updates_data:
                cmdb_id = update.get("cmdb_id")
                sci_id = update.get("sci_id")
                linked_invoice_item_id = update.get("linked_invoice_item_id")
                
                circuit_update = {}
                
                if update.get("do_override_vendor_name"):
                    supplier_override_name = update.get('supplier_override_name')
                    if supplier_override_name:
                           circuit_update["telco"] = supplier_override_name

                if update.get("do_update_cost") and sci_id:
                    invoice_month_total = update.get("invoice_month_total")
                    if invoice_month_total is not None:
                        try:
                            invoice_month_total_float = float(invoice_month_total)
                            if invoice_month_total_float >= 0:
                                client_tx.update_row("service_catalog_items", sci_id, {"datacom_cost_price": invoice_month_total_float})
                        except (ValueError, TypeError):
                            logging.error(f"Could not convert invoice_month_total '{invoice_month_total}' to float for cmdb_id: {cmdb_id}")

                if update.get("do_audit_circuit"):
                    circuit_update["audit_date"] = nowdate
                
                if update.get("do_audit_invoice_item") and linked_invoice_item_id:
                    client_tx.update_row("supplier_invoice_items", linked_invoice_item_id, {"audit_date": nowdate})

                if circuit_update:
                    client_tx.update_row("cmdb_circuits", cmdb_id, circuit_update)
            
            flash("Bulk updates applied successfully.", "success")
            
    except Exception as e:
        flash(f"An error occurred during bulk update: {e}. Changes were rolled back.", "error")
        logging.error(f"Bulk update failed: {traceback.format_exc()}")
    
    return redirect(url_for('zeus_export.zeus_export', customer=customer, usage_month=usage_month))

# 4. Define the NEW export routes
@zeus_export_bp.route("/export_circuits_csv", methods=["POST"])
def export_circuits_csv():
    """Exports selected CMDB circuits to a CSV file."""
    try:
        customer_name = request.form.get("customer", "Export")
        export_ids_json = request.form.get("export_ids")
        if not export_ids_json:
            flash("No IDs provided for export.", "error")
            return redirect(url_for('zeus_export.zeus_export'))

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
            sql_query = "SELECT * FROM cmdb_circuits WHERE id IN %s"
            circuits = client._execute_query(sql_query, (export_ids,), fetch='all')

            if not circuits:
                flash("No matching circuits found for export.", "warning")
                return redirect(url_for('zeus_export.zeus_export', customer=customer_name))

            string_io = io.StringIO()
            writer = csv.DictWriter(string_io, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()

            for circuit in circuits:
                row_data = {
                    'id': circuit.get('zeus_id'),
                    'name': circuit.get('circuit_id'),
                    'description': circuit.get('circuit_name'),
                    'state': circuit.get('status'),
                    'service_catalogue_item': circuit.get('service_catalogue_item_imported'),
                    'site': circuit.get('site_name'),
                    'site_code': circuit.get('site_code'),
                    'telco': circuit.get('telco'),
                    'priority': circuit.get('priority'),
                    'circuit_type': circuit.get('circuit_type'),
                    'circuit_service_type': circuit.get('circuit_service_type'),
                    'circuit_termination_type': circuit.get('circuit_termination_type'),
                    'access': circuit.get('access'),
                    'cir': circuit.get('cir'),
                    'pir': circuit.get('pir'),
                    'vlan': circuit.get('vlan'),
                    'pe_host': circuit.get('pe_host'),
                    'ip_pe': circuit.get('ip_pe'),
                    'ce_host': circuit.get('ce_host'),
                    'host': circuit.get('host'),
                    'ip_router': circuit.get('ip_router'),
                    'interface': circuit.get('interface'),
                    'linknet': circuit.get('linknet'),
                    'qosprofile': circuit.get('qosprofile'),
                    'notes': circuit.get('notes'),
                    'netops_sensor_id': circuit.get('netops_sensor_id'),
                    'netflow': circuit.get('netflow'),
                    'contract_start': circuit.get('contract_start'),
                    'contract_end': circuit.get('contract_end'),
                    'exchange': circuit.get('exchange'),
                    'region': circuit.get('region'),
                    'activated': circuit.get('activated'),
                    'decommissioned': circuit.get('decommissioned'),
                    'vendor_billing_end_date': circuit.get('vendor_billing_end_date'),
                    'service_end_date': circuit.get('service_end_date'),
                    'datacom_last_billing_run_date': circuit.get('datacom_last_billing_run_date'),
                    'datacom_billing_code': circuit.get('datacom_billing_code'),
                    'vendor_account_number': circuit.get('vendor_account_number')
                }
                writer.writerow({k: (v if v is not None else '') for k, v in row_data.items()})

        mem = io.BytesIO()
        mem.write(string_io.getvalue().encode('utf-8'))
        mem.seek(0)
        string_io.close()

        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{customer_name} Circuits {today_str}.csv"
        
        return Response(
            mem,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )

    except Exception as e:
        flash(f"An error occurred during CSV export: {e}", "error")
        logging.error(f"Circuit export failed: {traceback.format_exc()}")
        return redirect(url_for('zeus_export.zeus_export'))


@zeus_export_bp.route("/export_scis_csv", methods=["POST"])
def export_scis_csv():
    """Exports selected Service Catalogue Items to a CSV file."""
    try:
        customer_name = request.form.get("customer", "Export")
        export_ids_json = request.form.get("export_ids")
        if not export_ids_json:
            flash("No IDs provided for export.", "error")
            return redirect(url_for('zeus_export.zeus_export'))

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
            sql_query = "SELECT * FROM service_catalog_items WHERE id IN %s"
            scis = client._execute_query(sql_query, (export_ids,), fetch='all')

            if not scis:
                flash("No matching SCIs found for export.", "warning")
                return redirect(url_for('zeus_export.zeus_export', customer=customer_name))

            string_io = io.StringIO()
            writer = csv.DictWriter(string_io, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()

            for sci in scis:
                row_data = {
                    'id': sci.get('zeus_id'),
                    'service_line': sci.get('service_line'),
                    'master_service_catalogue_item': sci.get('master_service_catalogue_item'),
                    'netsuite_ru_item_code': sci.get('netsuite_ru_item_code'),
                    'unit': sci.get('unit'),
                    'service_billing_method': sci.get('service_billing_method'),
                    'billing_name': sci.get('billing_name'),
                    'host_types': sci.get('host_types'),
                    'service_description': sci.get('service_description'),
                    'billable': sci.get('billable'),
                    'include_in_billing_run': sci.get('include_in_billing_run'),
                    'contract_number': sci.get('contract_number'),
                    'service_key': sci.get('service_key'),
                    'datacom_cost_price': sci.get('datacom_cost_price'),
                    'customer_standard_price': sci.get('customer_standard_price'),
                    'adjustment_percentage': sci.get('adjustment_percentage'),
                    'customer_sell_price': sci.get('customer_sell_price'),
                    'additional_info': sci.get('additional_info'),
                    'billable_quantity': sci.get('billable_quantity'),
                    'pipeline_quantity': sci.get('pipeline_quantity'),
                    'total_bill_price': sci.get('total_bill_price')
                }
                writer.writerow({k: (v if v is not None else '') for k, v in row_data.items()})

        mem = io.BytesIO()
        mem.write(string_io.getvalue().encode('utf-8'))
        mem.seek(0)
        string_io.close()

        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{customer_name} SCIs {today_str}.csv"
        
        return Response(
            mem,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )

    except Exception as e:
        flash(f"An error occurred during SCI CSV export: {e}", "error")
        logging.error(f"SCI export failed: {traceback.format_exc()}")
        return redirect(url_for('zeus_export.zeus_export'))
