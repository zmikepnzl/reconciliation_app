import os
import traceback
import logging
import json
import pandas as pd
from flask import Blueprint, render_template, request, flash, redirect, url_for, Response, jsonify
from datetime import datetime

# Relative import from another file within the 'services' package
from .importer_invoice import _apply_rule 

# Imports from other parts of your application
from db_client import DBClient
from utils.transformations import apply_transformation

# Configure the Blueprint
mapping_manager_bp = Blueprint('mapping_manager', __name__, template_folder='../../templates', url_prefix='/admin/mapping_rules')

# --- Schema Cache & Validation Helpers ---
SCHEMA_CACHE = {}

def get_schema_info(client, table_name, column_name):
    """
    Fetches and caches schema information for a given table and column.
    """
    cache_key = f"{table_name}.{column_name}"
    if cache_key in SCHEMA_CACHE:
        return SCHEMA_CACHE[cache_key]

    sql = """
        SELECT data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s;
    """
    try:
        result = client._execute_query(sql, (table_name, column_name), fetch='one')
        if result:
            SCHEMA_CACHE[cache_key] = result
            return result
    except Exception as e:
        logging.error(f"Could not fetch schema for {cache_key}: {e}")
    return None

def validate_value(value, schema_info):
    """
    Validates a value against its target schema, returning a status and message.
    """
    if value is None or value == '':
        return {'status': 'ok', 'message': 'Value is empty.'}

    if not schema_info:
        return {'status': 'warning', 'message': 'Could not determine destination field type for validation.'}

    data_type = schema_info.get('data_type', '')
    max_length = schema_info.get('character_maximum_length')
    value_str = str(value)

    if 'character varying' in data_type and max_length:
        if len(value_str) > max_length:
            return {'status': 'warning', 'message': f'Warning: Text length ({len(value_str)}) is greater than field limit ({max_length}). Value may be truncated.'}

    if data_type in ['integer', 'bigint', 'numeric', 'double precision', 'real']:
        try:
            float(value)
        except (ValueError, TypeError):
            return {'status': 'error', 'message': f'Error: Value "{value}" is not a valid number.'}

    if 'date' in data_type or 'timestamp' in data_type:
        try:
            datetime.strptime(value_str.split(' ')[0], '%Y-%m-%d')
        except (ValueError, TypeError):
            return {'status': 'error', 'message': f'Error: Value "{value}" is not a valid date.'}

    return {'status': 'ok', 'message': 'Value is valid for this field type.'}

# --- General Helper Functions ---
def get_active_mappings_for_supplier(client, supplier_id):
    if not supplier_id:
        return []
    try:
        return client.get_rows(
            "import_mappings",
            columns="id, mapping_name as name",
            where_clause="supplier_id = %s",
            params=(supplier_id,),
            order_by="mapping_name"
        )
    except Exception as e:
        logging.error(f"Failed to get mappings for supplier {supplier_id}: {e}")
        return []

def get_mapping_row_by_name(client, mapping_name):
    if not mapping_name:
        return None
    try:
        return client.get_row("import_mappings", where_clause="mapping_name = %s", params=(mapping_name,))
    except Exception as e:
        logging.error(f"Failed to get mapping by name '{mapping_name}': {e}")
        return None

# --- Routes ---

@mapping_manager_bp.route("/mappings/lines/<int:mapping_id>", methods=["GET"])
def manage_mapping_lines(mapping_id):
    mapping_rule, full_rule_set = {}, {}
    sample_csv_headers, sample_csv_first_row, existing_ignore_rules = [], [], []
    try:
        with DBClient() as client:
            mapping_rule = client.get_row_by_id("import_mappings", mapping_id)
            if mapping_rule.get('sample_csv_headers'):
                sample_csv_headers = json.loads(mapping_rule['sample_csv_headers'])
            if mapping_rule.get('sample_csv_first_row'):
                sample_csv_first_row = json.loads(mapping_rule['sample_csv_first_row'])
            
            sql = """
                SELECT 
                    iml.*, 
                    mrm.rule_name, 
                    mrm.is_required, 
                    mrm.is_hidden, 
                    mrm.source_type_options,
                    mrm.link_table_lookup, 
                    mrm.link_field_lookup,
                    mrm.formula_template AS metadata_formula_template,
                    mrm.static_value AS metadata_static_value,
                    mrm.default_transformation,
                    mrm.transformation_args AS metadata_transformation_args
                FROM import_mapping_lines iml
                LEFT JOIN mapping_rules_metadata mrm ON iml.name = mrm.destination_field AND iml.field_role = mrm.field_role
                WHERE iml.mapping_name_id = %s AND iml.name != 'billing_month'
            """
            joined_rules = client._execute_query(sql, (mapping_id,), fetch='all')

            full_rule_set = {}
            all_transformations_fallback = ["To Text", "To Date", "To Integer", "To Decimal", "To Negative", "None"]
            
            for rule in joined_rules:
                role_key = rule['field_role'].lower().replace(" ", "_")
                if role_key not in full_rule_set:
                    full_rule_set[role_key] = []

                rule_to_display = dict(rule)
                rule_to_display['display_name'] = rule.get('rule_name', rule.get('name', ''))
                rule_to_display['required'] = rule.get('is_required', False)
                
                allowed_trans_str = rule.get('default_transformation')
                allowed_list = allowed_trans_str.split(',') if allowed_trans_str else all_transformations_fallback
                if 'None' in allowed_list:
                    allowed_list.remove('None')
                    allowed_list.insert(0, 'None')
                
                rule_to_display['allowed_transformations'] = allowed_list
                rule_to_display['is_hidden'] = rule.get('is_hidden', False)
                rule_to_display['source_type_options'] = rule.get('source_type_options', 'CSV,CSV Formula,Text Override,Link,None')
                rule_to_display['formula_template'] = rule.get('formula_template') if rule.get('formula_template') is not None else rule.get('metadata_formula_template')
                rule_to_display['static_value'] = rule.get('static_value') if rule.get('static_value') is not None else rule.get('metadata_static_value')
                rule_to_display['transformation_args'] = rule.get('transformation_args') if rule.get('transformation_args') is not None else rule.get('metadata_transformation_args')
                rule_to_display['link_table_lookup'] = rule.get('link_table_lookup')
                rule_to_display['link_field_lookup'] = rule.get('link_field_lookup')

                full_rule_set[role_key].append(rule_to_display)
            
            saved_billing_month = client._execute_query("SELECT * FROM import_mapping_lines WHERE mapping_name_id = %s AND name = 'billing_month'", (mapping_id,), fetch='one')
            billing_month_line_id = saved_billing_month.get('id', 'new_billing_month_rule') if saved_billing_month else 'new_billing_month_rule'

            billing_month_rule = {
                'id': billing_month_line_id, 'name': 'billing_month', 'field_role': 'header',
                'display_name': 'Billing Month', 'required': True, 'is_hidden': False,
                'source_type': 'Text Override', 'static_value': saved_billing_month.get('static_value', 'current') if saved_billing_month else 'current',
                'allowed_transformations': ['None'], 'transformation': 'None'
            }
            if 'header' not in full_rule_set:
                full_rule_set['header'] = []
            full_rule_set['header'].append(billing_month_rule)
            
            existing_ignore_rules = client.get_rows("import_mapping_lines", where_clause="mapping_name_id = %s AND field_role = 'Ignore Rule'", params=(mapping_id,))

    except Exception as e:
        flash("An error occurred while loading mapping lines.", "error")
        logging.error(f"Failed to load mapping lines: {traceback.format_exc()}")
    
    ordered_rule_set = {}
    order = ['account', 'header', 'item', 'line']
    for role in order:
        if role in full_rule_set:
            ordered_rule_set[role] = sorted(full_rule_set[role], key=lambda x: (not x.get('required', False), x.get('display_name') or ''))

    return render_template(
        "manage_mapping_lines.html",
        mapping_rule=mapping_rule,
        full_rule_set=ordered_rule_set, 
        sample_csv_headers=sample_csv_headers,
        sample_csv_first_row=sample_csv_first_row,
        existing_ignore_rules=existing_ignore_rules
    )


@mapping_manager_bp.route("/mappings/lines/save_set", methods=["POST"])
def save_mapping_lines_set():
    mapping_id = request.form.get('mapping_id')
    try:
        with DBClient() as client:
            mapping_details_data = {
                'mapping_name': request.form.get('mapping_name'),
                'description': request.form.get('description')
            }
            if mapping_details_data.get('mapping_name'):
                client.update_row("import_mappings", int(mapping_id), mapping_details_data)

            delete_sql = "DELETE FROM import_mapping_lines WHERE mapping_name_id = %s AND field_role = 'Ignore Rule'"
            client._execute_query(delete_sql, (mapping_id,))

            form_rules = {}
            for key, value in request.form.items():
                if '.' in key:
                    prefix, field = key.rsplit('.', 1)
                    if prefix not in form_rules:
                        form_rules[prefix] = {}
                    form_rules[prefix][field] = value

            for prefix, data in form_rules.items():
                if prefix.startswith('ignore_rule'):
                    ignore_rule_data = {
                        'mapping_name_id': mapping_id, 'name': 'Ignore Rule', 'field_role': 'Ignore Rule',
                        'source_type': data.get('source_type'), 'source_csv_column': data.get('source_csv_column'),
                        'formula_template': data.get('formula_template'), 'ignore_match': data.get('ignore_match')
                    }
                    if ignore_rule_data.get('ignore_match'):
                        client.create_row('import_mapping_lines', ignore_rule_data)
                
                elif 'id' in data:
                    line_id = data.get('id')
                    rule_data = {
                        'source_type': data.get('source_type'), 'source_csv_column': data.get('source_csv_column'),
                        'formula_template': data.get('formula_template'), 'static_value': data.get('static_value'),
                        'transformation': data.get('transformation'), 'transformation_args': data.get('transformation_args')
                    }
                    
                    if str(line_id).startswith('new_'):
                        rule_data.update({'mapping_name_id': mapping_id, 'name': data.get('name'), 'field_role': data.get('field_role')})
                        client.create_row('import_mapping_lines', rule_data)
                    else:
                        client.update_row('import_mapping_lines', line_id, rule_data)

            flash("Mapping rule set saved successfully!", "success")
    except Exception as e:
        flash(f"Could not save mapping rule set. Error: {e}", "error")
        logging.error(f"Failed to save mapping rule set: {traceback.format_exc()}")
    return redirect(url_for('mapping_manager.manage_mapping_lines', mapping_id=mapping_id))


@mapping_manager_bp.route("/mappings/lines/save_sample_data", methods=["POST"])
def save_sample_data():
    mapping_id = request.form.get('mapping_id')
    headers_json = request.form.get('headers_json')
    first_row_json = request.form.get('first_row_json')
    
    if not mapping_id:
        return jsonify({"status": "error", "message": "Mapping ID missing."}), 400
    
    try:
        with DBClient() as client:
            update_data = {
                'sample_csv_headers': headers_json,
                'sample_csv_first_row': first_row_json
            }
            client.update_row("import_mappings", mapping_id, update_data)
            return jsonify({"status": "success", "message": "Sample data saved."})
    except Exception as e:
        logging.error(f"Failed to save sample data for mapping {mapping_id}: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": "Failed to save sample data."}), 500
            
@mapping_manager_bp.route("/api/transform_preview", methods=["POST"])
def transform_preview():
    """
    Takes a raw value, transformation details, and destination field,
    then returns a preview of the output along with a validation check.
    This version correctly formats the transformation argument before calling the helper.
    """
    data = request.json
    raw_value = data.get('raw_value')
    transformation = data.get('transformation')
    transformation_args = data.get('transformation_args')
    destination_field = data.get('destination_field')
    field_role = data.get('field_role')
    
    if raw_value is None or transformation is None:
        return jsonify({
            "transformed_value": str(raw_value or ''),
            "validation_status": 'ok',
            "validation_message": 'No value to transform.'
        })

    transformed_value = None
    try:
        # --- FIX: Combine the transformation and its argument into one string ---
        if transformation_args:
            full_transformation_string = f"{transformation}:{transformation_args}"
        else:
            full_transformation_string = transformation

        # Now call the transformation function with the correct number of arguments (2)
        transformed_value = apply_transformation(raw_value, full_transformation_string)

    except Exception as e:
        logging.warning(f"Transformation failed during preview: {e}")
        return jsonify({
            "transformed_value": raw_value, 
            "validation_status": 'error',
            "validation_message": f"Transformation Error: {e}"
        })

    # Step 2: Validate the successfully transformed value against the database schema.
    validation_result = {'status': 'ok', 'message': 'Validation not performed.'}
    table_map = {
        'header': 'supplier_invoice_headers', 'line': 'supplier_invoice_lines',
        'item': 'supplier_invoice_items', 'account': 'supplier_account'
    }
    table_name = table_map.get(field_role)
    
    if table_name and destination_field:
        try:
            with DBClient() as client:
                schema_info = get_schema_info(client, table_name, destination_field)
                validation_result = validate_value(transformed_value, schema_info)
        except Exception as e:
            logging.error(f"Validation failed during preview: {e}", exc_info=True)
            validation_result = {'status': 'warning', 'message': 'Could not perform validation.'}

    return jsonify({
        "transformed_value": str(transformed_value or ''),
        "validation_status": validation_result.get('status'),
        "validation_message": validation_result.get('message')
    })

@mapping_manager_bp.route("/metadata_admin")
def mapping_metadata_admin():
    """Renders the mapping metadata administration page."""
    schema_data = {}
    try:
        with DBClient() as client:
            # Fetch all table names in the public schema
            tables_query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename;"
            tables = client._execute_query(tables_query, fetch='all')
            
            for table in tables:
                table_name = table['tablename']
                # Fetch all column names for each table
                columns_query = """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY column_name;
                """
                columns = client._execute_query(columns_query, (table_name,), fetch='all')
                schema_data[table_name] = [col['column_name'] for col in columns]

    except Exception as e:
        flash("Could not load database schema for Link fields.", "error")
        logging.error(f"Failed to fetch database schema: {e}")

    return render_template("mapping_metadata_admin.html", db_schema=schema_data)
    
# --- Mapping Metadata API Routes ---

@mapping_manager_bp.route("/api/metadata", methods=["GET"])
def get_all_mapping_rules():
    """Fetches all mapping rules metadata."""
    try:
        with DBClient() as client:
            rules = client.get_rows("mapping_rules_metadata", order_by="rule_name")
            return jsonify(rules)
    except Exception as e:
        logging.error(f"Error fetching mapping rules: {e}", exc_info=True)
        return jsonify({"message": "Failed to fetch mapping rules", "error": str(e)}), 500

@mapping_manager_bp.route("/api/metadata", methods=["POST"])
def create_mapping_rule():
    """Creates a new mapping rule metadata entry."""
    data = request.json
    if not data:
        return jsonify({"message": "No data provided"}), 400

    required_fields = ["rule_name", "field_role", "destination_field"]
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({"message": f"Missing required field: {field}"}), 400

    try:
        with DBClient() as client:
            data_to_insert = { k: v for k, v in data.items() if k != 'id' }
            new_rule = client.create_row("mapping_rules_metadata", data_to_insert)
            return jsonify(new_rule), 201
    except Exception as e:
        logging.error(f"Error creating mapping rule: {e}", exc_info=True)
        return jsonify({"message": "Failed to create mapping rule", "error": str(e)}), 500

@mapping_manager_bp.route("/api/metadata/<int:rule_id>", methods=["PUT"])
def update_mapping_rule(rule_id):
    """Updates an existing mapping rule metadata entry."""
    data = request.json
    if not data:
        return jsonify({"message": "No data provided"}), 400

    try:
        with DBClient() as client:
            data_to_update = { k: v for k, v in data.items() if k != 'id' }
            client.update_row("mapping_rules_metadata", rule_id, data_to_update)
            updated_rule = client.get_row_by_id("mapping_rules_metadata", rule_id)
            return jsonify(updated_rule), 200
    except Exception as e:
        logging.error(f"Error updating mapping rule {rule_id}: {e}", exc_info=True)
        return jsonify({"message": "Failed to update mapping rule", "error": str(e)}), 500

@mapping_manager_bp.route("/api/metadata/<int:rule_id>", methods=["DELETE"])
def delete_mapping_rule(rule_id):
    """Deletes a mapping rule metadata entry."""
    try:
        with DBClient() as client:
            client.delete_row("mapping_rules_metadata", rule_id)
            logging.info(f"Successfully deleted rule with ID: {rule_id}")
            return jsonify({"message": "Rule deleted successfully"}), 200
    except Exception as e:
        logging.error(f"Error deleting mapping rule {rule_id}: {e}", exc_info=True)
        return jsonify({"message": "Failed to delete mapping rule", "error": str(e)}), 500