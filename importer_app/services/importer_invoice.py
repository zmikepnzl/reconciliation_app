import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Tuple
from dateutil.relativedelta import relativedelta
import json
import io
import traceback
import os

from db_client import DBClient
from utils.transformations import apply_transformation, get_link_row_id


def _nan_to_null(v):
    if pd.isna(v) or v == '':
        return None
    return v

# --- NEW: Helper class to make formula formatting more flexible ---
class ForgivingDict(dict):
    """A dictionary that allows for flexible key lookup in string formatting."""
    def __missing__(self, key):
        # Try to find a key by ignoring case and spaces
        normalized_key = key.replace(" ", "").lower()
        for k, v in self.items():
            if str(k).replace(" ", "").lower() == normalized_key:
                return v
        # If no match is found, raise the original error
        raise KeyError(key)

def _apply_rule(rule: Dict, src_row: pd.Series, dest_dict: Dict, client: DBClient, supplier_id: int, global_context: Dict = None, parent_context: Dict = None):
    target_field = rule.get("name")
    if not target_field:
        return

    source_type = (rule.get("source_type") or "").lower()
    source_col = rule.get("source_csv_column")
    transformation = rule.get("transformation")
    static_value = rule.get("static_value")

    value = None

    if source_type == "csv":
        if source_col and source_col in src_row.index:
            value = src_row.get(source_col)
    elif source_type == "csv formula":
        template = rule.get("formula_template")
        if template:
            try:
                # FIX: Merge all contexts so the formula can see other rule outputs
                context = {}
                if global_context:
                    context.update(global_context)
                if parent_context:
                    context.update(parent_context)
                context.update(dest_dict)
                context.update(src_row.to_dict())

                logging.debug(f"Formula '{target_field}' context keys: {list(context.keys())}")
                value = template.format_map(ForgivingDict(context))
            except KeyError as e:
                logging.error(f"Formula error for '{target_field}': Missing key {e}")
                value = ""
    elif source_type == "text override":
        value = static_value
    elif source_type == "choice":
        value = static_value
    elif source_type == "link":
        link_table = rule.get("link_table_lookup")
        link_field = rule.get("link_field_lookup")
        link_value = src_row.get(source_col) if source_col else None

        if all([link_table, link_field, link_value]):
            value = get_link_row_id(client, link_table, link_field, link_value, supplier_id)

    value = apply_transformation(value, transformation)
    db_field_name = target_field.replace(" ", "_").lower()
    dest_dict[db_field_name] = value


# --- NEW HELPER FUNCTION FOR BILLING MONTH CALCULATION ---
def _calculate_billing_month(header_data, billing_month_rule=None):
    """Calculates the billing month based on invoice date and setting."""
    invoice_date_str = header_data.get('invoice_date')
    
    # Get the billing month setting from the rule, if available
    billing_month_setting = billing_month_rule.get('static_value') if billing_month_rule else 'current'

    if invoice_date_str and billing_month_setting:
        try:
            invoice_date = datetime.strptime(str(invoice_date_str).split(" ")[0], '%Y-%m-%d')
            billing_date = None
            if billing_month_setting == 'advance':
                billing_date = (invoice_date + relativedelta(months=1)).replace(day=1)
            elif billing_month_setting == 'arrears':
                billing_date = (invoice_date - relativedelta(months=1)).replace(day=1)
            elif billing_month_setting == 'current':
                billing_date = invoice_date.replace(day=1)

            if billing_date:
                header_data['billing_month'] = billing_date.strftime('%Y-%m-%d')
                logging.debug(f"Calculated billing month: {header_data['billing_month']} based on setting '{billing_month_setting}'")
        except (ValueError, TypeError) as e:
            logging.warning(f"Could not calculate billing month. Error: {e}")

def import_invoice_csv(client: DBClient, supplier_id: int, mapping_name: str, filepath: str) -> Tuple[int, int]:
    try:
        supplier_id = int(supplier_id)
    except (ValueError, TypeError):
        logging.error(f"Invalid supplier_id provided: {supplier_id}. Cannot proceed with import.")
        return 0, 0

    logging.debug(f"Starting import for supplier_id: {supplier_id}, mapping_name: {mapping_name}, filepath: {filepath}")

    # Fix for file-like objects passed from Flask
    if isinstance(filepath, io.StringIO):
        df = pd.read_csv(filepath, dtype=str)
    else:
        df = pd.read_csv(filepath, dtype=str)

    df.columns = df.columns.str.strip()
    logging.debug(f"CSV headers after stripping: {df.columns.tolist()}")
    logging.debug(f"First row of CSV data: {df.iloc[0].to_dict()}")

    mapping_row = client.get_row("import_mappings", where_clause="mapping_name = %s", params=(mapping_name,))
    if not mapping_row:
        logging.error(f"Mapping '{mapping_name}' not found. Cannot proceed with import.")
        return 0, 0

    supplier_details = client.get_row_by_id("suppliers", supplier_id)
    global_context = {
        "Supplier Short Name": supplier_details.get('supplier_short_name', '') if supplier_details else ''
    }
    logging.debug(f"Global context: {global_context}")

    mapping_id = mapping_row['id']
    rules = client.get_rows("import_mapping_lines", where_clause="mapping_name_id = %s", params=(mapping_id,))

    # ------------------- CRITICAL FIX --------------------------
    # Store the billing_month rule separately so it can be passed to the helper function.
    # Also, remove it from the main rule list to avoid trying to apply it directly.
    billing_timing_rule = next((r for r in rules if r.get('name') == 'billing_timing' and r.get('field_role') == 'header'), None)
    rules = [r for r in rules if not (r.get('name') == 'billing_timing' and r.get('field_role') == 'header')]
    
    billing_month_rule = next((r for r in rules if r.get('name') == 'billing_month' and r.get('field_role') == 'header'), None)
    rules = [r for r in rules if not (r.get('name') == 'billing_month' and r.get('field_role') == 'header')]
    # -----------------------------------------------------------

    logging.debug(f"Fetched {len(rules)} mapping rules for mapping ID {mapping_id}.")
    for r in rules:
        logging.debug(f"Rule loaded from DB: {json.dumps(r, indent=2, default=str)}")

    roles = {"header": [], "line": [], "item": [], "account": [], "ignore_rule": []}

    for r in rules:
        role = (r.get("field_role") or "ignore").lower().replace(" ", "_")
        if role in roles:
            roles[role].append(r)
        else:
            logging.warning(f"Unknown mapping rule role: '{role}' for rule '{r.get('name')}'")
    
    # ------------------- CRITICAL FIX --------------------------
    # Remove the billing_timing rule from the header rules to prevent it from being applied
    roles['header'] = [r for r in roles['header'] if r.get('name') != 'billing_timing']
    # -----------------------------------------------------------

    imported_headers, imported_lines = 0, 0

    # Get the correct column name for the invoice number from the mapping rules
    invoice_number_rule = next((r for r in roles["header"] if r.get('name') == 'invoice_number'), None)
    if not invoice_number_rule or not invoice_number_rule.get('source_csv_column'):
        logging.error("Invoice number mapping rule not found or is missing source column. Cannot group data.")
        return 0, 0
    invoice_number_source_column = invoice_number_rule['source_csv_column']

    for inv_num, grp in df.groupby(invoice_number_source_column):
        if pd.isna(inv_num) or not str(inv_num).strip():
            logging.debug(f"Skipping invoice group due to missing/empty Invoice number: {inv_num}")
            continue

        inv_num = str(inv_num).strip()
        first_row = grp.iloc[0]
        logging.debug(f"Processing InvoiceNumber: {inv_num}, First row data: {first_row.to_dict()}")

        header_data = {"invoice_number": inv_num}

        for rule in roles["header"]:
            logging.debug(f"Applying header rule: {rule.get('name')} (Source: {rule.get('source_csv_column')}, Transform: {rule.get('transformation')}, Args: {rule.get('transformation_args')})")
            _apply_rule(rule, first_row, header_data, client, supplier_id, global_context)

        # ------------------- CRITICAL FIX --------------------------
        # Pass the billing_month rule to the new helper function
        _calculate_billing_month(header_data, billing_month_rule)
        # -----------------------------------------------------------
        
        # --- CRITICAL FIX: Pop the billing_timing and other non-DB fields before inserting ---
        header_data.pop('billing_timing', None)
        header_data.pop('billing_month_logic', None) # Assuming this is the old name for billing_timing
        # -----------------------------------------------------------------------------------

        logging.debug(f"Header data after applying rules: {header_data}")

        account_number_rule = next((rule for rule in roles["account"] if rule["name"] == "account_number"), None)
        if account_number_rule:
            source_col = account_number_rule.get("source_csv_column")
            account_number_value = first_row.get(source_col) if source_col else None
            if account_number_value:
                account_id = get_link_row_id(client, "supplier_account", "account_number", account_number_value, supplier_id)
                if account_id:
                    header_data['account_number_id'] = account_id

        header_data = {k: _nan_to_null(v) for k, v in header_data.items()}
        header_data["supplier_id"] = supplier_id

        data_for_insert = {k: v for k, v in header_data.items() if k != 'id'}

        header_id = None
        header_id_from_db = client.get_row_id("supplier_invoice_headers", "invoice_number", inv_num)

        if header_id_from_db:
            header_id = header_id_from_db
            client.update_row("supplier_invoice_headers", header_id, header_data)
            logging.debug(f"Updated existing header for invoice {inv_num} with ID {header_id}.")
        else:
            new_header = client.create_row("supplier_invoice_headers", data_for_insert)
            header_id = new_header['id'] if new_header else None
            if header_id:
                imported_headers += 1
                logging.debug(f"Created new header for invoice {inv_num} with ID {header_id}.")

        # --- NEW DEBUG: Log header_id before processing lines ---
        logging.debug(f"Header ID for lines processing: {header_id}")

        if not header_id:
            logging.error(f"Failed to create or find header for invoice number {inv_num}. Skipping lines.")
            continue

        # Use item rules to create a grouping key
        item_grouping_rules = [r for r in roles["item"] if r.get('field_role') == 'item']
        item_grouping_cols = [r.get('source_csv_column') for r in item_grouping_rules if r.get('source_csv_column')]

        # ------------------- CRITICAL FIX --------------------------
        # Check if the billing_reference rule is set up, if not, add it to grouping
        billing_ref_rule = next((r for r in roles['item'] if r.get('name') == 'billing_reference'), None)
        if billing_ref_rule and billing_ref_rule.get('source_csv_column') and billing_ref_rule.get('source_csv_column') not in item_grouping_cols:
             item_grouping_cols.append(billing_ref_rule.get('source_csv_column'))
        # -----------------------------------------------------------
        
        # ------------------- CRITICAL FIX --------------------------
        # Check if the list of columns to group by is empty, and if so, skip the grouping.
        if not item_grouping_cols:
            logging.warning("No item grouping columns found in mapping. Falling back to line-by-line import.")
            item_grouping_cols = ['unique_reference_placeholder']
            grp['unique_reference_placeholder'] = grp.index
        # -----------------------------------------------------------

        for item_key, item_grp in grp.groupby(item_grouping_cols):
            # Process the grouped item lines
            item_data = {}
            first_item_row = item_grp.iloc[0]

            for rule in roles["item"]:
                _apply_rule(rule, first_item_row, item_data, client, supplier_id, global_context, parent_context=header_data)

            item_data["supplier_id"] = supplier_id
            if header_data.get('account_number_id'):
                item_data['account_number_id'] = header_data['account_number_id']

            item_ref = item_data.get("billing_reference")
            if not item_ref:
                logging.warning(f"Skipping row due to missing Billing Reference: {first_item_row.to_dict()}")
                continue
            
            # Use billing_reference as a unique key for items
            item_id = client.get_row_id("supplier_invoice_items", "billing_reference", item_ref)
            if item_id:
                client.update_row("supplier_invoice_items", item_id, item_data)
                logging.debug(f"Updated existing item for billing reference {item_ref} with ID {item_id}.")
            else:
                new_item = client.create_row("supplier_invoice_items", item_data)
                item_id = new_item['id'] if new_item else None
                logging.debug(f"Created new item for billing reference {item_ref} with ID {item_id}.")

            if not item_id:
                logging.error(f"Failed to create or find item for billing reference {item_ref}. Skipping lines.")
                continue

            for idx, line_row in item_grp.iterrows():
                logging.debug(f"Processing line {idx} for invoice {inv_num}. Raw row: {line_row.to_dict()}")
                should_skip = any(
                    line_row.get(rule.get("source_csv_column")) == rule.get("ignore_match")
                    for rule in roles.get("ignore_rule", [])
                    if rule.get("source_csv_column") and rule.get("ignore_match")
                )
                if should_skip:
                    logging.warning(f"Skipping line {idx} due to ignore rule match: {line_row.to_dict()}")
                    continue
                
                line_data = {}
                line_parent_context = {**header_data, **item_data}
                
                # Apply line rules
                for rule in roles["line"]:
                    _apply_rule(rule, line_row, line_data, client, supplier_id, global_context, parent_context=line_parent_context)

                line_data["item_id"] = item_id
                line_data["invoice_header_id"] = header_id
                
                line_ref = line_data.get("unique_reference")
                if not line_ref:
                    logging.warning(f"Skipping line due to missing Line Unique Ref: {line_row.to_dict()}")
                    continue
                
                line_id = client.get_row_id("supplier_invoice_lines", "unique_reference", line_ref)
                if line_id:
                    client.update_row("supplier_invoice_lines", line_id, line_data)
                    logging.debug(f"Updated existing line for unique reference {line_ref} with ID {line_id}.")
                else:
                    client.create_row("supplier_invoice_lines", line_data)
                    imported_lines += 1
                    logging.debug(f"Created new line for unique reference {line_ref}.")

    logging.debug(f"Import process finished. Imported {imported_headers} headers and {imported_lines} lines.")
    return imported_headers, imported_lines