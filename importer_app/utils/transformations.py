# In utils/transformations.py

import pandas as pd
import logging
from db_client import DBClient # Assuming this is correctly imported and used elsewhere

def apply_transformation(value, transformation):
    """
    Applies a data type transformation to a given value.
    Returns the transformed value or None if transformation is not applicable/fails.
    """
    if pd.isna(value) or value is None:
        return None # Return None to represent NULL in the database

    # Split transformation string into type and optional argument
    parts = (transformation or 'none').replace(" ", "").lower().split(':', 1)
    t_type = parts[0]
    t_arg = parts[1] if len(parts) > 1 else None

    try:
        if t_type == "none":
            return value
        elif t_type == "totext":
            return str(value)
        elif t_type == "tointeger":
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return None # Return None on conversion failure
        elif t_type == "todecimal":
            try:
                # Use rounding to ensure consistent decimal places
                return round(float(value), 2)
            except (ValueError, TypeError):
                return None # Return None on conversion failure
        elif t_type == "todate":
            try:
                # Handle various date formats here if needed, using t_arg
                # For now, stick to the format specified in your current code
                if t_arg: # If a specific format is provided, use it
                    dt = datetime.strptime(str(value).split(' ')[0], t_arg)
                else: # Default to YYYY/MM/DD if no arg, or try common formats
                    dt = pd.to_datetime(value, errors="coerce") # pandas can infer
                return dt.strftime("%Y-%m-%d") if pd.notna(dt) else None
            except Exception:
                return None # Return None on date parsing failure
        elif t_type == 'tonegative':
            try:
                return -abs(float(value))
            except (ValueError, TypeError):
                return None # Return None on conversion failure
        # Add more transformation types here as needed
        # For any unhandled transformation type, return the original value or None
        else:
            logging.warning(f"Unhandled transformation type: {t_type}. Returning original value.")
            return value # Or return None, depending on desired behavior

    except Exception as e:
        logging.error(f"Error applying transformation '{transformation}' to value '{value}': {e}", exc_info=True)
        return None # Return None on unexpected errors during transformation

def get_link_row_id(client: DBClient, table_name: str, field_name: str, field_value, supplier_id: int = None):
    """
    Finds the ID of a row in a linked table using the DBClient's helper methods.
    If the table is 'supplier_account' and the row doesn't exist, it creates it.
    """
    if not all([table_name, field_name, field_value is not None, str(field_value).strip()]):
        return None

    row_id = client.get_row_id(table_name, field_name, str(field_value).strip())

    # Special handling to auto-create supplier accounts if they don't exist
    if not row_id and table_name == 'supplier_account' and supplier_id:
        logging.info(f"Creating new supplier_account for '{field_value}' linked to supplier ID {supplier_id}.")
        new_account_data = {
            "supplier_id": supplier_id,
            "account_number": str(field_value).strip()
        }
        new_account = client.create_row("supplier_account", new_account_data)
        if new_account:
            row_id = new_account['id']
            logging.info(f"Successfully created supplier_account with ID {row_id}.")
        else:
            logging.error(f"Failed to create new supplier_account for '{field_value}'.")

    return row_id

def get_val(ctx, *keys):
    """
    Return the first non-empty value found by trying all the given keys/case-variants.
    """
    for k in keys:
        # Direct key match
        if k in ctx and pd.notna(ctx[k]) and ctx[k]:
            return str(ctx[k])
        # Case-insensitive match
        kl = k.lower()
        for kc in ctx.keys():
            if kc.lower() == kl and pd.notna(ctx[kc]) and ctx[kc]:
                return str(ctx[kc])
    return ""